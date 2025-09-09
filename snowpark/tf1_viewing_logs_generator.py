"""
TF1 Viewing Logs Generator (Snowpark)

Generates a realistic one-week TF1 viewing logs table using a single CTAS.
Features:
- 7 days x 48 half-hour slots with time-of-day and weekend weighting
- ~30% of logs linked to existing SF1PLUS_CRM customers
- French IP address generation by ISP ranges
- Smart TV/device metadata in semi-structured VARIANT column
- Ad break estimation based on watch duration

Entrypoints:
- run(session, sample_multiplier, attach_customer_pct, overwrite)
- sp_entry(session, sample_multiplier?, attach_customer_pct?, overwrite?)
"""

from typing import Optional

from snowflake.snowpark import Session


TARGET_DB = "SF1PLUS_DB"
RAW_SCHEMA = "RAW_DATA"
OUTPUT_TABLE = "TF1_VIEWING_LOGS"
CUSTOMER_TABLE = f"{TARGET_DB}.{RAW_SCHEMA}.SF1PLUS_CRM"


def _exec(session: Session, sql: str) -> None:
    session.sql(sql).collect()


def run(
    session: Session,
    sample_multiplier: int = 1,
    attach_customer_pct: float = 0.30,
    overwrite: bool = True,
) -> "Session":
    """
    Build TF1 viewing logs for one week.

    Args:
        session: Snowpark session
        sample_multiplier: scales events per slot (baseline ~100). Use 1 by default (≈10% sample)
        attach_customer_pct: fraction of events that should have a customer_id (default 0.30)
        overwrite: whether to replace the output table

    Returns:
        A small preview DataFrame (100 rows)
    """
    _exec(session, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    _exec(session, f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{RAW_SCHEMA}")

    full_table = f"{TARGET_DB}.{RAW_SCHEMA}.{OUTPUT_TABLE}"

    # Clamp attach_customer_pct to [0,1]
    attach_pct = max(0.0, min(1.0, float(attach_customer_pct)))

    # Baseline events per slot around 100; weight by time-of-day/day-of-week and scale by multiplier
    # We generate up to 600 candidates per slot and keep the first N via QUALIFY
    # This produces a realistic distribution without temporary tables.
    select_sql = f"""
        WITH params AS (
            SELECT 
                DATE_TRUNC('WEEK', CURRENT_DATE()) AS week_start,
                {sample_multiplier}::INTEGER AS sample_multiplier,
                {attach_pct}::FLOAT AS attach_customer_pct
        ),
        slots AS (
            SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS slot_index
            FROM TABLE(GENERATOR(ROWCOUNT => 336)) -- 7 days * 48 half-hour slots
        ),
        slot_times AS (
            SELECT 
                s.slot_index,
                DATEADD('minute', s.slot_index * 30, TO_TIMESTAMP_NTZ((SELECT week_start FROM params))) AS slot_start_ts
            FROM slots s
        ),
        slot_weight AS (
            SELECT 
                slot_index,
                slot_start_ts,
                EXTRACT(DOW FROM slot_start_ts) AS dow,
                EXTRACT(HOUR FROM slot_start_ts) AS hr,
                /* Time-of-day and weekend weighting */
                CASE 
                    WHEN EXTRACT(HOUR FROM slot_start_ts) BETWEEN 0 AND 5 THEN 1
                    WHEN EXTRACT(HOUR FROM slot_start_ts) BETWEEN 6 AND 8 THEN 3
                    WHEN EXTRACT(HOUR FROM slot_start_ts) BETWEEN 9 AND 12 THEN 4
                    WHEN EXTRACT(HOUR FROM slot_start_ts) = 13 THEN 6
                    WHEN EXTRACT(HOUR FROM slot_start_ts) BETWEEN 14 AND 17 THEN 4
                    WHEN EXTRACT(HOUR FROM slot_start_ts) BETWEEN 18 AND 19 THEN 6
                    WHEN EXTRACT(HOUR FROM slot_start_ts) = 20 THEN 8
                    WHEN EXTRACT(HOUR FROM slot_start_ts) BETWEEN 21 AND 22 THEN 12
                    WHEN EXTRACT(HOUR FROM slot_start_ts) = 23 THEN 4
                    ELSE 3
                END * (CASE WHEN EXTRACT(DOW FROM slot_start_ts) IN (0,6) THEN 1.2 ELSE 1 END) AS w
            FROM slot_times
        ),
        slot_cap AS (
            SELECT 
                slot_index,
                slot_start_ts,
                /* base ~100, jitter, scale by weight and multiplier; floor at 20, cap at 600 */
                LEAST(600, GREATEST(20, CAST(ROUND((100 + UNIFORM(0, 30, RANDOM())) * w * (SELECT sample_multiplier FROM params) / 10) AS INTEGER))) AS events_per_slot
            FROM slot_weight
        ),
        events AS (
            SELECT 
                s.slot_index,
                s.slot_start_ts,
                ROW_NUMBER() OVER (PARTITION BY s.slot_index ORDER BY RANDOM()) AS event_seq
            FROM slot_cap s
            JOIN TABLE(GENERATOR(ROWCOUNT => 600)) g
            QUALIFY event_seq <= s.events_per_slot
        ),
        /* Pre-sample a customer pool to map events deterministically */
        cust_pool AS (
            SELECT customer_id, ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
            FROM {CUSTOMER_TABLE}
            SAMPLE (2.5)
        ),
        pool_max AS (
            SELECT COALESCE(MAX(rn), 1) AS mx FROM cust_pool
        ),
        enriched AS (
            SELECT 
                /* Keys */
                UUID_STRING() AS log_id,
                'TF1' AS channel,
                slot_start_ts AS slot_start_time,
                DATEADD('second', UNIFORM(0, 1799, RANDOM()), slot_start_ts) AS event_time,
                /* Stable programme id per slot */
                'TF1-' || TO_CHAR(slot_start_ts, 'YYYYMMDD-HH24MI') AS programme_id,

                /* Attach customer for ~30% */
                CASE WHEN UNIFORM(0,1,RANDOM()) < (SELECT attach_customer_pct FROM params) THEN 1 ELSE 0 END AS attach_flag,
                e.slot_index,
                e.event_seq
            FROM events e
        ),
        with_customer AS (
            SELECT 
                log_id,
                channel,
                slot_start_time,
                event_time,
                programme_id,
                CASE WHEN attach_flag = 1 THEN cp.customer_id ELSE NULL END AS customer_id,
                slot_index,
                event_seq
            FROM enriched
            LEFT JOIN cust_pool cp
              ON ((slot_index * 1000 + event_seq) % (SELECT mx FROM pool_max)) + 1 = cp.rn
        ),
        with_device AS (
            SELECT 
                log_id, channel, slot_start_time, event_time, programme_id, customer_id,
                /* Device type */
                CASE 
                    WHEN UNIFORM(0,1,RANDOM()) < 0.45 THEN 'SmartTV'
                    WHEN UNIFORM(0,1,RANDOM()) < 0.70 THEN 'Mobile'
                    WHEN UNIFORM(0,1,RANDOM()) < 0.85 THEN 'Web'
                    ELSE 'Tablet'
                END AS device_type,
                /* OS by device type (approximate) */
                CASE 
                    WHEN device_type = 'SmartTV' THEN 
                        CASE WHEN UNIFORM(0,1,RANDOM()) < 0.5 THEN 'Tizen' WHEN UNIFORM(0,1,RANDOM()) < 0.8 THEN 'webOS' ELSE 'Android TV' END
                    WHEN device_type = 'Mobile' THEN 
                        CASE WHEN UNIFORM(0,1,RANDOM()) < 0.5 THEN 'Android' ELSE 'iOS' END
                    WHEN device_type = 'Tablet' THEN 
                        CASE WHEN UNIFORM(0,1,RANDOM()) < 0.5 THEN 'Android' ELSE 'iPadOS' END
                    ELSE 'ChromeOS'
                END AS os_name,
                /* Connection type */
                CASE WHEN UNIFORM(0,1,RANDOM()) < 0.7 THEN 'wifi' WHEN UNIFORM(0,1,RANDOM()) < 0.9 THEN 'ethernet' ELSE 'cellular' END AS connection_type,
                /* Bitrate and quality */
                CAST(ROUND(UNIFORM(800, 6500, RANDOM()))) AS bitrate_kbps,
                CAST(ROUND(UNIFORM(0, 5, RANDOM()))) AS buffer_events,
                ROUND(UNIFORM(0, 0.08, RANDOM()), 3) AS rebuffer_ratio,
                CAST(ROUND(UNIFORM(30, 1800, RANDOM()))) AS watch_seconds,
                /* IP address ranges (FR ISPs) */
                CASE 
                    WHEN UNIFORM(0,1,RANDOM()) < 0.4 THEN 
                        '81.' || LPAD(CAST(UNIFORM(48, 63, RANDOM()) AS STRING), 2, '0') || '.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING) || '.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING)
                    WHEN UNIFORM(0,1,RANDOM()) < 0.7 THEN 
                        '82.' || LPAD(CAST(UNIFORM(64, 127, RANDOM()) AS STRING), 3, '0') || '.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING) || '.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING)
                    ELSE 
                        '90.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING) || '.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING) || '.' || CAST(UNIFORM(0,255,RANDOM()) AS STRING)
                END AS ip_address,
                /* ISP name */
                CASE 
                    WHEN LEFT(ip_address, 3) = '81.' THEN 'Orange'
                    WHEN LEFT(ip_address, 3) = '82.' THEN 'Free'
                    ELSE 'Bouygues'
                END AS isp,
                /* Geo */
                'FR' AS country,
                CASE MOD(slot_index, 6)
                    WHEN 0 THEN 'Île-de-France'
                    WHEN 1 THEN 'Auvergne-Rhône-Alpes'
                    WHEN 2 THEN 'Provence-Alpes-Côte d\'Azur'
                    WHEN 3 THEN 'Nouvelle-Aquitaine'
                    WHEN 4 THEN 'Occitanie'
                    ELSE 'Hauts-de-France'
                END AS region,
                CASE MOD(event_seq, 6)
                    WHEN 0 THEN 'Paris'
                    WHEN 1 THEN 'Lyon'
                    WHEN 2 THEN 'Marseille'
                    WHEN 3 THEN 'Bordeaux'
                    WHEN 4 THEN 'Toulouse'
                    ELSE 'Lille'
                END AS city,
                /* Ad metrics */
                CAST(FLOOR(watch_seconds / 180) AS INTEGER) AS ad_breaks,
                CAST(FLOOR(watch_seconds / 180) * 30 AS INTEGER) AS ad_total_seconds,
                /* Event type */
                CASE 
                    WHEN UNIFORM(0,1,RANDOM()) < 0.05 THEN 'play_start'
                    WHEN UNIFORM(0,1,RANDOM()) < 0.80 THEN 'play'
                    WHEN UNIFORM(0,1,RANDOM()) < 0.90 THEN 'pause'
                    WHEN UNIFORM(0,1,RANDOM()) < 0.97 THEN 'seek'
                    ELSE 'play_end'
                END AS event_type,
                /* Semi-structured device details */
                OBJECT_CONSTRUCT(
                    'device_id', UUID_STRING(),
                    'session_id', UUID_STRING(),
                    'app_name', 'TF1+',
                    'app_version', '1.' || CAST(UNIFORM(0, 9, RANDOM()) AS STRING) || '.' || CAST(UNIFORM(0, 9, RANDOM()) AS STRING),
                    'player_version', '4.' || CAST(UNIFORM(0, 4, RANDOM()) AS STRING),
                    'resolution', CASE WHEN bitrate_kbps > 4000 THEN '1920x1080' WHEN bitrate_kbps > 2000 THEN '1280x720' ELSE '854x480' END,
                    'drm', CASE WHEN os_name IN ('Android TV','Android','ChromeOS') THEN 'widevine' WHEN os_name IN ('iOS','iPadOS') THEN 'fairplay' ELSE 'playready' END,
                    'manufacturer', CASE WHEN device_type='SmartTV' THEN 'Samsung' WHEN device_type='Mobile' THEN 'Apple' ELSE 'LG' END,
                    'model', CASE WHEN device_type='SmartTV' THEN 'QE55' WHEN device_type='Mobile' THEN 'iPhone' ELSE 'web' END
                ) AS device
            FROM with_customer
        )
        SELECT 
            log_id,
            channel,
            event_time,
            slot_start_time,
            programme_id,
            customer_id,
            device_type,
            os_name,
            connection_type,
            bitrate_kbps,
            buffer_events,
            rebuffer_ratio,
            watch_seconds,
            ad_breaks,
            ad_total_seconds,
            event_type,
            ip_address,
            isp,
            country,
            region,
            city,
            device
        FROM with_device
    """

    if overwrite:
        _exec(session, f"CREATE OR REPLACE TABLE {full_table} AS " + select_sql)
    else:
        _exec(session, f"CREATE TABLE IF NOT EXISTS {full_table} AS " + select_sql + " WHERE 1=0")
        _exec(session, f"INSERT INTO {full_table} " + select_sql)

    return session.sql(f"SELECT * FROM {full_table} SAMPLE (100 ROWS)")


def sp_entry(
    session: Session,
    sample_multiplier: Optional[int] = None,
    attach_customer_pct: Optional[float] = None,
    overwrite: bool = True,
):
    if sample_multiplier is None:
        sample_multiplier = 1
    if attach_customer_pct is None:
        attach_customer_pct = 0.30
    return run(session, sample_multiplier=sample_multiplier, attach_customer_pct=attach_customer_pct, overwrite=overwrite)


