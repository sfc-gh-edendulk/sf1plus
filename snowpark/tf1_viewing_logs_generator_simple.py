"""
TF1 Viewing Logs Generator - Simplified Version (Snowpark)

Generates realistic TF1 viewing logs using a simpler approach that avoids
complex SQL compilation issues in stored procedures.
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
    Build TF1 viewing logs for one week using a simplified approach.
    """
    _exec(session, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    _exec(session, f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{RAW_SCHEMA}")

    full_table = f"{TARGET_DB}.{RAW_SCHEMA}.{OUTPUT_TABLE}"
    attach_pct = max(0.0, min(1.0, float(attach_customer_pct)))
    
    # Create a temporary customer mapping table to work around subquery limitations
    temp_customer_map = f"{TARGET_DB}.{RAW_SCHEMA}.TEMP_CUSTOMER_MAP"
    _exec(session, f"""
        CREATE OR REPLACE TABLE {temp_customer_map} AS
        SELECT customer_id, ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
        FROM {CUSTOMER_TABLE}
        SAMPLE (10)
    """)

    # Ultra-simple approach: no subqueries, just deterministic patterns
    select_sql = f"""
        WITH base_events AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY SEQ4()) AS event_id,
                DATEADD('minute', 
                    (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 5, 
                    DATE_TRUNC('WEEK', CURRENT_DATE())
                ) AS event_time
            FROM TABLE(GENERATOR(ROWCOUNT => {2016 * sample_multiplier}))
        ),
        customer_map AS (
            SELECT customer_id, rn FROM {temp_customer_map}
        ),
        events_with_customer_flag AS (
            SELECT 
                event_id,
                event_time,
                CASE WHEN (event_id % 100) < ({attach_pct} * 100) THEN 1 ELSE 0 END AS should_attach_customer,
                ((event_id % 1000) + 1) AS customer_rn_target
            FROM base_events
        ),
        enriched AS (
            SELECT 
                UUID_STRING() AS log_id,
                'TF1' AS channel,
                e.event_time,
                DATE_TRUNC('hour', e.event_time) AS slot_start_time,
                'TF1-' || TO_CHAR(e.event_time, 'YYYYMMDD-HH24MI') AS programme_id,
                
                -- Use actual customer IDs via LEFT JOIN
                CASE WHEN e.should_attach_customer = 1 THEN c.customer_id ELSE NULL END AS customer_id,
                
                -- Device type based on event_id modulo
                CASE (e.event_id % 4)
                    WHEN 0 THEN 'SmartTV'
                    WHEN 1 THEN 'Mobile'
                    WHEN 2 THEN 'Web'
                    ELSE 'Tablet'
                END AS device_type,
                
                -- OS based on device type
                CASE 
                    WHEN (e.event_id % 4) = 0 THEN 
                        CASE (e.event_id % 3) WHEN 0 THEN 'Tizen' WHEN 1 THEN 'webOS' ELSE 'Android TV' END
                    WHEN (e.event_id % 4) = 1 THEN 
                        CASE (e.event_id % 2) WHEN 0 THEN 'Android' ELSE 'iOS' END
                    WHEN (e.event_id % 4) = 2 THEN 'ChromeOS'
                    ELSE 
                        CASE (e.event_id % 2) WHEN 0 THEN 'Android' ELSE 'iPadOS' END
                END AS os_name,
                
                -- Connection type
                CASE (e.event_id % 3)
                    WHEN 0 THEN 'wifi'
                    WHEN 1 THEN 'ethernet'
                    ELSE 'cellular'
                END AS connection_type,
                
                -- QoE metrics
                800 + (e.event_id % 5700) AS bitrate_kbps,
                e.event_id % 6 AS buffer_events,
                ROUND((e.event_id % 80) / 1000.0, 3) AS rebuffer_ratio,
                30 + (e.event_id % 1770) AS watch_seconds,
                
                -- Event type
                CASE (e.event_id % 20)
                    WHEN 0 THEN 'play_start'
                    WHEN 19 THEN 'play_end'
                    WHEN 18 THEN 'pause'
                    WHEN 17 THEN 'seek'
                    ELSE 'play'
                END AS event_type,
                
                -- IP and geo
                CASE (e.event_id % 3)
                    WHEN 0 THEN '81.' || LPAD((48 + (e.event_id % 16))::STRING, 2, '0') || '.' || 
                               ((e.event_id % 256))::STRING || '.' || ((e.event_id * 7) % 256)::STRING
                    WHEN 1 THEN '82.' || LPAD((64 + (e.event_id % 64))::STRING, 3, '0') || '.' || 
                               ((e.event_id % 256))::STRING || '.' || ((e.event_id * 7) % 256)::STRING
                    ELSE '90.' || ((e.event_id % 256))::STRING || '.' || 
                         ((e.event_id * 3) % 256)::STRING || '.' || ((e.event_id * 7) % 256)::STRING
                END AS ip_address,
                
                CASE (e.event_id % 3)
                    WHEN 0 THEN 'Orange'
                    WHEN 1 THEN 'Free'
                    ELSE 'Bouygues'
                END AS isp,
                
                'FR' AS country,
                
                CASE (e.event_id % 6)
                    WHEN 0 THEN 'Île-de-France'
                    WHEN 1 THEN 'Auvergne-Rhône-Alpes'
                    WHEN 2 THEN 'Provence-Alpes-Côte d''Azur'
                    WHEN 3 THEN 'Nouvelle-Aquitaine'
                    WHEN 4 THEN 'Occitanie'
                    ELSE 'Hauts-de-France'
                END AS region,
                
                CASE (e.event_id % 6)
                    WHEN 0 THEN 'Paris'
                    WHEN 1 THEN 'Lyon'
                    WHEN 2 THEN 'Marseille'
                    WHEN 3 THEN 'Bordeaux'
                    WHEN 4 THEN 'Toulouse'
                    ELSE 'Lille'
                END AS city,
                
                e.event_id
            FROM events_with_customer_flag e
            LEFT JOIN customer_map c ON c.rn = e.customer_rn_target
        ),
        final AS (
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
                CAST(FLOOR(watch_seconds / 180) AS INTEGER) AS ad_breaks,
                CAST(FLOOR(watch_seconds / 180) * 30 AS INTEGER) AS ad_total_seconds,
                event_type,
                ip_address,
                isp,
                country,
                region,
                city,
                OBJECT_CONSTRUCT(
                    'device_id', UUID_STRING(),
                    'session_id', UUID_STRING(),
                    'app_name', 'TF1+',
                    'app_version', '1.' || (event_id % 10)::STRING || '.' || ((event_id * 3) % 10)::STRING,
                    'player_version', '4.' || (event_id % 5)::STRING,
                    'resolution', CASE WHEN bitrate_kbps > 4000 THEN '1920x1080' 
                                      WHEN bitrate_kbps > 2000 THEN '1280x720' 
                                      ELSE '854x480' END,
                    'drm', CASE WHEN os_name IN ('Android TV','Android','ChromeOS') THEN 'widevine' 
                                WHEN os_name IN ('iOS','iPadOS') THEN 'fairplay' 
                                ELSE 'playready' END,
                    'manufacturer', CASE WHEN device_type='SmartTV' THEN 'Samsung' 
                                         WHEN device_type='Mobile' THEN 'Apple' 
                                         ELSE 'LG' END,
                    'model', CASE WHEN device_type='SmartTV' THEN 'QE55' 
                                  WHEN device_type='Mobile' THEN 'iPhone' 
                                  ELSE 'web' END
                ) AS device
            FROM enriched
        )
        SELECT * FROM final
    """

    if overwrite:
        _exec(session, f"CREATE OR REPLACE TABLE {full_table} AS " + select_sql)
    else:
        _exec(session, f"CREATE TABLE IF NOT EXISTS {full_table} AS " + select_sql + " WHERE 1=0")
        _exec(session, f"INSERT INTO {full_table} " + select_sql)

    # Clean up temp table
    _exec(session, f"DROP TABLE IF EXISTS {temp_customer_map}")

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
