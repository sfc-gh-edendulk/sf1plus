"""
TF1 Viewing Logs Generator - High Volume Version (Snowpark)

Generates millions of TF1 viewing logs efficiently for large-scale analytics.
Optimized for high throughput with simplified logic.
"""

from typing import Optional
from snowflake.snowpark import Session


TARGET_DB = "SF1PLUS_DB"
RAW_SCHEMA = "RAW_DATA"
OUTPUT_TABLE = "TF1_VIEWING_LOGS_HIGH_VOLUME"
CUSTOMER_TABLE = f"{TARGET_DB}.{RAW_SCHEMA}.SF1PLUS_CRM"


def _exec(session: Session, sql: str) -> None:
    session.sql(sql).collect()


def run(
    session: Session,
    total_events: int = 5_000_000,
    attach_customer_pct: float = 0.30,
    overwrite: bool = True,
) -> "Session":
    """
    Build high-volume TF1 viewing logs.
    
    Args:
        total_events: Total number of events to generate (default 5M)
        attach_customer_pct: Fraction with customer_id (default 0.30)
        overwrite: Replace existing table
    """
    _exec(session, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    _exec(session, f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{RAW_SCHEMA}")

    full_table = f"{TARGET_DB}.{RAW_SCHEMA}.{OUTPUT_TABLE}"
    attach_pct = max(0.0, min(1.0, float(attach_customer_pct)))

    # Create customer mapping for joins - get enough customers for proper cycling
    temp_customer_map = f"{TARGET_DB}.{RAW_SCHEMA}.TEMP_CUSTOMER_MAP_HV"
    _exec(session, f"""
        CREATE OR REPLACE TABLE {temp_customer_map} AS
        SELECT customer_id, ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rn
        FROM {CUSTOMER_TABLE}
        SAMPLE (50)  -- Get more customers for better coverage
    """)

    # High-volume generation with efficient patterns
    select_sql = f"""
        WITH base_events AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY SEQ4()) AS event_id,
                -- Spread events over 4 weeks for more variety
                DATEADD('second', 
                    (ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1) * 2, 
                    DATE_TRUNC('WEEK', CURRENT_DATE()) - INTERVAL '21 days'
                ) AS event_time
            FROM TABLE(GENERATOR(ROWCOUNT => {total_events}))
        ),
        customer_map AS (
            SELECT customer_id, rn FROM {temp_customer_map}
        ),
        customer_count AS (
            SELECT COUNT(*) AS total_customers FROM customer_map
        ),
        events_enriched AS (
            SELECT 
                e.event_id,
                e.event_time,
                -- Customer attachment (30% get real IDs) - cycle through available customers
                CASE WHEN (e.event_id % 100) < ({attach_pct} * 100) 
                     THEN c.customer_id 
                     ELSE NULL END AS customer_id
            FROM base_events e
            CROSS JOIN customer_count cc
            LEFT JOIN customer_map c ON c.rn = ((e.event_id % cc.total_customers) + 1)
        ),
        final AS (
            SELECT 
                CAST(UUID_STRING() AS STRING) AS log_id,
                CAST('TF1' AS STRING) AS channel,
                CAST(event_time AS TIMESTAMP_NTZ) AS event_time,
                CAST(DATE_TRUNC('hour', event_time) AS TIMESTAMP_NTZ) AS slot_start_time,
                CAST('TF1-' || TO_CHAR(event_time, 'YYYYMMDD-HH24') AS STRING) AS programme_id,
                CAST(customer_id AS STRING) AS customer_id,
                
                -- Device patterns (simplified for performance)
                CAST(CASE (event_id % 4)
                    WHEN 0 THEN 'SmartTV' WHEN 1 THEN 'Mobile' 
                    WHEN 2 THEN 'Web' ELSE 'Tablet'
                END AS STRING) AS device_type,
                
                CAST(CASE (event_id % 4)
                    WHEN 0 THEN 'Tizen' WHEN 1 THEN 'iOS' 
                    WHEN 2 THEN 'ChromeOS' ELSE 'Android'
                END AS STRING) AS os_name,
                
                CAST(CASE (event_id % 3)
                    WHEN 0 THEN 'wifi' WHEN 1 THEN 'ethernet' ELSE 'cellular'
                END AS STRING) AS connection_type,
                
                -- QoE metrics
                CAST(1000 + (event_id % 5000) AS NUMBER) AS bitrate_kbps,
                CAST(event_id % 8 AS NUMBER) AS buffer_events,
                CAST(ROUND((event_id % 50) / 1000.0, 3) AS FLOAT) AS rebuffer_ratio,
                CAST(60 + (event_id % 1200) AS NUMBER) AS watch_seconds,
                CAST(FLOOR((60 + (event_id % 1200)) / 180) AS NUMBER) AS ad_breaks,
                CAST(FLOOR((60 + (event_id % 1200)) / 180) * 30 AS NUMBER) AS ad_total_seconds,
                
                -- Event type
                CAST(CASE (event_id % 10)
                    WHEN 0 THEN 'play_start' WHEN 9 THEN 'play_end'
                    WHEN 8 THEN 'pause' WHEN 7 THEN 'seek' ELSE 'play'
                END AS STRING) AS event_type,
                
                -- French IP ranges
                CAST(CASE (event_id % 3)
                    WHEN 0 THEN '81.' || (50 + (event_id % 14))::STRING || '.' || 
                               (event_id % 256)::STRING || '.' || ((event_id * 7) % 256)::STRING
                    WHEN 1 THEN '82.' || (70 + (event_id % 50))::STRING || '.' || 
                               (event_id % 256)::STRING || '.' || ((event_id * 11) % 256)::STRING
                    ELSE '90.' || (event_id % 256)::STRING || '.' || 
                         ((event_id * 3) % 256)::STRING || '.' || ((event_id * 13) % 256)::STRING
                END AS STRING) AS ip_address,
                
                CAST(CASE (event_id % 3)
                    WHEN 0 THEN 'Orange' WHEN 1 THEN 'Free' ELSE 'Bouygues'
                END AS STRING) AS isp,
                
                CAST('FR' AS STRING) AS country,
                
                CAST(CASE (event_id % 8)
                    WHEN 0 THEN 'Île-de-France' WHEN 1 THEN 'Auvergne-Rhône-Alpes'
                    WHEN 2 THEN 'Provence-Alpes-Côte d''Azur' WHEN 3 THEN 'Nouvelle-Aquitaine'
                    WHEN 4 THEN 'Occitanie' WHEN 5 THEN 'Hauts-de-France'
                    WHEN 6 THEN 'Grand Est' ELSE 'Normandie'
                END AS STRING) AS region,
                
                CAST(CASE (event_id % 10)
                    WHEN 0 THEN 'Paris' WHEN 1 THEN 'Lyon' WHEN 2 THEN 'Marseille'
                    WHEN 3 THEN 'Toulouse' WHEN 4 THEN 'Nice' WHEN 5 THEN 'Nantes'
                    WHEN 6 THEN 'Strasbourg' WHEN 7 THEN 'Montpellier' 
                    WHEN 8 THEN 'Bordeaux' ELSE 'Lille'
                END AS STRING) AS city,
                
                -- Simplified device metadata for performance
                CAST(OBJECT_CONSTRUCT(
                    'device_id', 'dev_' || (event_id % 100000)::STRING,
                    'session_id', 'sess_' || (event_id % 10000)::STRING,
                    'app_name', 'TF1+',
                    'app_version', '2.' || (event_id % 5)::STRING,
                    'player_version', '5.' || (event_id % 3)::STRING,
                    'resolution', CASE WHEN (1000 + (event_id % 5000)) > 3000 THEN '1920x1080' 
                                      WHEN (1000 + (event_id % 5000)) > 1500 THEN '1280x720' 
                                      ELSE '854x480' END,
                    'manufacturer', CASE (event_id % 4)
                                       WHEN 0 THEN 'Samsung' WHEN 1 THEN 'Apple'
                                       WHEN 2 THEN 'LG' ELSE 'Sony' END
                ) AS VARIANT) AS device
                
            FROM events_enriched
        )
        SELECT * FROM final
    """

    if overwrite:
        _exec(session, f"CREATE OR REPLACE TABLE {full_table} AS " + select_sql)
    else:
        _exec(session, f"CREATE TABLE IF NOT EXISTS {full_table} AS " + select_sql + " WHERE 1=0")
        _exec(session, f"INSERT INTO {full_table} " + select_sql)

    # Clean up
    _exec(session, f"DROP TABLE IF EXISTS {temp_customer_map}")

    return session.sql(f"SELECT * FROM {full_table} SAMPLE (100 ROWS)")


def sp_entry(
    session: Session,
    total_events: Optional[int] = None,
    attach_customer_pct: Optional[float] = None,
    overwrite: bool = True,
):
    if total_events is None:
        total_events = 5_000_000
    if attach_customer_pct is None:
        attach_customer_pct = 0.30
    return run(session, total_events=total_events, attach_customer_pct=attach_customer_pct, overwrite=overwrite)
