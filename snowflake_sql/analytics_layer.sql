-- =====================================================
-- SF1+ Analytics Layer - GOLD Schema Setup
-- =====================================================
-- Creates analytical views and tables for SF1+ streaming platform
-- Combines CRM data with viewing logs for customer insights

USE ROLE <your_role>;
USE WAREHOUSE <your_wh>;
USE DATABASE SF1PLUS_DB;

-- =====================================================
-- 1. CREATE GOLD SCHEMA
-- =====================================================

CREATE SCHEMA IF NOT EXISTS SF1PLUS_DB.GOLD
    COMMENT = 'Analytics and business intelligence layer for SF1+ streaming platform';

USE SCHEMA SF1PLUS_DB.GOLD;

-- =====================================================
-- 2. CUSTOMER 360 VIEW
-- =====================================================
-- Comprehensive customer view combining CRM and viewing behavior

CREATE OR REPLACE VIEW CUSTOMER_360 AS
WITH customer_base AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        gender,
        profession,
        date_joined,
        subscription_level,
        CASE 
            WHEN overlap_type IN ('TRIPLE', 'EMAIL', 'PHONE', 'NAME') THEN 'Cross-Platform'
            ELSE 'SF1+ Only'
        END AS customer_type,
        overlap_type
    FROM SF1PLUS_DB.RAW_DATA.SF1PLUS_CRM
),
viewing_summary AS (
    SELECT 
        customer_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT DATE(event_time)) AS active_days,
        COUNT(DISTINCT programme_id) AS unique_programmes_watched,
        SUM(watch_seconds) AS total_watch_seconds,
        AVG(watch_seconds) AS avg_watch_seconds,
        SUM(ad_total_seconds) AS total_ad_seconds,
        
        -- Device preferences
        MODE(device_type) AS preferred_device,
        COUNT(DISTINCT device_type) AS device_types_used,
        
        -- Connection patterns
        MODE(connection_type) AS preferred_connection,
        AVG(bitrate_kbps) AS avg_bitrate,
        AVG(buffer_events) AS avg_buffer_events,
        AVG(rebuffer_ratio) AS avg_rebuffer_ratio,
        
        -- Geographic info (latest)
        LAST_VALUE(region) OVER (PARTITION BY customer_id ORDER BY event_time) AS region,
        LAST_VALUE(city) OVER (PARTITION BY customer_id ORDER BY event_time) AS city,
        LAST_VALUE(isp) OVER (PARTITION BY customer_id ORDER BY event_time) AS isp,
        
        -- Engagement metrics
        MIN(event_time) AS first_viewing_date,
        MAX(event_time) AS last_viewing_date,
        DATEDIFF('day', MIN(event_time), MAX(event_time)) + 1 AS viewing_span_days
        
    FROM SF1PLUS_DB.RAW_DATA.TF1_VIEWING_LOGS_HIGH_VOLUME
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
),
engagement_scores AS (
    SELECT 
        customer_id,
        -- Engagement score (0-100)
        LEAST(100, 
            (total_watch_seconds / 3600.0) * 2 +  -- Hours watched * 2
            active_days * 3 +                      -- Active days * 3  
            unique_programmes_watched * 0.5        -- Programme variety * 0.5
        ) AS engagement_score,
        
        -- Customer lifetime value proxy
        CASE subscription_level
            WHEN 'FREE' THEN 0
            WHEN 'BASIC' THEN 4.99
            WHEN 'STANDARD' THEN 9.99
            WHEN 'PREMIUM' THEN 14.99
        END * GREATEST(1, viewing_span_days / 30.0) AS estimated_clv,
        
        -- Viewing intensity
        CASE 
            WHEN total_watch_seconds / 3600.0 >= 50 THEN 'Heavy Viewer'
            WHEN total_watch_seconds / 3600.0 >= 20 THEN 'Regular Viewer'
            WHEN total_watch_seconds / 3600.0 >= 5 THEN 'Light Viewer'
            ELSE 'Minimal Viewer'
        END AS viewer_segment
        
    FROM viewing_summary v
    JOIN customer_base c ON v.customer_id = c.customer_id
)
SELECT 
    -- Customer Identity
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.gender,
    c.profession,
    c.subscription_level,
    c.customer_type,
    c.date_joined,
    
    -- Viewing Behavior
    COALESCE(v.total_events, 0) AS total_events,
    COALESCE(v.active_days, 0) AS active_days,
    COALESCE(v.unique_programmes_watched, 0) AS unique_programmes_watched,
    COALESCE(v.total_watch_seconds / 3600.0, 0) AS total_watch_hours,
    COALESCE(v.avg_watch_seconds, 0) AS avg_watch_seconds,
    COALESCE(v.total_ad_seconds / 60.0, 0) AS total_ad_minutes,
    
    -- Technical Profile
    v.preferred_device,
    v.device_types_used,
    v.preferred_connection,
    v.avg_bitrate,
    v.avg_buffer_events,
    v.avg_rebuffer_ratio,
    
    -- Geographic
    v.region,
    v.city,
    v.isp,
    
    -- Engagement Metrics
    v.first_viewing_date,
    v.last_viewing_date,
    v.viewing_span_days,
    COALESCE(e.engagement_score, 0) AS engagement_score,
    COALESCE(e.estimated_clv, 0) AS estimated_clv,
    COALESCE(e.viewer_segment, 'No Activity') AS viewer_segment,
    
    -- Activity Status
    CASE 
        WHEN v.last_viewing_date IS NULL THEN 'Never Active'
        WHEN DATEDIFF('day', v.last_viewing_date, CURRENT_DATE()) <= 7 THEN 'Active'
        WHEN DATEDIFF('day', v.last_viewing_date, CURRENT_DATE()) <= 30 THEN 'Recently Active'
        ELSE 'Inactive'
    END AS activity_status

FROM customer_base c
LEFT JOIN viewing_summary v ON c.customer_id = v.customer_id
LEFT JOIN engagement_scores e ON c.customer_id = e.customer_id;

-- =====================================================
-- 3. VIEWING ANALYTICS VIEWS
-- =====================================================

-- Programme Performance Analytics
CREATE OR REPLACE VIEW PROGRAMME_ANALYTICS AS
SELECT 
    programme_id,
    DATE(slot_start_time) AS programme_date,
    HOUR(slot_start_time) AS programme_hour,
    
    -- Viewership Metrics
    COUNT(*) AS total_events,
    COUNT(DISTINCT customer_id) AS unique_viewers,
    COUNT(DISTINCT EXTRACT(HOUR FROM event_time)) AS hours_with_activity,
    
    -- Engagement Metrics
    SUM(watch_seconds) AS total_watch_seconds,
    AVG(watch_seconds) AS avg_watch_seconds,
    MEDIAN(watch_seconds) AS median_watch_seconds,
    
    -- Quality Metrics
    AVG(bitrate_kbps) AS avg_bitrate,
    AVG(buffer_events) AS avg_buffer_events,
    AVG(rebuffer_ratio) AS avg_rebuffer_ratio,
    
    -- Ad Performance
    SUM(ad_total_seconds) AS total_ad_seconds,
    AVG(ad_breaks) AS avg_ad_breaks,
    
    -- Device Distribution
    COUNT_IF(device_type = 'SmartTV') AS smarttv_viewers,
    COUNT_IF(device_type = 'Mobile') AS mobile_viewers,
    COUNT_IF(device_type = 'Web') AS web_viewers,
    COUNT_IF(device_type = 'Tablet') AS tablet_viewers,
    
    -- Geographic Distribution (top regions)
    MODE(region) AS top_region,
    COUNT(DISTINCT region) AS regions_reached,
    
    -- Event Type Distribution
    COUNT_IF(event_type = 'play_start') AS play_starts,
    COUNT_IF(event_type = 'play_end') AS play_ends,
    COUNT_IF(event_type = 'pause') AS pauses,
    COUNT_IF(event_type = 'seek') AS seeks,
    
    -- Completion Rate Proxy
    COUNT_IF(event_type = 'play_end')::FLOAT / NULLIF(COUNT_IF(event_type = 'play_start'), 0) AS completion_rate

FROM SF1PLUS_DB.RAW_DATA.TF1_VIEWING_LOGS_HIGH_VOLUME
GROUP BY programme_id, programme_date, programme_hour
ORDER BY programme_date DESC, programme_hour;

-- Peak Times Analytics
CREATE OR REPLACE VIEW PEAK_TIMES_ANALYTICS AS
SELECT 
    DATE(event_time) AS viewing_date,
    EXTRACT(HOUR FROM event_time) AS viewing_hour,
    EXTRACT(DOW FROM event_time) AS day_of_week,
    CASE EXTRACT(DOW FROM event_time)
        WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' 
        WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday'
    END AS day_name,
    
    -- Activity Metrics
    COUNT(*) AS total_events,
    COUNT(DISTINCT customer_id) AS unique_viewers,
    COUNT(DISTINCT programme_id) AS unique_programmes,
    
    -- Engagement
    SUM(watch_seconds) AS total_watch_seconds,
    AVG(watch_seconds) AS avg_watch_seconds,
    
    -- Quality
    AVG(bitrate_kbps) AS avg_bitrate,
    AVG(buffer_events) AS avg_buffer_events,
    
    -- Device Mix
    COUNT_IF(device_type = 'SmartTV')::FLOAT / COUNT(*) AS smarttv_pct,
    COUNT_IF(device_type = 'Mobile')::FLOAT / COUNT(*) AS mobile_pct,
    COUNT_IF(device_type = 'Web')::FLOAT / COUNT(*) AS web_pct,
    COUNT_IF(device_type = 'Tablet')::FLOAT / COUNT(*) AS tablet_pct

FROM SF1PLUS_DB.RAW_DATA.TF1_VIEWING_LOGS_HIGH_VOLUME
GROUP BY viewing_date, viewing_hour, day_of_week
ORDER BY viewing_date DESC, viewing_hour;

-- Device Preferences Analytics
CREATE OR REPLACE VIEW DEVICE_ANALYTICS AS
SELECT 
    device_type,
    os_name,
    connection_type,
    
    -- Usage Metrics
    COUNT(*) AS total_events,
    COUNT(DISTINCT customer_id) AS unique_users,
    COUNT(DISTINCT programme_id) AS unique_programmes,
    
    -- Engagement
    SUM(watch_seconds) AS total_watch_seconds,
    AVG(watch_seconds) AS avg_watch_seconds,
    
    -- Technical Performance
    AVG(bitrate_kbps) AS avg_bitrate,
    AVG(buffer_events) AS avg_buffer_events,
    AVG(rebuffer_ratio) AS avg_rebuffer_ratio,
    
    -- User Demographics (where available)
    COUNT_IF(customer_id IS NOT NULL) AS identified_users,
    COUNT_IF(customer_id IS NOT NULL)::FLOAT / COUNT(*) AS identification_rate,
    
    -- Geographic Distribution
    COUNT(DISTINCT region) AS regions_used,
    MODE(region) AS top_region,
    
    -- Time Patterns
    MODE(EXTRACT(HOUR FROM event_time)) AS peak_hour,
    COUNT(DISTINCT DATE(event_time)) AS active_days

FROM SF1PLUS_DB.RAW_DATA.TF1_VIEWING_LOGS_HIGH_VOLUME
GROUP BY device_type, os_name, connection_type
ORDER BY total_events DESC;

-- =====================================================
-- 4. BUSINESS INTELLIGENCE SUMMARY TABLES
-- =====================================================

-- Daily Summary Table (Materialized for Performance)
CREATE OR REPLACE TABLE DAILY_SUMMARY AS
SELECT 
    DATE(event_time) AS summary_date,
    
    -- Core Metrics
    COUNT(*) AS total_events,
    COUNT(DISTINCT customer_id) AS unique_viewers,
    COUNT(DISTINCT programme_id) AS unique_programmes,
    
    -- Engagement
    SUM(watch_seconds) / 3600.0 AS total_watch_hours,
    AVG(watch_seconds) AS avg_watch_seconds,
    
    -- Revenue Proxy (ad minutes)
    SUM(ad_total_seconds) / 60.0 AS total_ad_minutes,
    
    -- Quality Metrics
    AVG(bitrate_kbps) AS avg_bitrate,
    AVG(buffer_events) AS avg_buffer_events,
    AVG(rebuffer_ratio) AS avg_rebuffer_ratio,
    
    -- Device Distribution
    COUNT_IF(device_type = 'SmartTV')::FLOAT / COUNT(*) AS smarttv_share,
    COUNT_IF(device_type = 'Mobile')::FLOAT / COUNT(*) AS mobile_share,
    COUNT_IF(device_type = 'Web')::FLOAT / COUNT(*) AS web_share,
    COUNT_IF(device_type = 'Tablet')::FLOAT / COUNT(*) AS tablet_share,
    
    -- Customer Segmentation
    COUNT_IF(customer_id IS NOT NULL)::FLOAT / COUNT(*) AS identified_viewer_rate,
    
    CURRENT_TIMESTAMP() AS last_updated

FROM SF1PLUS_DB.RAW_DATA.TF1_VIEWING_LOGS_HIGH_VOLUME
GROUP BY DATE(event_time)
ORDER BY summary_date DESC;

-- =====================================================
-- 5. SAMPLE ANALYTICAL QUERIES
-- =====================================================

-- Top Customers by Engagement
CREATE OR REPLACE VIEW TOP_CUSTOMERS AS
SELECT 
    customer_id,
    first_name,
    last_name,
    subscription_level,
    viewer_segment,
    engagement_score,
    total_watch_hours,
    unique_programmes_watched,
    preferred_device,
    region,
    estimated_clv
FROM CUSTOMER_360
WHERE customer_id IS NOT NULL
ORDER BY engagement_score DESC
LIMIT 100;

-- Programme Performance Ranking
CREATE OR REPLACE VIEW TOP_PROGRAMMES AS
SELECT 
    programme_id,
    programme_date,
    programme_hour,
    unique_viewers,
    total_watch_seconds / 3600.0 AS total_watch_hours,
    completion_rate,
    avg_bitrate,
    top_region
FROM PROGRAMME_ANALYTICS
WHERE programme_date >= CURRENT_DATE() - 7  -- Last 7 days
ORDER BY unique_viewers DESC
LIMIT 50;

-- Peak Viewing Hours
CREATE OR REPLACE VIEW PEAK_HOURS AS
SELECT 
    viewing_hour,
    day_name,
    AVG(unique_viewers) AS avg_unique_viewers,
    AVG(total_watch_seconds) / 3600.0 AS avg_watch_hours,
    AVG(smarttv_pct) AS avg_smarttv_share,
    AVG(mobile_pct) AS avg_mobile_share
FROM PEAK_TIMES_ANALYTICS
WHERE viewing_date >= CURRENT_DATE() - 7
GROUP BY viewing_hour, day_name, day_of_week
ORDER BY day_of_week, viewing_hour;

-- =====================================================
-- 6. GRANTS AND PERMISSIONS
-- =====================================================

-- Grant read access to analytics role (adjust as needed)
-- GRANT USAGE ON SCHEMA SF1PLUS_DB.GOLD TO ROLE ANALYTICS_ROLE;
-- GRANT SELECT ON ALL VIEWS IN SCHEMA SF1PLUS_DB.GOLD TO ROLE ANALYTICS_ROLE;
-- GRANT SELECT ON ALL TABLES IN SCHEMA SF1PLUS_DB.GOLD TO ROLE ANALYTICS_ROLE;

-- =====================================================
-- VALIDATION QUERIES
-- =====================================================

-- Validate Customer 360
SELECT 
    viewer_segment,
    COUNT(*) AS customers,
    AVG(engagement_score) AS avg_engagement,
    AVG(total_watch_hours) AS avg_watch_hours
FROM CUSTOMER_360 
GROUP BY viewer_segment
ORDER BY avg_engagement DESC;

-- Validate Daily Summary
SELECT * FROM DAILY_SUMMARY ORDER BY summary_date DESC LIMIT 10;

-- Validate Device Analytics
SELECT 
    device_type,
    COUNT(*) AS events,
    unique_users,
    avg_bitrate,
    avg_rebuffer_ratio
FROM DEVICE_ANALYTICS 
ORDER BY events DESC;

SHOW VIEWS IN SCHEMA SF1PLUS_DB.GOLD;
