CREATE DATABASE IF NOT EXISTS SF1PLUS_DB;
CREATE SCHEMA IF NOT EXISTS SF1PLUS_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS SF1PLUS_DB.RAW_DATA;


USE SCHEMA SF1PLUS_DB.BRONZE;

CREATE OR REPLACE STAGE CODE_STAGE
  COMMENT = 'Stage for sf1plus Snowpark code';

-- Verify stage
SHOW STAGES;

-- 3) Upload the Python file to the stage (run in SnowSQL or a client that supports PUT)
-- If using SnowSQL on your Mac, this absolute path should work:
-- Replace account/region/role/warehouse context as needed before running PUT.
PUT file:///Users/edendulk/code/sf1plus/snowpark/sf1plus_crm_generator.py
  @SF1PLUS_DB.BRONZE.CODE_STAGE
  AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify file landed
LIST @SF1PLUS_DB.BRONZE.CODE_STAGE;

USE SCHEMA SF1PLUS_DB.RAW_DATA;

CREATE OR REPLACE PROCEDURE SF1PLUS_DB.RAW_DATA.GENERATE_SF1PLUS_CRM(target_rows NUMBER, overwrite BOOLEAN)
RETURNS TABLE (
  customer_id STRING,
  email STRING,
  phone STRING,
  first_name STRING,
  last_name STRING,
  gender STRING,
  profession STRING,
  date_joined DATE,
  subscription_level STRING,
  overlap_type STRING
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
IMPORTS = ('@SF1PLUS_DB.BRONZE.CODE_STAGE/sf1plus_crm_generator.py')
HANDLER = 'sf1plus_crm_generator.sp_entry';

CALL SF1PLUS_DB.RAW_DATA.GENERATE_SF1PLUS_CRM(4000000, TRUE);

SELECT COUNT(*) AS row_count
FROM SF1PLUS_DB.RAW_DATA.SF1PLUS_CRM;

SELECT 
  COUNT(*)::FLOAT / (SELECT COUNT(*) FROM SF1PLUS_DB.RAW_DATA.SF1PLUS_CRM) AS overlap_ratio
FROM SF1PLUS_DB.RAW_DATA.SF1PLUS_CRM c
WHERE EXISTS (
  SELECT 1
  FROM CROCEVIA_DB.RAW_DATA.CROCEVIA_CRM s
  WHERE s.EMAIL = c.email OR s.PHONE = c.phone
);