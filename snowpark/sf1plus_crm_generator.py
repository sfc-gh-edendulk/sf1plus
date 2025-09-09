"""
SF1+ Streaming CRM Generator (Snowpark)

Generates a synthetic TV streaming CRM with 4,000,000 rows, French localization, 
and 25% overlap with CROCEVIA_DB.RAW_DATA.CROCEVIA_CRM using deterministic rules
and controlled data messiness (missing fields, minor mutations, duplicates).

This module is designed to run inside Snowflake as a stored procedure import.
The main entrypoint is the function `run(session, target_rows, overwrite)`.
"""

from typing import Optional
import uuid

from snowflake.snowpark import Session


# Configuration (schemas align to user's standards: stage in BRONZE, tables in RAW_DATA)
TARGET_DB = "SF1PLUS_DB"
RAW_SCHEMA = "RAW_DATA"
BRONZE_SCHEMA = "BRONZE"
OUTPUT_TABLE = "SF1PLUS_CRM"
SOURCE_TABLE = "CROCEVIA_DB.RAW_DATA.CROCEVIA_CRM"


def _exec(session: Session, sql: str) -> None:
    session.sql(sql).collect()


def run(session: Session, target_rows: int = 4_000_000, overwrite: bool = True) -> "Session":
    """
    Build SF1+ CRM table with 4M rows and ~25% overlap to CROCEVIA CRM.

    - 90% base records, 10% duplicates with slight mutations => exactly 4,000,000 rows
    - Overlap split on base records (~25%): ~8% triple, ~10% email-only, ~7% phone-only
    - Missingness: 15% emails missing (outside email-overlap), 20% phones missing (outside phone-overlap)

    Returns a Snowpark DataFrame preview (100 rows) for stored procedure TABLE return.
    """
    # Create DB and schemas
    _exec(session, f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")
    _exec(session, f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{RAW_SCHEMA}")

    # Parameters
    base_count = int(round(target_rows * 0.90))
    dup_count = target_rows - base_count

    # We want 25% of TOTAL rows to overlap with CROCEVIA (exactly ~1,000,000 of 4,000,000)
    # Since duplicates (10%) should not alter overlap totals, assign ALL overlap on the 90% base.
    # Therefore, overlap on base must be (target_rows * 0.25) distributed across TRIPLE/EMAIL/PHONE.
    overlap_total_target = int(round(target_rows * 0.25))
    # Distribute with proportions 8:10:7 (sum=25)
    triple_share, email_share, phone_share = 8, 10, 7
    sum_share = triple_share + email_share + phone_share
    triple_count = int(round(overlap_total_target * triple_share / sum_share))
    email_extra_count = int(round(overlap_total_target * email_share / sum_share))
    phone_extra_count = overlap_total_target - triple_count - email_extra_count

    # Temp objects (use UUID suffix for uniqueness across concurrent runs)
    _suffix = uuid.uuid4().hex[:6]
    tmp_source = f"TEMP_SF1_SOURCE_{_suffix}"
    tmp_base = f"TEMP_SF1_BASE_{_suffix}"
    tmp_final = f"TEMP_SF1_FINAL_{_suffix}"

    # Source sample for overlaps (ensure enough rows, cycle via modulo)
    _exec(
        session,
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {tmp_source} AS
        SELECT 
            EMAIL AS SRC_EMAIL,
            PHONE AS SRC_PHONE,
            FIRST_NAME AS SRC_FIRST_NAME,
            LAST_NAME AS SRC_LAST_NAME,
            ROW_NUMBER() OVER (ORDER BY RANDOM()) AS SRC_RN
        FROM {SOURCE_TABLE}
        WHERE EMAIL IS NOT NULL OR PHONE IS NOT NULL
        """,
    )

    # Build base dataset with deterministic French-flavored values and overlap assignments
    _exec(
        session,
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {tmp_base} AS
        WITH base_rows AS (
            SELECT 
                ROW_NUMBER() OVER (ORDER BY SEQ4()) AS row_id
            FROM TABLE(GENERATOR(ROWCOUNT => {base_count}))
        ),
        thresholds AS (
            SELECT 
                {triple_count} AS t_triple,
                {triple_count + email_extra_count} AS t_email,
                {triple_count + email_extra_count + phone_extra_count} AS t_phone
        ),
        src AS (
            SELECT *, (SRC_RN) AS k FROM {tmp_source}
        ),
        assigned AS (
            SELECT 
                b.row_id,
                CASE 
                    WHEN b.row_id <= (SELECT t_triple FROM thresholds) THEN 'TRIPLE'
                    WHEN b.row_id <= (SELECT t_email FROM thresholds) THEN 'EMAIL'
                    WHEN b.row_id <= (SELECT t_phone FROM thresholds) THEN 'PHONE'
                    ELSE 'NONE'
                END AS overlap_type,
                /* Deterministic name lists */
                CASE (b.row_id % 20)
                    WHEN 0 THEN 'Jean' WHEN 1 THEN 'Marie' WHEN 2 THEN 'Pierre' WHEN 3 THEN 'Sophie'
                    WHEN 4 THEN 'Michel' WHEN 5 THEN 'Catherine' WHEN 6 THEN 'Philippe' WHEN 7 THEN 'Nathalie'
                    WHEN 8 THEN 'Alain' WHEN 9 THEN 'Isabelle' WHEN 10 THEN 'François' WHEN 11 THEN 'Sylvie'
                    WHEN 12 THEN 'Bernard' WHEN 13 THEN 'Martine' WHEN 14 THEN 'Patrick' WHEN 15 THEN 'Christine'
                    WHEN 16 THEN 'Daniel' WHEN 17 THEN 'Françoise' WHEN 18 THEN 'Thierry' ELSE 'Monique'
                END AS gen_first_name,
                CASE (b.row_id % 25)
                    WHEN 0 THEN 'Martin' WHEN 1 THEN 'Bernard' WHEN 2 THEN 'Dubois' WHEN 3 THEN 'Thomas'
                    WHEN 4 THEN 'Robert' WHEN 5 THEN 'Petit' WHEN 6 THEN 'Richard' WHEN 7 THEN 'Durand'
                    WHEN 8 THEN 'Leroy' WHEN 9 THEN 'Moreau' WHEN 10 THEN 'Simon' WHEN 11 THEN 'Laurent'
                    WHEN 12 THEN 'Lefebvre' WHEN 13 THEN 'Michel' WHEN 14 THEN 'Garcia' WHEN 15 THEN 'David'
                    WHEN 16 THEN 'Bertrand' WHEN 17 THEN 'Roux' WHEN 18 THEN 'Vincent' WHEN 19 THEN 'Fournier'
                    WHEN 20 THEN 'Morel' WHEN 21 THEN 'Girard' WHEN 22 THEN 'Andre' WHEN 23 THEN 'Lefevre'
                    ELSE 'Mercier'
                END AS gen_last_name,
                /* Join key for cycling through source sample */
                (b.row_id % NULLIF((SELECT MAX(SRC_RN) FROM {tmp_source}), 0)) + 1 AS src_k
            FROM base_rows b
        ),
        joined AS (
            SELECT a.*, s.SRC_EMAIL, s.SRC_PHONE, s.SRC_FIRST_NAME, s.SRC_LAST_NAME
            FROM assigned a
            LEFT JOIN {tmp_source} s
              ON s.SRC_RN = a.src_k
        ),
        enriched AS (
            SELECT 
                /* Identity fields with controlled overlap */
                CASE overlap_type 
                    WHEN 'TRIPLE' THEN COALESCE(SRC_FIRST_NAME, gen_first_name)
                    ELSE gen_first_name
                END AS first_name,
                CASE overlap_type 
                    WHEN 'TRIPLE' THEN COALESCE(SRC_LAST_NAME, gen_last_name)
                    ELSE gen_last_name
                END AS last_name,
                CASE overlap_type
                    WHEN 'TRIPLE' THEN SRC_EMAIL
                    WHEN 'EMAIL' THEN SRC_EMAIL
                    ELSE LOWER(gen_first_name) || '.' || LOWER(gen_last_name) || '@' ||
                         CASE (row_id % 6)
                            WHEN 0 THEN 'gmail.com' WHEN 1 THEN 'orange.fr' WHEN 2 THEN 'free.fr'
                            WHEN 3 THEN 'wanadoo.fr' WHEN 4 THEN 'sfr.fr' ELSE 'laposte.net'
                         END
                END AS email_raw,
                CASE overlap_type
                    WHEN 'TRIPLE' THEN SRC_PHONE
                    WHEN 'PHONE' THEN SRC_PHONE
                    ELSE '0' || (1 + (row_id % 6)) || ' ' ||
                         LPAD(UNIFORM(10, 99, RANDOM()), 2, '0') || ' ' ||
                         LPAD(UNIFORM(10, 99, RANDOM()), 2, '0') || ' ' ||
                         LPAD(UNIFORM(10, 99, RANDOM()), 2, '0') || ' ' ||
                         LPAD(UNIFORM(10, 99, RANDOM()), 2, '0')
                END AS phone_raw,
                /* Other streaming-specific fields */
                CASE (row_id % 2) WHEN 0 THEN 'Male' ELSE 'Female' END AS gender,
                CASE (row_id % 8)
                    WHEN 0 THEN 'Engineer' WHEN 1 THEN 'Teacher' WHEN 2 THEN 'Student' WHEN 3 THEN 'Nurse'
                    WHEN 4 THEN 'Sales' WHEN 5 THEN 'Artist' WHEN 6 THEN 'Manager' ELSE 'Consultant'
                END AS profession,
                DATEADD(day, -UNIFORM(0, 3650, RANDOM()), CURRENT_DATE()) AS date_joined,
                CASE (row_id % 4)
                    WHEN 0 THEN 'FREE' WHEN 1 THEN 'BASIC' WHEN 2 THEN 'STANDARD' ELSE 'PREMIUM'
                END AS subscription_level,
                overlap_type,
                row_id
            FROM joined
        ),
        with_missing AS (
            SELECT 
                'SF1-' || LPAD(row_id::STRING, 10, '0') AS customer_id,
                first_name,
                last_name,
                /* Missingness: outside overlap sets */
                CASE WHEN overlap_type IN ('TRIPLE','EMAIL') THEN email_raw
                     WHEN UNIFORM(0,1,RANDOM()) < 0.15 THEN NULL
                     ELSE email_raw END AS email,
                CASE WHEN overlap_type IN ('TRIPLE','PHONE') THEN phone_raw
                     WHEN UNIFORM(0,1,RANDOM()) < 0.20 THEN NULL
                     ELSE phone_raw END AS phone,
                gender, profession, date_joined, subscription_level, overlap_type
            FROM enriched
        )
        SELECT * FROM with_missing
        """,
    )

    # Create duplicates (10% of total) with minor mutations, keep total rows exact
    _exec(
        session,
        f"""
        CREATE OR REPLACE TEMPORARY TABLE {tmp_final} AS
        WITH dups AS (
            SELECT 
                customer_id || '_DUP' AS customer_id,
                /* mutate email/phone for ~50% */
                CASE WHEN UNIFORM(0,1,RANDOM()) < 0.5 AND email IS NOT NULL THEN 
                    REGEXP_REPLACE(email, '@', CAST(UNIFORM(0,9,RANDOM()) AS STRING) || '@')
                ELSE email END AS email,
                CASE WHEN UNIFORM(0,1,RANDOM()) < 0.5 AND phone IS NOT NULL THEN 
                    SUBSTR(phone, 1, LENGTH(phone)-1) || CAST(UNIFORM(0,9,RANDOM()) AS STRING)
                ELSE phone END AS phone,
                first_name, last_name, gender, profession, date_joined, subscription_level,
                'DUPLICATE' AS overlap_type
            FROM (
                SELECT * FROM {tmp_base}
                WHERE overlap_type = 'NONE'
                QUALIFY ROW_NUMBER() OVER (ORDER BY RANDOM()) <= {dup_count}
            )
        )
        SELECT customer_id, email, phone, first_name, last_name, gender, profession, date_joined, subscription_level, overlap_type
        FROM {tmp_base}
        UNION ALL
        SELECT customer_id, email, phone, first_name, last_name, gender, profession, date_joined, subscription_level, overlap_type
        FROM dups
        """,
    )

    # Persist final table
    full_table = f"{TARGET_DB}.{RAW_SCHEMA}.{OUTPUT_TABLE}"
    if overwrite:
        _exec(session, f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM {tmp_final}")
    else:
        _exec(session, f"CREATE TABLE IF NOT EXISTS {full_table} AS SELECT * FROM {tmp_final} WHERE 1=0")
        _exec(session, f"INSERT INTO {full_table} SELECT * FROM {tmp_final}")

    # Return a small preview (
    return session.sql(f"SELECT * FROM {full_table} SAMPLE (100 ROWS)")


# Optional SP-compatible entrypoint name
def sp_entry(session: Session, target_rows: Optional[int] = None, overwrite: bool = True):
    if target_rows is None:
        target_rows = 4_000_000
    return run(session, target_rows=target_rows, overwrite=overwrite)



