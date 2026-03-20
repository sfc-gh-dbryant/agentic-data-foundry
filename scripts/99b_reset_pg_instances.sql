-- ============================================================================
-- RESET PostgreSQL SOURCE INSTANCE
-- ============================================================================
-- This procedure connects to the PostgreSQL SOURCE instance via EAI
-- and truncates all tables to reset for testing.
--
-- Prerequisites:
-- - External Access Integration DBAONTAP_PG_EAI must exist
-- - Secrets must be configured for PostgreSQL credentials
-- ============================================================================

USE DATABASE DBAONTAP_ANALYTICS;
USE WAREHOUSE DBRYANT_COCO_WH_S;

CREATE OR REPLACE PROCEDURE METADATA.RESET_PG_SOURCE()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'psycopg2')
HANDLER = 'reset_pg_source'
EXTERNAL_ACCESS_INTEGRATIONS = (DBAONTAP_PG_EAI)
SECRETS = ('pg_creds' = DBAONTAP_ANALYTICS.METADATA.PG_SOURCE_SECRET)
AS
$$
import psycopg2
import _snowflake

def reset_pg_source(session):
    try:
        secret = _snowflake.get_generic_secret_string('pg_creds')
        parts = secret.split(':')
        user = parts[0]
        password = parts[1] if len(parts) > 1 else ''
        
        conn = psycopg2.connect(
            host="source-pg-host.example.snowflake.app",
            database="dbaontap",
            user=user,
            password=password,
            port=5432
        )
        
        cur = conn.cursor()
        
        tables_deleted = []
        for table in ['order_items', 'orders', 'support_tickets', 'products', 'customers']:
            cur.execute(f"TRUNCATE TABLE public.{table} CASCADE")
            tables_deleted.append(table)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "SUCCESS",
            "message": "PostgreSQL SOURCE instance reset",
            "tables_truncated": tables_deleted
        }
        
    except Exception as e:
        return {
            "status": "ERROR", 
            "error": str(e)
        }
$$;

CREATE OR REPLACE PROCEDURE METADATA.RESET_PG_LANDING()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'psycopg2')
HANDLER = 'reset_pg_landing'
EXTERNAL_ACCESS_INTEGRATIONS = (DBAONTAP_PG_EAI)
SECRETS = ('pg_creds' = DBAONTAP_ANALYTICS.METADATA.PG_LANDING_SECRET)
AS
$$
import psycopg2
import _snowflake

def reset_pg_landing(session):
    try:
        secret = _snowflake.get_generic_secret_string('pg_creds')
        parts = secret.split(':')
        user = parts[0]
        password = parts[1] if len(parts) > 1 else ''
        
        conn = psycopg2.connect(
            host="landing-pg-host.example.snowflake.app",
            database="dbaontap",
            user=user,
            password=password,
            port=5432
        )
        
        cur = conn.cursor()
        
        tables_deleted = []
        for table in ['order_items', 'orders', 'support_tickets', 'products', 'customers']:
            cur.execute(f"TRUNCATE TABLE public.{table} CASCADE")
            tables_deleted.append(table)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            "status": "SUCCESS",
            "message": "PostgreSQL LANDING instance reset",
            "tables_truncated": tables_deleted
        }
        
    except Exception as e:
        return {
            "status": "ERROR", 
            "error": str(e)
        }
$$;

SELECT 'PostgreSQL reset procedures created (requires EAI and secrets)' as status;
