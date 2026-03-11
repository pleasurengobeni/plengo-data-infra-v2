#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap the data database: pipeline schema + etl_metrics table.
# The Dash dashboard reads from pipeline.etl_metrics; Airflow DAGs write to it.
# POSTGRES_DATA_DB / POSTGRES_DATA_USER are set in .env (same vars used by
# the pipeline-monitor service in docker-compose.yaml).
# ─────────────────────────────────────────────────────────────────────────────
DATA_DB="${POSTGRES_DATA_DB:-airflow}"
DATA_USER="${POSTGRES_DATA_USER:-airflow}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DATA_DB" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS pipeline;

    -- One row per DAG run. Dash aggregates these over time.
    CREATE TABLE IF NOT EXISTS pipeline.etl_metrics (
        id                          BIGSERIAL   PRIMARY KEY,
        dag_id                      TEXT        NOT NULL,
        env                         TEXT        NOT NULL DEFAULT 'dev',
        schema                      TEXT,
        table_name                  TEXT        NOT NULL,
        run_id                      TEXT,
        run_start                   TIMESTAMP,
        run_end                     TIMESTAMP,
        run_duration_secs           INTEGER,
        run_status                  TEXT        NOT NULL DEFAULT 'success',
        error_msg                   TEXT,
        batch_size                  INTEGER,
        rows_loaded_now             INTEGER     NOT NULL DEFAULT 0,
        target_total_records        INTEGER,
        target_today_records        INTEGER,
        target_max_upload_date      TIMESTAMP,
        target_first_upload_date    TIMESTAMP,
        target_max_incremental      TEXT,
        source_total_records        INTEGER,
        source_max_incremental      TEXT,
        source_loaded_to_max_target INTEGER,
        perc_loaded                 NUMERIC(5,2),
        avg_daily_load              INTEGER,
        weekday_loads               JSONB,
        created_at                  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    GRANT USAGE  ON SCHEMA pipeline TO $DATA_USER;
    GRANT ALL    ON ALL TABLES    IN SCHEMA pipeline TO $DATA_USER;
    GRANT ALL    ON ALL SEQUENCES IN SCHEMA pipeline TO $DATA_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT ALL ON TABLES    TO $DATA_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT ALL ON SEQUENCES TO $DATA_USER;
EOSQL



