-- sql/init_schemas.sql

-- =========================================
-- LAYER 1: RAW (Immutable landing zone)
-- =========================================
-- Règle d'or: JAMAIS modifier/supprimer de raw
-- C'est ton "source of truth" pour l'audit

CREATE SCHEMA IF NOT EXISTS raw;

-- Pourquoi "loaded_at" dans CHAQUE table raw?
-- → Pour tracker quand la donnée est arrivée (data lineage)
-- → Pour détecter si l'ingestion lag (ex: données de hier arrivent aujourd'hui)

COMMENT ON SCHEMA raw IS 'Immutable landing zone - data as received from sources';


-- =========================================
-- LAYER 2: STAGING (Light cleanup)
-- =========================================
-- Objectif: Typage, normalisation, pas de business logic
-- Idempotent: On peut re-run sans casser

CREATE SCHEMA IF NOT EXISTS staging;

COMMENT ON SCHEMA staging IS 'Cleaned and typed data - no business logic yet';


-- =========================================
-- LAYER 3: WAREHOUSE (Business value)
-- =========================================
-- Objectif: Golden records, metrics, segments
-- C'est ce que les analysts/dashboards utilisent

CREATE SCHEMA IF NOT EXISTS warehouse;

COMMENT ON SCHEMA warehouse IS 'Business-ready data - golden records and metrics';


-- =========================================
-- METADATA TABLE (Data Observability)
-- =========================================
-- Track pipeline runs pour debugging
-- Permet de répondre: "Quand ce DAG a-t-il run pour la dernière fois?"

CREATE TABLE IF NOT EXISTS warehouse.pipeline_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    rows_inserted INT,
    rows_updated INT,
    run_status VARCHAR(20),  -- 'success', 'failed', 'running'
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- Index pour query rapide "Quels pipelines ont fail aujourd'hui?"
CREATE INDEX idx_pipeline_runs ON warehouse.pipeline_metadata(pipeline_name, started_at DESC);

COMMENT ON TABLE warehouse.pipeline_metadata IS 'Observability - tracks all pipeline executions';


-- =========================================
-- GRANT PERMISSIONS (Sécurité de base)
-- =========================================
-- En prod, tu aurais des users séparés:
-- - airflow_user (INSERT only sur raw/staging)
-- - dbt_user (SELECT raw, INSERT warehouse)
-- - analyst_user (SELECT only warehouse)

-- Pour l'instant, on donne tout au user "dataeng" (c'est du dev)
GRANT ALL PRIVILEGES ON SCHEMA raw TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dataeng;
GRANT ALL PRIVILEGES ON SCHEMA warehouse TO dataeng;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA warehouse TO dataeng;

-- Permissions pour les futures tables (sinon il faut re-GRANT)
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO dataeng;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT ALL ON TABLES TO dataeng;
