-- sql/create_raw_tables.sql
-- Tables RAW pour recevoir les données brutes

-- =========================================
-- TABLE: raw.csv_customers
-- Source: messy_customers.csv
-- =========================================

DROP TABLE IF EXISTS raw.csv_customers;

CREATE TABLE raw.csv_customers (
    -- ID unique (du CSV)
    customer_id VARCHAR(100) PRIMARY KEY,
    
    -- Informations personnelles
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Adresse
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(10),
    
    -- Timestamps
    created_at VARCHAR(50),  -- VARCHAR car formats mixtes!
    
    -- Metadata d'ingestion
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255)
);

-- Index pour recherche rapide par email
CREATE INDEX idx_csv_customers_email ON raw.csv_customers(email);

-- Index pour recherche par date de load
CREATE INDEX idx_csv_customers_loaded_at ON raw.csv_customers(loaded_at);

COMMENT ON TABLE raw.csv_customers IS 
'Raw customer data from CSV - intentionally messy for data quality demo';

COMMENT ON COLUMN raw.csv_customers.created_at IS 
'Mixed date formats: ISO, DD/MM/YYYY, MM-DD-YYYY - will be cleaned in staging';
