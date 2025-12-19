-- Regional Economics Database - Schema Creation
-- Database: Regional_Economics_Database_NRW
-- Version: 1.0
-- Created: December 2024

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;  -- Geographic data support
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- UUID generation

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Geographic Dimension
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_id SERIAL PRIMARY KEY,
    region_code VARCHAR(20) UNIQUE NOT NULL,
    region_name VARCHAR(255) NOT NULL,
    region_name_en VARCHAR(255),  -- English name
    region_type VARCHAR(50) NOT NULL,  -- 'district', 'urban_district', 'administrative_district', 'state', 'country'
    parent_region_code VARCHAR(20),
    ruhr_area BOOLEAN DEFAULT FALSE,
    lower_rhine_region BOOLEAN DEFAULT FALSE,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    geometry GEOMETRY(MULTIPOLYGON, 4326),  -- PostGIS geometry
    population_2023 INTEGER,  -- Reference population for context
    area_sqkm DECIMAL(10, 2),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_parent_region FOREIGN KEY (parent_region_code) 
        REFERENCES dim_geography(region_code) ON DELETE SET NULL
);

-- Indexes for geography
CREATE INDEX idx_geo_region_code ON dim_geography(region_code);
CREATE INDEX idx_geo_region_type ON dim_geography(region_type);
CREATE INDEX idx_geo_ruhr ON dim_geography(ruhr_area) WHERE ruhr_area = TRUE;
CREATE INDEX idx_geo_lower_rhine ON dim_geography(lower_rhine_region) WHERE lower_rhine_region = TRUE;
CREATE INDEX idx_geo_geometry ON dim_geography USING GIST(geometry);

COMMENT ON TABLE dim_geography IS 'Geographic dimension containing all regions (districts, states, country)';
COMMENT ON COLUMN dim_geography.region_type IS 'Type: district, urban_district, administrative_district, state, country';

-- Time Dimension
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    quarter INTEGER CHECK (quarter BETWEEN 1 AND 4),
    month INTEGER CHECK (month BETWEEN 1 AND 12),
    reference_date DATE,
    reference_type VARCHAR(50),  -- 'year_end', 'mid_year', 'annual_avg', 'annual_total'
    is_complete BOOLEAN DEFAULT TRUE,  -- Whether data for this period is complete
    fiscal_year INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, reference_date, reference_type)
);

-- Indexes for time
CREATE INDEX idx_time_year ON dim_time(year);
CREATE INDEX idx_time_ref_date ON dim_time(reference_date);
CREATE INDEX idx_time_ref_type ON dim_time(reference_type);

COMMENT ON TABLE dim_time IS 'Time dimension with various reference dates and periods';

-- Indicator Dimension
CREATE TABLE IF NOT EXISTS dim_indicator (
    indicator_id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(100) UNIQUE NOT NULL,
    indicator_name VARCHAR(255) NOT NULL,
    indicator_name_en VARCHAR(255),
    indicator_category VARCHAR(100) NOT NULL,  -- 'demographics', 'labor_market', 'business_economy', etc.
    indicator_subcategory VARCHAR(100),
    source_system VARCHAR(100) NOT NULL,  -- 'regional_db', 'state_db', 'ba'
    source_table_id VARCHAR(50),
    unit_of_measure VARCHAR(50),
    unit_description TEXT,
    description TEXT,
    calculation_method TEXT,
    update_frequency VARCHAR(50),
    typical_reference_date VARCHAR(50),
    data_type VARCHAR(50),  -- 'count', 'rate', 'currency', 'percentage'
    is_derived BOOLEAN DEFAULT FALSE,  -- Whether calculated from other indicators
    parent_indicator_id INTEGER,
    aggregation_method VARCHAR(50),  -- 'sum', 'average', 'weighted_avg', 'median'
    quality_notes TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_parent_indicator FOREIGN KEY (parent_indicator_id) 
        REFERENCES dim_indicator(indicator_id) ON DELETE SET NULL
);

-- Indexes for indicators
CREATE INDEX idx_indicator_code ON dim_indicator(indicator_code);
CREATE INDEX idx_indicator_category ON dim_indicator(indicator_category);
CREATE INDEX idx_indicator_source ON dim_indicator(source_system);
CREATE INDEX idx_indicator_active ON dim_indicator(is_active) WHERE is_active = TRUE;

COMMENT ON TABLE dim_indicator IS 'Indicator metadata describing all economic and social metrics';

-- Economic Sector Dimension (WZ 2008)
CREATE TABLE IF NOT EXISTS dim_economic_sector (
    sector_id SERIAL PRIMARY KEY,
    sector_code VARCHAR(20) UNIQUE NOT NULL,
    sector_name VARCHAR(255) NOT NULL,
    sector_name_en VARCHAR(255),
    sector_level INTEGER NOT NULL,  -- 1=Section, 2=Division, 3=Group, 4=Class
    parent_sector_code VARCHAR(20),
    wz_version VARCHAR(20) DEFAULT 'WZ2008',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_parent_sector FOREIGN KEY (parent_sector_code) 
        REFERENCES dim_economic_sector(sector_code) ON DELETE SET NULL
);

-- Indexes for sectors
CREATE INDEX idx_sector_code ON dim_economic_sector(sector_code);
CREATE INDEX idx_sector_level ON dim_economic_sector(sector_level);

COMMENT ON TABLE dim_economic_sector IS 'Economic sector classification (WZ 2008)';

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Demographics Fact Table
CREATE TABLE IF NOT EXISTS fact_demographics (
    fact_id BIGSERIAL PRIMARY KEY,
    geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    indicator_id INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
    value NUMERIC(20,4),
    gender VARCHAR(20),  -- 'male', 'female', 'total'
    nationality VARCHAR(50),  -- 'german', 'foreign', 'total'
    age_group VARCHAR(50),
    migration_background VARCHAR(100),
    data_quality_flag VARCHAR(20) DEFAULT 'V',  -- V=Validated, E=Estimated, P=Provisional
    confidence_score DECIMAL(3,2),  -- 0.00 to 1.00
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_id, time_id, indicator_id, gender, nationality, age_group, migration_background)
);

-- Indexes for demographics
CREATE INDEX idx_demo_geo ON fact_demographics(geo_id);
CREATE INDEX idx_demo_time ON fact_demographics(time_id);
CREATE INDEX idx_demo_indicator ON fact_demographics(indicator_id);
CREATE INDEX idx_demo_gender ON fact_demographics(gender);
CREATE INDEX idx_demo_nationality ON fact_demographics(nationality);
CREATE INDEX idx_demo_composite ON fact_demographics(geo_id, time_id, indicator_id);

COMMENT ON TABLE fact_demographics IS 'Population and demographic indicators';

-- Labor Market Fact Table
CREATE TABLE IF NOT EXISTS fact_labor_market (
    fact_id BIGSERIAL PRIMARY KEY,
    geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    indicator_id INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
    sector_id INTEGER REFERENCES dim_economic_sector(sector_id),
    value NUMERIC(20,4),
    gender VARCHAR(20),
    nationality VARCHAR(50),
    employment_type VARCHAR(50),  -- 'full_time', 'part_time', 'total'
    education_level VARCHAR(100),
    workplace_residence VARCHAR(50),  -- 'workplace', 'residence'
    age_group VARCHAR(50),
    data_quality_flag VARCHAR(20) DEFAULT 'V',
    confidence_score DECIMAL(3,2),
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_id, time_id, indicator_id, sector_id, gender, nationality, 
           employment_type, education_level, workplace_residence, age_group)
);

-- Indexes for labor market
CREATE INDEX idx_labor_geo ON fact_labor_market(geo_id);
CREATE INDEX idx_labor_time ON fact_labor_market(time_id);
CREATE INDEX idx_labor_indicator ON fact_labor_market(indicator_id);
CREATE INDEX idx_labor_sector ON fact_labor_market(sector_id);
CREATE INDEX idx_labor_composite ON fact_labor_market(geo_id, time_id, indicator_id);

COMMENT ON TABLE fact_labor_market IS 'Employment, unemployment, and wage indicators';

-- Business Economy Fact Table
CREATE TABLE IF NOT EXISTS fact_business_economy (
    fact_id BIGSERIAL PRIMARY KEY,
    geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    indicator_id INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
    sector_id INTEGER REFERENCES dim_economic_sector(sector_id),
    value NUMERIC(20,4),
    size_class VARCHAR(50),  -- '0', '1-9', '10-49', '50-249', '250+', 'total'
    establishment_type VARCHAR(100),
    turnover_type VARCHAR(100),  -- 'construction', 'total', etc.
    data_quality_flag VARCHAR(20) DEFAULT 'V',
    confidence_score DECIMAL(3,2),
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_id, time_id, indicator_id, sector_id, size_class, establishment_type, turnover_type)
);

-- Indexes for business economy
CREATE INDEX idx_business_geo ON fact_business_economy(geo_id);
CREATE INDEX idx_business_time ON fact_business_economy(time_id);
CREATE INDEX idx_business_indicator ON fact_business_economy(indicator_id);
CREATE INDEX idx_business_sector ON fact_business_economy(sector_id);
CREATE INDEX idx_business_composite ON fact_business_economy(geo_id, time_id, indicator_id);

COMMENT ON TABLE fact_business_economy IS 'Business establishments, registrations, insolvencies, GDP';

-- Healthcare Fact Table
CREATE TABLE IF NOT EXISTS fact_healthcare (
    fact_id BIGSERIAL PRIMARY KEY,
    geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    indicator_id INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
    value NUMERIC(20,4),
    gender VARCHAR(20),
    care_level VARCHAR(50),  -- 'level_1', 'level_2', ..., 'level_5', 'total'
    facility_type VARCHAR(100),  -- 'hospital', 'outpatient', 'residential', etc.
    provider_type VARCHAR(100),  -- 'public', 'private_nonprofit', 'private_forprofit'
    service_type VARCHAR(100),
    data_quality_flag VARCHAR(20) DEFAULT 'V',
    confidence_score DECIMAL(3,2),
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_id, time_id, indicator_id, gender, care_level, facility_type, provider_type, service_type)
);

-- Indexes for healthcare
CREATE INDEX idx_health_geo ON fact_healthcare(geo_id);
CREATE INDEX idx_health_time ON fact_healthcare(time_id);
CREATE INDEX idx_health_indicator ON fact_healthcare(indicator_id);
CREATE INDEX idx_health_composite ON fact_healthcare(geo_id, time_id, indicator_id);

COMMENT ON TABLE fact_healthcare IS 'Healthcare facilities, personnel, and care recipients';

-- Public Finance Fact Table
CREATE TABLE IF NOT EXISTS fact_public_finance (
    fact_id BIGSERIAL PRIMARY KEY,
    geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    indicator_id INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
    value NUMERIC(20,4),
    finance_type VARCHAR(100),  -- 'receipt', 'expenditure', 'tax'
    category VARCHAR(100),
    income_bracket VARCHAR(100),
    data_quality_flag VARCHAR(20) DEFAULT 'V',
    confidence_score DECIMAL(3,2),
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_id, time_id, indicator_id, finance_type, category, income_bracket)
);

-- Indexes for public finance
CREATE INDEX idx_finance_geo ON fact_public_finance(geo_id);
CREATE INDEX idx_finance_time ON fact_public_finance(time_id);
CREATE INDEX idx_finance_indicator ON fact_public_finance(indicator_id);

COMMENT ON TABLE fact_public_finance IS 'Municipal finances and income tax statistics';

-- Infrastructure Fact Table
CREATE TABLE IF NOT EXISTS fact_infrastructure (
    fact_id BIGSERIAL PRIMARY KEY,
    geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    indicator_id INTEGER NOT NULL REFERENCES dim_indicator(indicator_id),
    value NUMERIC(20,4),
    infrastructure_type VARCHAR(100),  -- 'road', 'rail', 'broadband', etc.
    classification VARCHAR(100),  -- Road class: 'federal_highway', 'federal_road', 'state_road', 'district_road'
    data_quality_flag VARCHAR(20) DEFAULT 'V',
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(geo_id, time_id, indicator_id, infrastructure_type, classification)
);

-- Indexes for infrastructure
CREATE INDEX idx_infra_geo ON fact_infrastructure(geo_id);
CREATE INDEX idx_infra_time ON fact_infrastructure(time_id);
CREATE INDEX idx_infra_indicator ON fact_infrastructure(indicator_id);

COMMENT ON TABLE fact_infrastructure IS 'Infrastructure indicators including road networks';

-- Commuters Fact Table
CREATE TABLE IF NOT EXISTS fact_commuters (
    fact_id BIGSERIAL PRIMARY KEY,
    origin_geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    destination_geo_id INTEGER NOT NULL REFERENCES dim_geography(geo_id),
    time_id INTEGER NOT NULL REFERENCES dim_time(time_id),
    commuter_count INTEGER NOT NULL,
    commuter_direction VARCHAR(20) NOT NULL,  -- 'incoming', 'outgoing', 'net'
    sector_id INTEGER REFERENCES dim_economic_sector(sector_id),
    data_quality_flag VARCHAR(20) DEFAULT 'V',
    source_url TEXT,
    notes TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(origin_geo_id, destination_geo_id, time_id, commuter_direction, sector_id)
);

-- Indexes for commuters
CREATE INDEX idx_commute_origin ON fact_commuters(origin_geo_id);
CREATE INDEX idx_commute_dest ON fact_commuters(destination_geo_id);
CREATE INDEX idx_commute_time ON fact_commuters(time_id);
CREATE INDEX idx_commute_direction ON fact_commuters(commuter_direction);

COMMENT ON TABLE fact_commuters IS 'Commuter flows between geographic regions';

-- ============================================================================
-- AUDIT AND QUALITY TABLES
-- ============================================================================

-- Data Extraction Log
CREATE TABLE IF NOT EXISTS data_extraction_log (
    log_id BIGSERIAL PRIMARY KEY,
    source_system VARCHAR(100) NOT NULL,
    table_id VARCHAR(50),
    indicator_code VARCHAR(100),
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extraction_start TIMESTAMP,
    extraction_end TIMESTAMP,
    records_extracted INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    status VARCHAR(50) NOT NULL,  -- 'success', 'partial', 'failed'
    error_message TEXT,
    error_traceback TEXT,
    execution_time_seconds NUMERIC(10,2),
    extraction_parameters JSONB,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

-- Indexes for extraction log
CREATE INDEX idx_extract_source ON data_extraction_log(source_system);
CREATE INDEX idx_extract_status ON data_extraction_log(status);
CREATE INDEX idx_extract_timestamp ON data_extraction_log(extraction_timestamp DESC);

COMMENT ON TABLE data_extraction_log IS 'Audit log for all data extraction operations';

-- Data Quality Checks
CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    check_type VARCHAR(100) NOT NULL,  -- 'completeness', 'consistency', 'validity', 'accuracy'
    check_name VARCHAR(255) NOT NULL,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_checked INTEGER,
    records_passed INTEGER,
    records_failed INTEGER,
    pass_rate DECIMAL(5,2),
    threshold DECIMAL(5,2),
    status VARCHAR(50),  -- 'passed', 'failed', 'warning'
    failure_details JSONB,
    recommendations TEXT,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

-- Indexes for quality checks
CREATE INDEX idx_quality_table ON data_quality_checks(table_name);
CREATE INDEX idx_quality_type ON data_quality_checks(check_type);
CREATE INDEX idx_quality_timestamp ON data_quality_checks(check_timestamp DESC);
CREATE INDEX idx_quality_status ON data_quality_checks(status);

COMMENT ON TABLE data_quality_checks IS 'Results of data quality validation checks';

-- Data Lineage
CREATE TABLE IF NOT EXISTS data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    source_table VARCHAR(100),
    source_id BIGINT,
    target_table VARCHAR(100) NOT NULL,
    target_id BIGINT NOT NULL,
    transformation_type VARCHAR(100),  -- 'extract', 'transform', 'aggregate', 'derive'
    transformation_description TEXT,
    transformation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT CURRENT_USER
);

-- Indexes for lineage
CREATE INDEX idx_lineage_source ON data_lineage(source_table, source_id);
CREATE INDEX idx_lineage_target ON data_lineage(target_table, target_id);
CREATE INDEX idx_lineage_timestamp ON data_lineage(transformation_timestamp DESC);

COMMENT ON TABLE data_lineage IS 'Tracks data lineage and transformations';

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Latest data by indicator and region
CREATE OR REPLACE VIEW vw_latest_indicators AS
SELECT 
    g.region_code,
    g.region_name,
    g.region_type,
    t.year,
    t.reference_date,
    i.indicator_code,
    i.indicator_name,
    i.indicator_category,
    'demographics' AS source_table,
    d.value,
    d.gender,
    d.nationality,
    d.age_group,
    d.data_quality_flag
FROM fact_demographics d
JOIN dim_geography g ON d.geo_id = g.geo_id
JOIN dim_time t ON d.time_id = t.time_id
JOIN dim_indicator i ON d.indicator_id = i.indicator_id
WHERE t.year = (SELECT MAX(year) FROM dim_time)

UNION ALL

SELECT 
    g.region_code,
    g.region_name,
    g.region_type,
    t.year,
    t.reference_date,
    i.indicator_code,
    i.indicator_name,
    i.indicator_category,
    'labor_market' AS source_table,
    l.value,
    l.gender,
    l.nationality,
    NULL AS age_group,
    l.data_quality_flag
FROM fact_labor_market l
JOIN dim_geography g ON l.geo_id = g.geo_id
JOIN dim_time t ON l.time_id = t.time_id
JOIN dim_indicator i ON l.indicator_id = i.indicator_id
WHERE t.year = (SELECT MAX(year) FROM dim_time);

COMMENT ON VIEW vw_latest_indicators IS 'Latest available data for all indicators';

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

-- Function to update timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for dim_geography
CREATE TRIGGER trg_geography_updated_at
    BEFORE UPDATE ON dim_geography
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for dim_indicator
CREATE TRIGGER trg_indicator_updated_at
    BEFORE UPDATE ON dim_indicator
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- GRANTS (Adjust as needed)
-- ============================================================================

-- Create roles (if they don't exist)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_user') THEN
        CREATE ROLE etl_user LOGIN PASSWORD 'change_me';
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'read_only_user') THEN
        CREATE ROLE read_only_user LOGIN PASSWORD 'change_me';
    END IF;
END
$$;

-- Grant permissions
GRANT CONNECT ON DATABASE "Regional_Economics_Database_NRW" TO etl_user, read_only_user;
GRANT USAGE ON SCHEMA public TO etl_user, read_only_user;

-- ETL user needs write access
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO etl_user;

-- Read-only user
GRANT SELECT ON ALL TABLES IN SCHEMA public TO read_only_user;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE 'Schema creation completed successfully!';
    RAISE NOTICE 'Database: Regional_Economics_Database_NRW';
    RAISE NOTICE 'Tables created: %', (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE');
END $$;

