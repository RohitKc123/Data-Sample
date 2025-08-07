-- PostgreSQL initialization script for COVID-19 Data Engineering Project

-- Create database if not exists (this is handled by docker-compose environment)
-- CREATE DATABASE covid_data;

-- Connect to the database
\c covid_data;

-- Create covid_data table
CREATE TABLE IF NOT EXISTS covid_data (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    country VARCHAR(100) NOT NULL,
    new_cases INTEGER,
    total_cases INTEGER,
    new_cases_7day_avg DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, country)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_covid_data_date_country 
ON covid_data(date, country);

CREATE INDEX IF NOT EXISTS idx_covid_data_date 
ON covid_data(date);

CREATE INDEX IF NOT EXISTS idx_covid_data_country 
ON covid_data(country);

-- Create a view for recent data
CREATE OR REPLACE VIEW recent_covid_data AS
SELECT 
    date,
    country,
    new_cases,
    total_cases,
    new_cases_7day_avg,
    created_at
FROM covid_data
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY country, date DESC;

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE covid_data TO postgres;
GRANT ALL PRIVILEGES ON SEQUENCE covid_data_id_seq TO postgres;
GRANT SELECT ON recent_covid_data TO postgres;

-- Insert a sample record to verify table structure
INSERT INTO covid_data (date, country, new_cases, total_cases, new_cases_7day_avg)
VALUES ('2020-01-01', 'Test Country', 0, 0, 0.0)
ON CONFLICT (date, country) DO NOTHING; 