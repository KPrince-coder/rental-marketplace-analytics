-- Create schemas for the three-layer architecture
-- Raw layer for data as close as possible to the source
CREATE SCHEMA IF NOT EXISTS raw_data;
-- Curated layer for cleaned and transformed data
CREATE SCHEMA IF NOT EXISTS curated;
-- Presentation layer for aggregated data and metrics
CREATE SCHEMA IF NOT EXISTS presentation;
-- Grant permissions
GRANT USAGE ON SCHEMA raw_data TO PUBLIC;
GRANT USAGE ON SCHEMA curated TO PUBLIC;
GRANT USAGE ON SCHEMA presentation TO PUBLIC;
-- Set search path
SET search_path TO raw_data,
    curated,
    presentation;