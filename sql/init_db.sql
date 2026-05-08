-- =============================================================================
-- ClimaData Solutions — Database Initialization Script
-- =============================================================================
-- Usage: mysql -u root < sql/init_db.sql
-- This script creates the database and all required tables.
-- Safe to run multiple times (uses IF NOT EXISTS).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS climadata
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE climadata;

-- ── Dimension: Cities Catalog ───────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_cities (
    city_id     INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    country     VARCHAR(100) NOT NULL,
    latitude    DECIMAL(7,4) NOT NULL,
    longitude   DECIMAL(7,4) NOT NULL,
    UNIQUE KEY uq_city_coords (latitude, longitude)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Fact: Hourly Weather Data ───────────────────────────────────────────────
-- Stores per-hour meteorological observations per city.
-- PK is composite (city_id + timestamp) to support UPSERT idempotency.

CREATE TABLE IF NOT EXISTS fact_hourly_weather (
    city_id             INT            NOT NULL,
    timestamp           DATETIME       NOT NULL,
    temperature_2m      DECIMAL(5,2)   DEFAULT NULL,
    relative_humidity   DECIMAL(5,2)   DEFAULT NULL,
    wind_speed_10m      DECIMAL(5,2)   DEFAULT NULL,
    weather_code        INT            DEFAULT NULL,
    heat_index          DECIMAL(5,2)   DEFAULT NULL,
    heat_index_alert    TINYINT(1)     DEFAULT 0,
    high_wind_alert     TINYINT(1)     DEFAULT 0,
    ingested_at         DATETIME       NOT NULL,
    PRIMARY KEY (city_id, timestamp),
    CONSTRAINT fk_hourly_city FOREIGN KEY (city_id)
        REFERENCES dim_cities(city_id) ON DELETE CASCADE,
    INDEX idx_hourly_timestamp (timestamp),
    INDEX idx_hourly_city_time (city_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Fact: Daily Weather Data ────────────────────────────────────────────────
-- Stores per-day aggregated meteorological data per city.
-- PK is composite (city_id + date) to support UPSERT idempotency.

CREATE TABLE IF NOT EXISTS fact_daily_weather (
    city_id             INT            NOT NULL,
    date                DATE           NOT NULL,
    temperature_max     DECIMAL(5,2)   DEFAULT NULL,
    temperature_min     DECIMAL(5,2)   DEFAULT NULL,
    precipitation_sum   DECIMAL(7,2)   DEFAULT NULL,
    wind_speed_max      DECIMAL(5,2)   DEFAULT NULL,
    sunrise             TIME           DEFAULT NULL,
    sunset              TIME           DEFAULT NULL,
    heavy_rain_alert    TINYINT(1)     DEFAULT 0,
    high_wind_alert     TINYINT(1)     DEFAULT 0,
    ingested_at         DATETIME       NOT NULL,
    PRIMARY KEY (city_id, date),
    CONSTRAINT fk_daily_city FOREIGN KEY (city_id)
        REFERENCES dim_cities(city_id) ON DELETE CASCADE,
    INDEX idx_daily_date (date),
    INDEX idx_daily_city_date (city_id, date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Log: Pipeline Executions ────────────────────────────────────────────────
-- Tracks every pipeline run: when it started, how many rows it processed,
-- and whether it succeeded or failed.

CREATE TABLE IF NOT EXISTS log_executions (
    execution_id    INT AUTO_INCREMENT PRIMARY KEY,
    start_time      DATETIME       NOT NULL,
    end_time        DATETIME       DEFAULT NULL,
    mode            VARCHAR(20)    NOT NULL,
    cities_success  INT            DEFAULT 0,
    cities_failed   INT            DEFAULT 0,
    rows_inserted   INT            DEFAULT 0,
    errors_count    INT            DEFAULT 0,
    status          VARCHAR(20)    DEFAULT 'running'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Log: Discarded Data ─────────────────────────────────────────────────────
-- Records every data point that was discarded during validation,
-- along with the reason for rejection.

CREATE TABLE IF NOT EXISTS log_discarded_data (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    execution_id        INT            NOT NULL,
    city_id             INT            DEFAULT NULL,
    record_timestamp    VARCHAR(50)    DEFAULT NULL,
    variable_name       VARCHAR(50)    NOT NULL,
    invalid_value       VARCHAR(100)   DEFAULT NULL,
    reason              VARCHAR(255)   NOT NULL,
    discarded_at        DATETIME       NOT NULL,
    CONSTRAINT fk_discard_execution FOREIGN KEY (execution_id)
        REFERENCES log_executions(execution_id) ON DELETE CASCADE,
    CONSTRAINT fk_discard_city FOREIGN KEY (city_id)
        REFERENCES dim_cities(city_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Seed: Insert the 10 target cities ───────────────────────────────────────

INSERT INTO dim_cities (name, country, latitude, longitude) VALUES
    ('San Salvador',    'El Salvador',          13.6929, -89.2182),
    ('Santa Ana',       'El Salvador',          14.6349, -89.5591),
    ('Guatemala City',  'Guatemala',            14.6349, -90.5069),
    ('Tegucigalpa',     'Honduras',             14.0723, -87.1921),
    ('Managua',         'Nicaragua',            12.1150, -86.2362),
    ('San Jose',        'Costa Rica',            9.9281, -84.0907),
    ('Panama City',     'Panama',                8.9824, -79.5199),
    ('Santo Domingo',   'Dominican Republic',   18.4861, -69.9312),
    ('Kingston',        'Jamaica',              18.0179, -76.8099),
    ('San Juan',        'Puerto Rico',          18.4655, -66.1057)
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    country = VALUES(country);
