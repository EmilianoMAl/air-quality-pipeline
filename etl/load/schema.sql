-- ============================================
-- Schema: Air Quality Pipeline
-- Descripción: Tablas para almacenar datos
--              de monitoreo de calidad del aire
-- ============================================

-- Tabla de estaciones de monitoreo
CREATE TABLE IF NOT EXISTS locations (
    location_id     INTEGER PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    locality        VARCHAR(255),
    country         VARCHAR(10),
    latitude        DECIMAL(10, 6) NOT NULL,
    longitude       DECIMAL(10, 6) NOT NULL,
    is_mobile       BOOLEAN DEFAULT FALSE,
    is_monitor      BOOLEAN DEFAULT FALSE,
    sensors_count   INTEGER DEFAULT 0,
    extracted_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para queries frecuentes
CREATE INDEX IF NOT EXISTS idx_locations_country 
    ON locations(country);

CREATE INDEX IF NOT EXISTS idx_locations_locality 
    ON locations(locality);

-- Tabla de mediciones por sensor
CREATE TABLE IF NOT EXISTS measurements (
    id              BIGSERIAL PRIMARY KEY,
    location_id     INTEGER REFERENCES locations(location_id),
    parameter       VARCHAR(50) NOT NULL,   -- pm25, pm10, no2, co, etc.
    value           DECIMAL(12, 4),
    unit            VARCHAR(20),
    measured_at     TIMESTAMPTZ NOT NULL,
    extracted_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para queries de series temporales
CREATE INDEX IF NOT EXISTS idx_measurements_location 
    ON measurements(location_id);

CREATE INDEX IF NOT EXISTS idx_measurements_parameter 
    ON measurements(parameter);

CREATE INDEX IF NOT EXISTS idx_measurements_measured_at 
    ON measurements(measured_at DESC);

-- Vista útil para el dashboard
CREATE OR REPLACE VIEW latest_measurements AS
SELECT 
    l.name          AS station_name,
    l.locality,
    l.country,
    l.latitude,
    l.longitude,
    m.parameter,
    m.value,
    m.unit,
    m.measured_at
FROM measurements m
JOIN locations l ON m.location_id = l.location_id
WHERE m.measured_at >= NOW() - INTERVAL '24 hours'
ORDER BY m.measured_at DESC;
