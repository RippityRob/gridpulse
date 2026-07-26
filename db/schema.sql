-- GridPulse schema. Applied automatically by the Postgres container on
-- first start (mounted into /docker-entrypoint-initdb.d/), or manually:
--   psql -U gridpulse -d gridpulse -f db/schema.sql

CREATE TABLE IF NOT EXISTS readings (
    time         timestamptz      NOT NULL,
    sensor_id    integer          NOT NULL,
    sequence     bigint           NOT NULL,
    voltage_v    double precision NOT NULL,
    current_a    double precision NOT NULL,
    frequency_hz double precision NOT NULL,
    fault        boolean          NOT NULL DEFAULT false
);

-- Query pattern is almost always "recent data for a sensor",
-- so lead the index with sensor_id, then time descending.
CREATE INDEX IF NOT EXISTS readings_sensor_time_idx
    ON readings (sensor_id, time DESC);

-- Convenience view: per-sensor stats over the last 5 minutes.
CREATE OR REPLACE VIEW sensor_recent AS
SELECT
    sensor_id,
    count(*)                          AS samples,
    round(avg(voltage_v)::numeric, 1) AS avg_voltage_v,
    round(min(voltage_v)::numeric, 1) AS min_voltage_v,
    round(avg(current_a)::numeric, 1) AS avg_current_a,
    round(avg(frequency_hz)::numeric, 3) AS avg_freq_hz,
    count(*) FILTER (WHERE fault)     AS fault_samples,
    max(time)                         AS last_seen
FROM readings
WHERE time > now() - interval '5 minutes'
GROUP BY sensor_id
ORDER BY sensor_id;