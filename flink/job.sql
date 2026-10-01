-- Creates the Iceberg tables and the streaming job that fills them.
--
-- Submitted with the SQL client, so it runs on the stock JVM-only Flink image
-- (there is no PyFlink image variant for 1.20.1):
--   docker compose exec jobmanager /opt/flink/bin/sql-client.sh -f /opt/job/job.sql
--
-- Two tables, deliberately:
--
--   events     the upsert target. Iceberg v2, partitioned by day, PRIMARY KEY
--               on event_id so the generator's duplicate ids collapse to one
--               row instead of two.
--   events_v1  append-only, never changes shape. Gives the schema-evolution
--               scenarios (TRA-48) something stable to compare against.
--
-- The upsert key is the point of the exercise. Without a PRIMARY KEY, Flink
-- appends and the duplicate check in the scorecard is guaranteed to find
-- duplicates; with it, a duplicate event_id updates the existing row and the
-- check stays green. That is what TRA-45 asks to prove.
--
-- On partitioning: this Flink build's parser rejects a function call inside
-- PARTITIONED BY (`days(event_time)` fails with a ParseException), so the day
-- is materialised as a DATE column and partitioned on that. Same Iceberg v2
-- day-partitioned layout, expressed in a form the parser accepts.

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'pipeline.name' = 'lakehouse-ingest';
SET 'parallelism.default' = '2';

-- The source. event_time is declared TIMESTAMP_LTZ(3) and parsed by the JSON
-- format's own ISO-8601 reader rather than by CAST(string AS TIMESTAMP_LTZ).
-- That cast uses Flink's default `yyyy-MM-dd HH:mm:ss` and dies on the `T`
-- separator at index 10; the connector's ISO-8601 parser is what actually
-- understands the `...T...Z` the generator emits.
CREATE TABLE IF NOT EXISTS `events_source` (
    event_id   STRING,
    user_id    STRING,
    event_time TIMESTAMP_LTZ(3),
    event_type STRING,
    campaign_id STRING,
    country    STRING,
    revenue    DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'lakehouse-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE IF NOT EXISTS `events` (
    event_id     STRING,
    user_id      STRING,
    event_time   TIMESTAMP_LTZ(3),
    event_date   DATE,
    event_type   STRING,
    campaign_id  STRING,
    country      STRING,
    revenue      DOUBLE,
    processing_time TIMESTAMP_LTZ(3),
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (event_date)
WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'lakehouse',
    'catalog-type' = 'hadoop',
    'warehouse' = 'file:///warehouse',
    'format' = 'iceberg',
    'write-format' = 'parquet',
    'streaming' = 'true',
    'write-mode' = 'upsert',
    'full-compaction.delta-commits' = 'true'
);

CREATE TABLE IF NOT EXISTS `events_v1` (
    event_id     STRING,
    user_id      STRING,
    event_time   TIMESTAMP_LTZ(3),
    event_date   DATE,
    event_type   STRING,
    campaign_id  STRING,
    country      STRING,
    revenue      DOUBLE,
    processing_time TIMESTAMP_LTZ(3)
) PARTITIONED BY (event_date)
WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'lakehouse',
    'catalog-type' = 'hadoop',
    'warehouse' = 'file:///warehouse',
    'format' = 'iceberg',
    'write-format' = 'parquet',
    'write-mode' = 'append'
);

-- The upsert sink. Attaching this to the end makes submitting the file both
-- create the schema and start ingestion.
--
-- event_date is derived from event_time here rather than in the source schema,
-- so the partition key cannot disagree with the timestamp it comes from.
INSERT INTO `events`
SELECT
    event_id,
    user_id,
    event_time,
    -- The partition key is derived here rather than read from the source, so it
    -- cannot disagree with the timestamp it comes from.
    CAST(event_time AS DATE),
    event_type,
    campaign_id,
    country,
    revenue,
    CURRENT_TIMESTAMP
FROM `events_source`;