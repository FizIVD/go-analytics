-- Создание базы данных
CREATE DATABASE IF NOT EXISTS events;

-- Целевая таблица
CREATE TABLE events.events_raw
(
    event_id String,
    event_time DateTime64(3),
    device_id String,
    profile_id Int64,
    action String,
    extras String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY event_time
SETTINGS index_granularity = 8192;

-- Таблица с Kafka-сообщениями
CREATE TABLE events.kafka_events
(
    `value` String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'user-events',
    kafka_group_name = 'ch-group',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 4,
    kafka_commit_every_batch = 1;

-- Материализованное представление для парсинга JSON
CREATE MATERIALIZED VIEW events.kafka_to_raw TO events.events_raw AS
SELECT
    JSONExtractString(value, 'event_id') AS event_id,
    toDateTime64(JSONExtractUInt(value, 'event_time') / 1000, 3) AS event_time,
    JSONExtractString(value, 'device_id') AS device_id,
    JSONExtractInt(value, 'profile_id') AS profile_id,
    JSONExtractString(value, 'action') AS action,
    JSONExtractRaw(value, 'extras') AS extras
FROM events.kafka_events;
