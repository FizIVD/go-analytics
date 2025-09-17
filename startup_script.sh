#!/bin/bash

# ANSI color codes
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting services check...${NC}"

# --- Check ClickHouse ---
echo -e "${YELLOW}Waiting for ClickHouse to be healthy...${NC}"
while ! nc -z clickhouse 8123; do
    sleep 1
done
echo -e "${GREEN}ClickHouse is up.${NC}"

echo -e "${YELLOW}Checking ClickHouse tables...${NC}"
DB_EXISTS=$(clickhouse-client -h clickhouse --query="EXISTS DATABASE events")
TABLE_RAW_EXISTS=$(clickhouse-client -h clickhouse --query="EXISTS TABLE events.events_raw")
TABLE_KAFKA_EXISTS=$(clickhouse-client -h clickhouse --query="EXISTS TABLE events.kafka_events")
VIEW_EXISTS=$(clickhouse-client -h clickhouse --query="EXISTS TABLE events.kafka_to_raw")

if [ "$DB_EXISTS" -eq 1 ] && [ "$TABLE_RAW_EXISTS" -eq 1 ] && [ "$TABLE_KAFKA_EXISTS" -eq 1 ] && [ "$VIEW_EXISTS" -eq 1 ]; then
    echo -e "${GREEN}Database and tables exist.${NC}"
    RECORD_COUNT=$(clickhouse-client -h clickhouse --query="SELECT count() FROM events.events_raw")
    LAST_RECORD_TIME=$(clickhouse-client -h clickhouse --query="SELECT formatDateTime(max(event_time), '%d.%m.%Y %H:%M:%S') FROM events.events_raw")
    echo -e "${GREEN}ClickHouse is ready to accept data. Table events.events_raw has ${RECORD_COUNT} records. Last record time: ${LAST_RECORD_TIME}${NC}"
else
    echo -e "${RED}Database or tables do not exist.${NC}"
fi

# --- Check Kafka ---
echo -e "${YELLOW}Waiting for Kafka to be healthy...${NC}"
while ! nc -z kafka 9092; do
    sleep 1
done
echo -e "${GREEN}Kafka is up.${NC}"

echo -e "${YELLOW}Checking Kafka topics...${NC}"
TOPIC_EXISTS=$(kcat -b kafka:9092 -L -t user-events)
if [ -n "$TOPIC_EXISTS" ]; then
    echo -e "${GREEN}Topic 'user-events' exists.${NC}"
    MESSAGE_COUNT=$(kcat -b kafka:9092 -t user-events -C -e -q | wc -l)
    # The time of the last message is not easily available with standard tools.
    # I will report the number of messages.
    echo -e "${GREEN}Kafka is ready. Topic 'user-events' contains ${MESSAGE_COUNT} messages.${NC}"
else
    echo -e "${RED}Topic 'user-events' does not exist.${NC}"
fi

# --- Check API ---
echo -e "${YELLOW}Waiting for API to be healthy...${NC}"
while ! wget --spider -q http://api:8080/health; do
    sleep 1
done
echo -e "${GREEN}API is ready to accept requests.${NC}"


echo -e "${BLUE}All services checked.${NC}"
