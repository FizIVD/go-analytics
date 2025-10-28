#!/bin/bash

# ANSI color codes
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting services check...${NC}"

# Порты ClickHouse
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000

# Общие параметры
CLICKHOUSE_OPTS="-h clickhouse --port $CLICKHOUSE_NATIVE_PORT"
[ -n "$CLICKHOUSE_USER" ] && CLICKHOUSE_OPTS="$CLICKHOUSE_OPTS -u $CLICKHOUSE_USER"
[ -n "$CLICKHOUSE_PASSWORD" ] && CLICKHOUSE_OPTS="$CLICKHOUSE_OPTS --password $CLICKHOUSE_PASSWORD"

CLICKHOUSE_HTTP_OPTS="-h clickhouse --port $CLICKHOUSE_HTTP_PORT"
[ -n "$CLICKHOUSE_USER" ] && CLICKHOUSE_HTTP_OPTS="$CLICKHOUSE_HTTP_OPTS -u $CLICKHOUSE_USER"
[ -n "$CLICKHOUSE_PASSWORD" ] && CLICKHOUSE_HTTP_OPTS="$CLICKHOUSE_HTTP_OPTS --password $CLICKHOUSE_PASSWORD"

# --- Check ClickHouse ---
echo -e "${YELLOW}Waiting for ClickHouse to be healthy...${NC}"
while ! nc -z clickhouse $CLICKHOUSE_HTTP_PORT; do
    sleep 1
done
echo -e "${GREEN}ClickHouse is up.${NC}"

echo -e "${YELLOW}Checking ClickHouse tables...${NC}"
DB_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="SELECT count() FROM system.databases WHERE name = 'events'" 2>/dev/null)
TABLE_RAW_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="EXISTS TABLE events.events_raw" 2>/dev/null)
TABLE_KAFKA_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="EXISTS TABLE events.kafka_events" 2>/dev/null)
VIEW_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="EXISTS TABLE events.kafka_to_raw" 2>/dev/null)

if [ "$DB_EXISTS" = "1" ] && [ "$TABLE_RAW_EXISTS" = "1" ] && [ "$TABLE_KAFKA_EXISTS" = "1" ] && [ "$VIEW_EXISTS" = "1" ]; then
    echo -e "${GREEN}Database and tables exist.${NC}"
    RECORD_COUNT=$(clickhouse-client $CLICKHOUSE_OPTS --query="SELECT count() FROM events.events_raw" 2>/dev/null || echo 0)
    LAST_RECORD_TIME=$(clickhouse-client $CLICKHOUSE_OPTS --query="SELECT formatDateTime(max(event_time), '%d.%m.%Y %H:%i:%S') FROM events.events_raw" 2>/dev/null || echo "N/A")
    echo -e "${GREEN}ClickHouse is ready. Table events.events_raw has ${RECORD_COUNT} records. Last record time: ${LAST_RECORD_TIME}${NC}"
else
    echo -e "${RED}Database or tables do not exist.${NC}"
fi

# --- Check user and grants ---
if [ -n "$CLICKHOUSE_USER" ]; then
    echo -e "${YELLOW}Testing connection with user: ${CLICKHOUSE_USER}${NC}"
    
    if clickhouse-client $CLICKHOUSE_OPTS --query="SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}Successfully connected as ${CLICKHOUSE_USER}${NC}"
    else
        echo -e "${RED}Failed to connect as ${CLICKHOUSE_USER}${NC}"
    fi
fi

# --- Check Kafka ---
echo -e "${YELLOW}Waiting for Kafka to be healthy...${NC}"
while ! nc -z kafka 9092; do
    sleep 1
done
echo -e "${GREEN}Kafka is up.${NC}"

echo -e "${YELLOW}Waiting for Kafka topic 'user-events'...${NC}"
until kcat -b kafka:9092 -L 2>/dev/null | grep -q "user-events"; do
    sleep 1
done
echo -e "${GREEN}Kafka is ready. Topic 'user-events' exists.${NC}"

# --- Check API ---
echo -e "${YELLOW}Waiting for API to be healthy...${NC}"
until wget --spider -q http://api:8080/health; do
    sleep 1
done
echo -e "${GREEN}API is ready to accept requests.${NC}"

echo -e "${BLUE}All services checked.${NC}"
