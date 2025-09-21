#!/bin/bash

# ANSI color codes
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Starting services check...${NC}"

# Опции для clickhouse-client (поддержка user/pass)
CLICKHOUSE_OPTS="-h clickhouse -m 8123"
[ -n "$CLICKHOUSE_USER" ] && CLICKHOUSE_OPTS="$CLICKHOUSE_OPTS -u $CLICKHOUSE_USER"
[ -n "$CLICKHOUSE_PASSWORD" ] && CLICKHOUSE_OPTS="$CLICKHOUSE_OPTS --password $CLICKHOUSE_PASSWORD"

# --- Check ClickHouse ---
echo -e "${YELLOW}Waiting for ClickHouse to be healthy...${NC}"
while ! nc -z clickhouse 8123; do
    sleep 1
done
echo -e "${GREEN}ClickHouse is up.${NC}"

echo -e "${YELLOW}Checking ClickHouse tables...${NC}"
DB_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="SELECT count(*) FROM system.databases WHERE name = 'events'" 2>/dev/null)
TABLE_RAW_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="EXISTS TABLE events.events_raw" 2>/dev/null)
TABLE_KAFKA_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="EXISTS TABLE events.kafka_events" 2>/dev/null)
VIEW_EXISTS=$(clickhouse-client $CLICKHOUSE_OPTS --query="EXISTS TABLE events.kafka_to_raw" 2>/dev/null)

if [ "$DB_EXISTS" = "1" ] && [ "$TABLE_RAW_EXISTS" = "1" ] && [ "$TABLE_KAFKA_EXISTS" = "1" ] && [ "$VIEW_EXISTS" = "1" ]; then
    echo -e "${GREEN}Database and tables exist.${NC}"
    RECORD_COUNT=$(clickhouse-client $CLICKHOUSE_OPTS --query="SELECT count() FROM events.events_raw" 2>/dev/null || echo 0)
    LAST_RECORD_TIME=$(clickhouse-client $CLICKHOUSE_OPTS --query="SELECT formatDateTime(max(event_time), '%d.%m.%Y %H:%M:%S') FROM events.events_raw" 2>/dev/null || echo "N/A")
    echo -e "${GREEN}ClickHouse is ready to accept data. Table events.events_raw has ${RECORD_COUNT} records. Last record time: ${LAST_RECORD_TIME}${NC}"
else
    echo -e "${RED}Database or tables do not exist.${NC}"
fi

# Проверяем пользователя
if [ -n "$CLICKHOUSE_USER" ]; then
    echo -e "${YELLOW}Testing connection with user: ${CLICKHOUSE_USER}${NC}"
    
    if clickhouse-client $CLICKHOUSE_OPTS --query="SELECT 1" >/dev/null 2>&1; then
        echo -e "${GREEN}Successfully connected as ${CLICKHOUSE_USER}${NC}"
        
        # Получаем список ролей и привилегий пользователя
        USER_ROLES=$(clickhouse-client $CLICKHOUSE_OPTS --raw --query="SELECT arrayStringConcat(groupArray(access_type), ', ') FROM system.grants WHERE entity_name = '$CLICKHOUSE_USER'" 2>/dev/null || echo "[]")
        echo -e "${GREEN}User ${CLICKHOUSE_USER} has grants: ${USER_ROLES}${NC}"
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



echo -e "${YELLOW}Checking Kafka topics...${NC}"
sleep 10 # Ждем немного, чтобы Kafka полностью инициализировался
# Проверяем список топиков и удаляем лишние символы
TOPICS_LIST=$(kcat -b kafka:9092 -L 2>/dev/null | grep -c "user-events" 2>/dev/null || echo "0")
# Убедимся, что это число
TOPICS_LIST=$(echo "$TOPICS_LIST" | tr -d '\n\r ')

if [ "$TOPICS_LIST" -gt 0 ] 2>/dev/null; then
    echo -e "${GREEN}Topic 'user-events' exists.${NC}"
    echo -e "${GREEN}Kafka is ready. Topic 'user-events' is available.${NC}"
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