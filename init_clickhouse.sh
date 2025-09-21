#!/bin/bash
set -e

# Подставляем переменные в SQL скрипт
envsubst < /docker-entrypoint-initdb.d/init_clickhouse.sql.template > /docker-entrypoint-initdb.d/init_clickhouse.sql

# Запускаем стандартный entrypoint ClickHouse
exec /entrypoint.sh