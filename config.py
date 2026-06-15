# -*- coding: utf-8 -*-
"""Общие константы проекта: адреса сервисов, имена топиков, пути к JAR-коннекторам."""
import os

# Kafka
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_EVENTS = "iot_events"          # сырые события IoT
TOPIC_AGGREGATES = "iot_aggregates"  # результат агрегации

# PostgreSQL (поднимается в docker-compose)
PG_URL = "jdbc:postgresql://localhost:5432/iot"
PG_USER = "postgres"
PG_PASSWORD = "postgres"
PG_TABLE = "device_types"

# Набор id типов устройств (должен совпадать с dml.sql)
DEVICE_TYPE_IDS = [1, 2, 3, 4]

# Пути к JAR-коннекторам (скачиваются в ./jars, см. README)
_JARS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")

def _jar(name: str) -> str:
    return "file://" + os.path.join(_JARS_DIR, name)

KAFKA_CONNECTOR = _jar("flink-sql-connector-kafka-3.3.0-1.20.jar")
JDBC_CONNECTOR = _jar("flink-connector-jdbc-3.3.0-1.20.jar")
PG_DRIVER = _jar("postgresql-42.7.4.jar")
