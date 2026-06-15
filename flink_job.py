#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Основной Flink-пайплайн итогового проекта (работа в event time).

Поток обработки:
  1. [Table API] источник Kafka (iot_events) и JDBC-справочник PostgreSQL (device_types);
  2. SQL lookup join: к каждому событию подтягиваем наименование типа устройства;
  3. переход Table -> DataStream;
  4. минутное окно по типу устройства (event time): средняя температура и медиана влажности
     считаются в ProcessWindowFunction на Python;
  5. переход DataStream -> Table;
  6. [Table API] запись результата в Kafka (iot_aggregates).
"""
import sys
from datetime import datetime

from pyflink.common import Configuration, Types, WatermarkStrategy, Time, Row, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment, Schema, DataTypes

import config
from aggregates import avg, median, format_minute


class EventTimestampAssigner(TimestampAssigner):
    """Извлекает event-time (epoch мс) из строки join-потока для назначения watermark."""
    def extract_timestamp(self, value, record_timestamp):
        # value — Row(type_id, type_name, temperature, humidity, event_time)
        return value[4]


class MinuteAggregate(ProcessWindowFunction):
    """Считает за минутное окно по каждому типу устройства:
    среднюю температуру и медиану влажности."""
    def process(self, key, context, elements):
        temps, hums = [], []
        type_name = None
        for e in elements:
            temps.append(e[2])      # temperature
            hums.append(e[3])       # humidity
            type_name = e[1]        # type_name (одинаков в пределах ключа)
        if not temps:               # окно без событий не публикуем
            return
        yield Row(
            format_minute(context.window().start),  # window_time 'hh:mm'
            type_name,
            round(avg(temps), 2),                    # средняя температура
            round(median(hums), 2),                  # медиана влажности
        )


def build_tenv():
    conf = Configuration()
    conf.set_integer("rest.port", 8081)
    conf.set_string("execution.runtime-mode", "STREAMING")
    # Указываем Flink интерпретатор Python из venv: иначе порождённый
    # python-процесс не находит pyflink. sys.executable = текущий venv python.
    conf.set_string("python.executable", sys.executable)
    conf.set_string("python.client.executable", sys.executable)
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)
    # JAR-коннекторы Kafka и JDBC + драйвер PostgreSQL
    env.add_jars(config.KAFKA_CONNECTOR, config.JDBC_CONNECTOR, config.PG_DRIVER)
    tenv = StreamTableEnvironment.create(env)
    return env, tenv


def create_source_tables(tenv):
    # Источник Kafka (Table API). proc_time нужен для темпорального lookup join.
    tenv.execute_sql(f"""
        CREATE TEMPORARY TABLE iot_events (
            device_id    INT,
            type_id      INT,
            event_time   BIGINT,
            temperature  DOUBLE,
            humidity     DOUBLE,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.TOPIC_EVENTS}',
            'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink_iot',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # Источник-справочник PostgreSQL (Table API, JDBC). Используется как lookup-таблица.
    tenv.execute_sql(f"""
        CREATE TEMPORARY TABLE device_types (
            id        INT,
            type_name STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{config.PG_URL}',
            'table-name' = '{config.PG_TABLE}',
            'username' = '{config.PG_USER}',
            'password' = '{config.PG_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)


def create_sink_table(tenv):
    # Приёмник результата в Kafka (Table API)
    tenv.execute_sql(f"""
        CREATE TEMPORARY TABLE iot_aggregates (
            window_time      STRING,
            type_name        STRING,
            avg_temperature  DOUBLE,
            median_humidity  DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.TOPIC_AGGREGATES}',
            'properties.bootstrap.servers' = '{config.KAFKA_BOOTSTRAP}',
            'format' = 'json'
        )
    """)


def joined_table(tenv):
    # Lookup join: к каждому событию из Kafka подтягиваем type_name из справочника PG.
    return tenv.sql_query("""
        SELECT
            k.type_id      AS type_id,
            d.type_name    AS type_name,
            k.temperature  AS temperature,
            k.humidity     AS humidity,
            k.event_time   AS event_time
        FROM iot_events AS k
        JOIN device_types FOR SYSTEM_TIME AS OF k.proc_time AS d
          ON k.type_id = d.id
    """)


def main():
    env, tenv = build_tenv()
    create_source_tables(tenv)
    create_sink_table(tenv)
    joined = joined_table(tenv)

    # Переход Table -> DataStream
    ds = tenv.to_data_stream(joined)

    # Event-time окно 1 минута по типу устройства, агрегация в ProcessWindowFunction
    result_ds = (
        ds.assign_timestamps_and_watermarks(
            # допускаем опоздание событий до 5 секунд
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(EventTimestampAssigner())
        )
        .key_by(lambda r: r[0], key_type=Types.INT())  # ключ = type_id
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(
            MinuteAggregate(),
            output_type=Types.ROW_NAMED(
                ["window_time", "type_name", "avg_temperature", "median_humidity"],
                [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
            ),
        )
    )

    # Переход DataStream -> Table
    result_table = tenv.from_data_stream(
        result_ds,
        Schema.new_builder()
        .column("window_time", DataTypes.STRING())
        .column("type_name", DataTypes.STRING())
        .column("avg_temperature", DataTypes.DOUBLE())
        .column("median_humidity", DataTypes.DOUBLE())
        .build(),
    )

    # Запись результата в Kafka (Table API sink)
    result_table.execute_insert("iot_aggregates").wait()


if __name__ == "__main__":
    main()
