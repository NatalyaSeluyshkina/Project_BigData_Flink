#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверочный consumer: читает итоговый топик iot_aggregates и печатает результаты."""
import json

from kafka import KafkaConsumer

import config


def main():
    consumer = KafkaConsumer(
        config.TOPIC_AGGREGATES,
        bootstrap_servers=[config.KAFKA_BOOTSTRAP],
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"👂 Читаем топик '{config.TOPIC_AGGREGATES}' (Ctrl+C для остановки)\n")
    try:
        for msg in consumer:
            r = msg.value
            print(
                f"{r['window_time']} | {r['type_name']:<28} | "
                f"avg t={r['avg_temperature']:>6} | median h={r['median_humidity']:>6}"
            )
    except KeyboardInterrupt:
        print("\n⏹  Остановка consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
