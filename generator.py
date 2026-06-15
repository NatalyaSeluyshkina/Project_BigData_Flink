#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Генератор сообщений IoT-устройств.
Раз в секунду формирует событие и публикует JSON в топик Kafka iot_events."""
import json
import random
import time

from kafka import KafkaProducer

import config


def build_event(type_ids):
    """Сформировать одно событие IoT.
    type_ids — список допустимых идентификаторов типов устройств (из справочника PG)."""
    return {
        "device_id": random.randint(1, 100),          # идентификатор устройства
        "type_id": random.choice(type_ids),           # тип устройства (ссылка на справочник)
        "event_time": int(time.time() * 1000),         # время события, epoch мс
        "temperature": round(random.uniform(15.0, 35.0), 2),  # температура
        "humidity": round(random.uniform(20.0, 90.0), 2),     # влажность
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=[config.KAFKA_BOOTSTRAP],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"✅ Producer подключён к {config.KAFKA_BOOTSTRAP}")
    print(f"📤 Публикуем события в топик '{config.TOPIC_EVENTS}' (Ctrl+C для остановки)\n")
    try:
        while True:
            event = build_event(config.DEVICE_TYPE_IDS)
            producer.send(config.TOPIC_EVENTS, value=event)
            print(f"✉️  {event}")
            time.sleep(1)  # раз в секунду
    except KeyboardInterrupt:
        print("\n⏹  Остановка генератора")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
