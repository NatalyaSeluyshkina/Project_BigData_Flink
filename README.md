# Итоговый проект BigData — Flink IoT-телеметрия

Потоковый пайплайн на **Apache Flink (PyFlink)**:

```
generator.py → Kafka(iot_events) → flink_job.py → Kafka(iot_aggregates) → consumer.py
                                        ↑
                          PostgreSQL device_types (JDBC-справочник)
```

Генератор IoT-событий публикует сообщения в Kafka. Flink в режиме **event time**
соединяет события со статичным справочником типов устройств из PostgreSQL, считает за
каждую минуту по каждому типу устройства **среднюю температуру** и **медиану влажности**
и пишет результат обратно в Kafka. Источник и приёмник реализованы на **SQL/Table API**;
внутри джоба есть переход **Table → DataStream** (оконная агрегация с медианой) и обратно
**DataStream → Table**.

## Состав

| Файл                | Назначение                                                        |
|---------------------|-------------------------------------------------------------------|
| `docker-compose.yml`| Kafka + Zookeeper + PostgreSQL                                    |
| `sql/ddl.sql`       | DDL справочника `device_types`                                    |
| `sql/dml.sql`       | Наполнение справочника типов устройств                            |
| `config.py`         | Общие константы (адреса, топики, пути к JAR)                      |
| `aggregates.py`     | Чистые функции: среднее, медиана, формат `hh:mm` (+ юнит-тесты)   |
| `generator.py`      | Генератор IoT-событий → Kafka                                     |
| `flink_job.py`      | Flink-пайплайн                                                    |
| `consumer.py`       | Проверочный consumer итогового топика                            |

## Требования
- Docker + Docker Compose
- **Python 3.11** (важно: `apache-flink==1.20.*` поддерживает Python 3.8–3.11,
  на 3.12+ установка не пройдёт)
- Java 11 / 17 / 21 (проверено на Java 21)

## 1. Поднять инфраструктуру (Kafka + Postgres)

```bash
docker compose up -d
```

Справочник `device_types` создаётся и наполняется автоматически из `sql/ddl.sql`
и `sql/dml.sql`. Проверка:

```bash
docker compose exec postgres psql -U postgres -d iot -c "SELECT * FROM device_types;"
```

## 2. Установить Python-зависимости (Python 3.11)

```bash
python3.11 -m venv venv
source venv/bin/activate
```

Из-за известной несовместимости новой `setuptools` (≥81) со сборкой зависимости
`apache-beam` установку нужно запускать с ограничением `setuptools<81`:

```bash
printf "setuptools<81\n" > /tmp/flink_constraints.txt
PIP_CONSTRAINT=/tmp/flink_constraints.txt pip install -r requirements.txt
```

Проверка: `python -c "import pyflink, pyflink.version; print(pyflink.version.__version__)"` → `1.20.1`.

## 3. Скачать JAR-коннекторы в ./jars

```bash
mkdir -p jars && cd jars
curl -sSL -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
curl -sSL -O https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar
curl -sSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
cd ..
```

Имена файлов должны совпадать с путями в `config.py`.

## 4. Запустить (три терминала, в каждом активирован venv)

Терминал 1 — генератор событий (раз в секунду):
```bash
source venv/bin/activate && python generator.py
```

Терминал 2 — Flink-пайплайн (первые результаты появляются через 1–2 минуты, после
закрытия первого минутного окна):
```bash
source venv/bin/activate && python flink_job.py
```

Терминал 3 — проверочный consumer:
```bash
source venv/bin/activate && python consumer.py
```

Пример вывода `consumer.py`:
```
12:07 | Датчик температуры           | avg t= 26.44 | median h= 67.66
12:07 | Метеостанция                 | avg t=  23.7 | median h= 64.66
12:07 | Датчик влажности почвы       | avg t= 31.77 | median h=  35.8
12:07 | Датчик температуры теплицы   | avg t= 23.75 | median h= 49.98
```

Flink Web UI: http://localhost:8081

> Примечание: `flink_job.py` сам прописывает интерпретатор Python из venv
> (`python.executable`), поэтому отдельной настройки не требуется — достаточно
> запускать его питоном из venv.

## 5. Тесты

```bash
source venv/bin/activate && python -m pytest tests/ -v
```

## 6. Остановка

```bash
docker compose down -v
```

## Как устроен `flink_job.py`

1. `[Table API]` источник Kafka `iot_events` (JSON) и JDBC-справочник `device_types`.
2. SQL **lookup join** (`FOR SYSTEM_TIME AS OF`) — к каждому событию подтягивается
   `type_name` из PostgreSQL.
3. Переход **Table → DataStream** (`to_data_stream`).
4. Назначение watermark по `event_time`, `key_by` по типу устройства,
   **окно Tumbling 1 минута (event time)**; в `ProcessWindowFunction` на Python
   считаются средняя температура и точная медиана влажности.
5. Переход **DataStream → Table** (`from_data_stream`).
6. `[Table API]` запись результата в Kafka-топик `iot_aggregates` (JSON):
   `window_time` (hh:mm), `type_name`, `avg_temperature`, `median_humidity`.
