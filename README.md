# Kafka Stock Streaming Pipeline

A fault-tolerant, real-time data pipeline that streams stock prices from a MySQL database through Kafka into HDFS, with checkpointing so it recovers exactly where it left off after a crash. The full system runs locally in Docker.

Built for CS 544: Introduction to Big Data Systems at UW–Madison (Spring 2026).

## How it works

```
price_generator → MySQL → producer → Kafka (4 partitions) → consumer → HDFS
```

1. **Generate:** `price_generator.py` writes new stock prices into a MySQL table.
2. **Produce:** `producer.py` polls MySQL for new rows only, serializes each one as a Protocol Buffers message, and publishes it to Kafka, keyed by ticker so each stock's prices stay in order.
3. **Consume:** `consumer.py` reads from Kafka, writes the data to HDFS, and saves its progress with checkpoints so a restart resumes at the right place instead of losing or reprocessing data.

## Reliability features

- **Waits for dependencies:** retries until Kafka and MySQL are ready, so containers can start in any order.
- **Durable writes:** the producer uses `acks="all"` and automatic retries, so messages aren't lost if a broker is slow.
- **Incremental reads:** tracks the last row sent, so only new data is published.
- **Crash recovery:** consumer checkpoints let the pipeline pick up exactly where it stopped.

## Tech stack

Python · Apache Kafka · Protocol Buffers · MySQL · SQLAlchemy · HDFS · Docker Compose

## Project structure

```
src/
  price_generator.py   Writes simulated stock prices to MySQL
  producer.py          MySQL → Kafka producer
  consumer.py          Kafka → HDFS consumer with checkpointing
  report.proto         Protocol Buffers message schema
  debug.py             Helper for inspecting messages
docker-compose.yml     Kafka, MySQL, and HDFS services
Dockerfile.*           Images for each service
```

## Running it

Requires Docker and Docker Compose.

```bash
docker compose up -d
```

The services start together, and the producer waits until Kafka and the database are ready before streaming.
