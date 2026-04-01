# TheLook E-commerce Lakehouse

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0-017CEE?logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694A?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Postgres](https://img.shields.io/badge/Postgres-15-%23316192.svg?logo=postgresql&logoColor=white)](#)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-000000?logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Debezium](https://img.shields.io/badge/Debezium-3.0-931812)](https://debezium.io/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-FF694A?logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-00D4FF?logo=delta&logoColor=white)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3%20Compatible-C72C48?logo=minio&logoColor=white)](https://min.io/)
[![Trino](https://img.shields.io/badge/Trino-latest-FF4B4B?logo=trino&logoColor=white)](https://trino.io/)
[![Superset](https://img.shields.io/badge/Apache%20Superset-3.1-FF694A?logo=apache-superset&logoColor=white)](https://superset.apache.org/)

A streaming data lakehouse built on the **TheLook E-commerce** dataset, following the **Medallion Architecture**. The platform captures real-time product, order, and user events from PostgreSQL via Debezium CDC into Kafka, processes them through Spark Structured Streaming onto Delta Lake (MinIO), and serves business-ready metrics through Trino and Superset dashboards — all orchestrated by Airflow and dbt.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Problem Statement & Goals](#2-problem-statement--goals)
3. [Architecture](#3-architecture)
4. [Data Modeling](#4-data-modeling)
5. [Key Features](#5-key-features)
6. [Quick Start](#6-quick-start)
7. [Tech Stack](#7-tech-stack)
8. [Project Structure](#8-project-structure)
9. [Service Endpoints](#9-service-endpoints)
10. [Dashboards](#10-dashboards)
11. [Future Roadmap](#11-future-roadmap)

---

## 1. Overview

TheLook E-commerce is a self-contained data lakehouse that simulates a modern e-commerce platform's data infrastructure. The system ingests real-time transaction data (orders, items, users, events, products) from a PostgreSQL source via Debezium CDC into Kafka, then processes and serves it across three analytical layers following the Medallion Architecture:

- **Bronze (staging):** Raw CDC events captured verbatim from Kafka — contains Debezium envelope (`operation`, `before`/`after` payloads, `event_ts_ms`).
- **Silver (intermediate):** Deduplicated, cleaned, and type-corrected data — one record per entity key.
- **Gold (mart):** Business-ready dimension and fact tables — customer segments, order metrics, product performance, session funnels.

The serving layer uses Trino to query Delta Lake tables directly via Hive Metastore, and Superset for BI dashboards.

---

## 2. Problem Statement & Goals

### The Challenge

E-commerce platforms generate high-velocity, heterogeneous data streams — user events, orders, product updates, inventory changes — that traditional batch ETL cannot capture with sufficient freshness. Meanwhile, analysts need up-to-date customer segments, product performance metrics, and cohort analysis without impacting the operational database.

### Project Goals

- **Real-time CDC Ingestion:** Capture every insert, update, and delete from PostgreSQL WAL via Debezium into Kafka with millisecond latency.
- **Streaming-first Lakehouse:** Use Spark Structured Streaming to write incremental micro-batches to Delta Lake on MinIO, decoupling storage from compute.
- **Medallion Data Quality:** Progress data through three quality tiers (staging → intermediate → mart) with deduplication, type enforcement, and business logic.
- **SQL-first Serving:** Enable ad-hoc and dashboard queries via Trino on Delta Lake, without duplicating data into a separate serving database.
- **Automated Orchestration:** Orchestrate the entire pipeline (data generation → CDC → streaming → dbt transformations → serving) with Airflow DAGs.

### Key Business Questions

The platform is designed to answer:

**Customer Intelligence**
- Who are the high-value customers, and what drives their lifetime value?
- How do customer cohorts behave over time (retention, repeat purchases)?
- What is the funnel from session → cart → order?

**Product Performance**
- Which products drive the most revenue? Which are underperforming?
- How does product mix vary by category, brand, and price tier?

**Operational Analytics**
- What is the daily order volume and revenue trend?
- How does fulfillment performance vary by distribution center?
- What is the session-level event funnel (page views → add to cart → checkout)?

---

## 3. Architecture

![Architecture Diagram](./assets/architecture.png)

Data flows through the platform as follows:

1. **Source (OLTP):** TheLook data generator continuously writes synthetic e-commerce events into PostgreSQL.
2. **CDC Ingestion:** Debezium captures row-level changes from PostgreSQL WAL (pgoutput plugin) and publishes them to Kafka topics in Avro format (Schema Registry enforced).
3. **Bronze Layer (staging):** Spark Structured Streaming consumes Kafka topics and writes raw CDC records to Delta Lake on MinIO — one partition per topic.
4. **Silver Layer (intermediate):** Spark deduplicates records per entity key, cleans data types, and applies business rules. dbt handles incremental logic with watermark-based filtering.
5. **Gold Layer (mart):** dbt models aggregate data into dimension and fact tables (dim_customers, dim_products, dim_date, fct_orders, etc.) stored as Delta tables.
6. **Serving:** Trino queries Delta Lake tables via Hive Metastore for ad-hoc SQL. Superset connects to Trino for visualization.
7. **Orchestration:** Airflow DAGs schedule and monitor the full pipeline lifecycle.

---

## 4. Data Modeling

![Data Model](./assets/data_model.png)

The analytical schema follows a **star schema** pattern optimized for BI queries:

- **Dimensions:** `dim_customers`, `dim_products`, `dim_date` — slowly changing attributes, customer tiers, product price tiers.
- **Facts:** `fct_orders`, `fct_order_items`, `fct_events`, `fct_sessions` — atomic transactions and events at the grain of order, item, and session.

Key modeling decisions:
- `event_ts_ms` (epoch milliseconds from Debezium) is the watermark/dedup key — not business timestamps.
- `created_at` columns in orders are epoch **seconds** — requires division by 1000 before casting to timestamp.
- `dim_customers` uses a `changed_users` watermark CTE: only recalculates customers with CDC events since the last run, preserving SCD semantics without full table rewrite.
- `dim_date` is a static date spine (2020–2030) generated via `UNNEST(sequence(...))` at Trino runtime.

---

## 5. Key Features

- **Real-time CDC Pipeline**
  Debezium captures every row-level change from PostgreSQL WAL using the `pgoutput` plugin. Each event carries the full `before`/`after` state, the operation type (`c`/`u`/`d`/`r`), and the event timestamp in epoch milliseconds. Schema Registry enforces Avro schemas on all Kafka topics, ensuring schema evolution is tracked and validated.

- **Spark Structured Streaming**
  Spark 3.5 reads from Kafka using the `kafka` source, deserializes Avro via Schema Registry, and writes micro-batches to Delta Lake on MinIO every trigger interval. Checkpointing ensures exactly-once semantics across restarts.

- **Delta Lake on MinIO**
  All layers (staging, intermediate, mart) use Delta Lake format, providing ACID transactions, schema enforcement, and time-travel capabilities on S3-compatible object storage.

- **Hive Metastore + Trino**
  Hive Metastore 4.1 manages table metadata (schema, partitioning, location) for all Delta tables. Trino serves as the SQL query engine, federating queries across all three medallic schemas (`staging`, `intermediate`, `mart`) without data duplication.

- **dbt Transformations**
  dbt 1.7 models implement the intermediate and mart layers. Materialization strategies: `ephemeral` for staging models (CTEs only), `incremental` with `delete+insert` merge for intermediate models, and `table` with `on_table_exists: drop` for mart models. Tests enforce data quality (not-null, unique, relationship, accepted values, range bounds).

- **Airflow Orchestration**
  Airflow 3.0 DAGs orchestrate the full pipeline: data generation → CDC capture → Spark streaming → dbt transformation → data freshness checks. Cosmos provider enables native dbt integration with Airflow.

- **Data Generator**
  A Python-based synthetic data generator produces realistic TheLook e-commerce events (orders, items, users, products, events, sessions) and inserts them into PostgreSQL, triggering CDC events downstream.

---

## 6. Quick Start

### Prerequisites

- **Docker & Docker Compose v2**
- **RAM:** 8GB minimum (16GB+ recommended)
- **Ports:** 5432, 9092, 29092, 8081, 8083, 8085, 8088, 8089, 8090, 9000, 9001, 9083, 9084, 9085

### Start Services

```bash
make up-core           # Core services (Postgres, Kafka, Spark, MinIO, Trino, HMS)
make up-datagen        # + data generator
make up-airflow        # + Airflow + dbt
make up-explore        # + JupyterLab
make up-all            # Everything (core + datagen + explore + airflow)
make down              # Stop all
make ps                # Show running containers
```

### Run dbt

```bash
make build-dbt         # Build dbt image (first time only)
make ps                # Wait for services healthy, then:
docker compose exec dbt dbt run --project-dir /dbt --profiles-dir /dbt
```

---

## 7. Tech Stack

| Category | Technology | Version | Purpose |
|---|---|---|---|
| Source DB | PostgreSQL | 15 | Core OLTP database with WAL enabled for CDC |
| CDC | Debezium | 3.0 | Capture row-level changes from PostgreSQL WAL |
| Message Broker | Apache Kafka | 3.9 (KRaft) | Decouple ingestion from downstream processing |
| Schema Registry | Confluent Schema Registry | 8.0 | Avro schema enforcement on Kafka topics |
| Stream Processing | Apache Spark | 3.5.6 | Structured Streaming from Kafka to Delta Lake |
| Table Format | Delta Lake | 3.0 | ACID storage, schema enforcement, time travel |
| Object Storage | MinIO | latest | S3-compatible data lake storage |
| Metastore | Hive Metastore + MariaDB | 4.1 / 10.5 | Table metadata management |
| Query Engine | Trino | latest | SQL query engine on Delta Lake |
| Orchestration | Apache Airflow + dbt | 3.0 / 1.7 | Pipeline scheduling and transformation management |
| Visualization | Apache Superset | 3.1 | BI dashboards |
| Notebook | JupyterLab | latest | Interactive data exploration |

---

## 8. Project Structure

```
.
├── assets/                    # Architecture diagrams, dashboard screenshots
├── data-generator/            # Synthetic TheLook data generator
├── infra/
│   ├── airflow/              # Airflow configuration and DAGs
│   ├── spark/                # Spark cluster (master + worker)
│   ├── hive-metastore/       # Hive Metastore + MariaDB backend
│   ├── kafka/                # Kafka broker and KRaft configuration
│   ├── debezium/             # Debezium CDC connector config
│   ├── schema-registry/      # Confluent Schema Registry
│   ├── trino/                # Trino query engine
│   ├── superset/             # Apache Superset BI
│   ├── jupyter-lab/          # JupyterLab for exploration
│   └── dbt/                  # dbt project (models, tests, macros)
│       └── models/
│           ├── staging/      # Bronze layer — raw CDC passthrough
│           ├── intermediate/ # Silver layer — deduplication & cleaning
│           └── mart/         # Gold layer — business dimensions & facts
├── docker-compose.yaml        # Service orchestration
├── Makefile                   # Convenience commands
└── .env                      # Environment variables
```

---

## 9. Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| Spark Master UI | http://localhost:8088 | — |
| JupyterLab | http://localhost:8888 | — |
| MinIO Console | http://localhost:9001 | minio / minio123 |
| Trino UI | http://localhost:8080 | — |
| Apache Superset | http://localhost:8089 | admin / admin123 |
| Schema Registry | http://localhost:8081 | — |
| Debezium REST API | http://localhost:8083 | — |
| Apache Airflow | http://localhost:8085 | admin / admin123 |
| Kafka | localhost:9092 | — |

---

## 10. Dashboards

![Executive Dashboard](./assets/dashboard/executive.png)
![Product Analytics](./assets/dashboard/products.png)
![Customer Insights](./assets/dashboard/customers.png)

---

## 11. Future Roadmap

### Infrastructure & Scalability

- **Cloud Migration:** Transition from local Docker to managed cloud services (AWS EMR/MSK/S3, GCP Dataproc/PubSub/GCS, or Azure Synapse/Spark).
- **Infrastructure as Code:** Terraform or Pulumi to provision cloud resources programmatically.
- **Multi-region Replication:** Kafka MirrorMaker for cross-region CDC event replication.

### Data Engineering

- **Real-time Serving:** Replace batch dbt with streaming aggregations (Spark Structured Streaming aggregations, Flink for complex event processing).
- **Unified Serving Layer:** Migrate from per-layer schema queries to Apache Iceberg for time-travel queries across layers.
- **Advanced Analytics:** Customer lifetime value modeling, product recommendation engine, anomaly detection for fraud signals.

### DevOps & Observability

- **CI/CD:** GitHub Actions for automated linting (Ruff/Black), dbt tests, Docker image building.
- **Data Contracts:** Great Expectations or dbt Data Tests for declarative data quality enforcement at each medallic layer.
- **Monitoring:** Prometheus + Grafana for Kafka lag, Spark job metrics, and Trino query performance. Slack alerting for Airflow DAG failures.
- **Data Catalog:** Apache Atlas or OpenMetadata for data lineage, column-level documentation, and discovery.
