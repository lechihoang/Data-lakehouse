# Notebook — Streaming Ingestion

**File:** `workspace/stream_processor.ipynb`

JupyterLab notebook that reads from Kafka, parses JSON, writes to Delta Lake, and registers HMS metadata.

## Cell-by-Cell Walkthrough

### Cell 1 — Spark Initialization

Creates a Spark session with Delta Lake and Hive Metastore support.

### Cell 2 — Kafka Configuration

Defines broker address, topics, and starting offsets. Edit this cell to change which tables are streamed.

### Cell 3 — StructType Schemas

Defines typed schemas for each CDC table:

```python
event_schema = StructType([
    StructField("id",        StringType(),  True),
    StructField("user_id",   StringType(),  True),   # Nullable → ghost events
    StructField("session_id",StringType(),  True),
    StructField("event_type",StringType(),  True),
    StructField("uri",       StringType(),  True),
    StructField("created_at",StringType(),  True),
    StructField("city",      StringType(),  True),
    StructField("kafka_ts",  StringType(),  True),   # Watermark
    # ... more fields
])
```

> Schema `.avsc` files in `/schemas` (mounted from `infra/jupyter-lab/schemas/`) are for **reference only**. If changed, restart the Jupyter container.

### Cell 4 — Bootstrap HMS: Static Tables

```python
# 1. Create staging schema
trino_exec("CREATE SCHEMA IF NOT EXISTS staging")

# 2. Register products table
trino_exec("""
CREATE TABLE IF NOT EXISTS delta.staging.products (
    id INTEGER, name VARCHAR, category VARCHAR, ...
) WITH (location = 's3a://lakehouse/staging/products')
""")

# 3. Bootstrap data via JDBC
products_df = spark.read.jdbc(
    url=f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}",
    table="products",
    properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD}
)
products_df.write.format("delta").mode("overwrite").saveAsTable("staging.products")
```

Runs once at startup. Idempotent — safe to re-run.

### Cell 5 — Bootstrap HMS: CDC Tables

```python
trino_exec("""
CREATE TABLE IF NOT EXISTS delta.staging.orders (
    id VARCHAR, kafka_ts VARCHAR, status VARCHAR, ...
) WITH (location = 's3a://lakehouse/staging/orders')
""")
# Repeat for users, order_items, events
```

Registers HMS metadata **before** Spark writes to those tables. Without this, HMS has no metadata and Trino cannot query the tables.

### Cell 6 — Start Streaming

```python
events_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_EVENTS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
)

parsed = events_stream.select(
    F.from_json(F.col("value").cast("string"), event_schema).alias("data")
).select("data.*")

query = (
    parsed.writeStream
        .format("delta")
        .option("checkpointLocation", f"s3a://lakehouse/checkpoints/events")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .toTable("staging.events")
)
```

Repeat for each CDC table (orders, users, order_items).

## HMS Registration Sequence

```
Cell 4  → CREATE SCHEMA staging
         → CREATE TABLE + JDBC bootstrap: products, dist_centers

Cell 5  → CREATE TABLE: orders, users, order_items, events
         (HMS now knows location for all staging tables)

Cell 6  → Spark writes to staging.* (HMS already has metadata)

dbt run → Reads from staging.* via HMS
         → Creates intermediate.*, mart.* (auto-registers with HMS)
```

## Why Does the Notebook Register HMS?

Hive Metastore is a **metadata registry**, not a discovery engine. It does NOT scan MinIO for new Delta tables.

> Without HMS registration, Trino and dbt cannot see `staging.*` tables → query fails with "table not found".

This pattern is standard for Spark + HMS + Delta setups (Databricks Unity Catalog, AWS EMR, Cloudera).

## Avoiding Duplicates on Restart

| Setting | Behavior |
|---------|----------|
| `startingOffsets: "latest"` | Resumes from last committed offset → **no duplicates** |
| `startingOffsets: "earliest"` | Re-reads from beginning → **duplicates accumulate** |

> Always use `startingOffsets: "latest"` in production. If you must restart from earliest, truncate the staging table first:

```python
spark.sql("TRUNCATE TABLE delta.staging.events")
```

## JupyterLab Container

Built from `pyspark-notebook 3.5` with:
- Hive 4.1.0 client JAR (must match HMS server version — mismatch causes protocol errors)
- Delta 3.0
- Mounted `/schemas` from `infra/jupyter-lab/schemas/`
