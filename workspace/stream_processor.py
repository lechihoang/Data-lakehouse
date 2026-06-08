#!/usr/bin/env python
# coding: utf-8

# 1) Run cells 1 → 2 → 3 → 4
# 2) Run cell 5 to start streaming continuously
# 3) Press ■ to stop

# ## 1 — Health Checks

# In[ ]:


import socket
import time

def wait_for_tcp(host, port, label, timeout=120):
    print(f"Waiting for {label} ({host}:{port})...", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection((host, port), timeout=2).close()
            print(" OK")
            return
        except OSError:
            print(".", end="", flush=True)
            time.sleep(3)
    raise TimeoutError(f"{label} not ready after {timeout}s")

wait_for_tcp("postgres", 5432, "PostgreSQL")
wait_for_tcp("minio", 9000, "MinIO")
wait_for_tcp("hive-metastore", 9083, "Hive Metastore")
wait_for_tcp("spark-master", 7077, "Spark Master")
wait_for_tcp("kafka", 9092, "Kafka Broker")
wait_for_tcp("schema-registry", 8081, "Schema Registry")

print("\nAll required services ready.")


# ## 2 — Spark Session

# In[ ]:


import os
import time
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")

DELTA_BASE = "s3a://lakehouse/staging"
CHECKPOINT_BASE = "s3a://lakehouse/checkpoints"

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "thelook")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

active = SparkSession.getActiveSession()
if active is not None:
    active.stop()
    time.sleep(2)

spark = (
    SparkSession.builder
    .appName("TheLookStreaming")
    .master(SPARK_MASTER)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("Spark session ready.")


# ## 3 Schema Helpers

# In[ ]:


import json
import pyspark.sql.functions as F
import pyspark.sql.types as T

AVRO2SPARK = {
    "string": T.StringType(), "int": T.IntegerType(), "long": T.LongType(),
    "double": T.DoubleType(), "float": T.FloatType(), "boolean": T.BooleanType(),
}

SPARK2DELTA = {
    "string": "VARCHAR", "integer": "INTEGER", "long": "BIGINT",
    "double": "DOUBLE", "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP", "date": "DATE",
}

def load_local_schema(table):
    with open(f"/schemas/{table}.avsc", "r") as f:
        return json.load(f)

def schema_to_struct(avro_schema):
    fields = []
    for f in avro_schema.get("fields", []):
        ft = f["type"]
        nullable = isinstance(ft, list) and "null" in ft
        inner = [t for t in (ft if isinstance(ft, list) else [ft]) if t != "null"]
        st = AVRO2SPARK.get(inner[0] if inner else "string", T.StringType())
        fields.append(T.StructField(f["name"], st, nullable))
    return T.StructType(fields)

def create_table_ddl(name, schema, location, extra_cols=None):
    col_defs = [
        f"  {f.name} {SPARK2DELTA.get(f.dataType.simpleString().lower().split('(')[0], 'VARCHAR')}"
        for f in schema.fields
    ]
    if extra_cols:
        col_defs += extra_cols
    cols = ",\n".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS {name} (\n{cols}\n)\nWITH (\n  location = '{location}'\n)"

print("Helpers ready.")


# ## 4 — Bootstrap Static Tables

# In[ ]:


import trino

conn = trino.dbapi.connect(
    host=TRINO_HOST, port=8080,
    user="admin", catalog="delta", schema="staging",
)


def trino(sql):
    try:
        cur = conn.cursor(); cur.execute(sql)
        try: cur.fetchall()
        except trino.exceptions.TrinoDataError: pass
        return True
    except Exception as e:
        print(f"  WARN: {e}")
        return False


trino("CREATE SCHEMA IF NOT EXISTS staging")
print("[OK] CREATE SCHEMA staging")

for src in ("products", "dist_centers"):
    name = f"ref_{src}"
    path = f"{DELTA_BASE}/{name}"
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}")
        .option("dbtable", f"public.{src}").option("user", DB_USER)
        .option("password", DB_PASSWORD).option("driver", "org.postgresql.Driver")
        .load()
        .withColumn("operation", F.lit("r"))
        .withColumn("event_ts_ms", F.expr("cast(unix_timestamp() * 1000 as long)"))
    )
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print(f"  Bootstrapped {name}: {df.count()} rows")

    ddl = create_table_ddl(name, df.schema, path,
                           extra_cols=["operation VARCHAR", "event_ts_ms BIGINT"])
    ok = trino(ddl)
    print(f"  [{'OK' if ok else 'FAIL'}] CREATE TABLE staging.{name}")

print("Static ref tables ready.")


# ## 5 Start CDC Streams

# In[ ]:


spark.conf.set("spark.sql.shuffle.partitions", "4")

from pyspark.sql.functions import from_json

CDC_TOPICS = {
    "thelook.public.users":       "users",
    "thelook.public.orders":      "orders",
    "thelook.public.order_items": "order_items",
    "thelook.public.events":      "events",
}

avro_schemas = {t: load_local_schema(k) for t, k in CDC_TOPICS.items()}
table_schemas = {k: schema_to_struct(avro_schemas[t]) for t, k in CDC_TOPICS.items()}

DELTA_EXTRA = [
    T.StructField("kafka_ts", T.TimestampType()),
    T.StructField("op", T.StringType()),
    T.StructField("ts_ms", T.StringType()),
]

def delta_schema(table):
    return T.StructType(list(table_schemas[table].fields) + DELTA_EXTRA)

for table in CDC_TOPICS.values():
    path = f"{DELTA_BASE}/{table}"
    try:
        spark.read.format("delta").load(path)
    except Exception:
        empty = spark.createDataFrame([], delta_schema(table))
        (empty.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").save(path))
        print(f"  Created empty Delta table: {table}")

def upsert_to_delta(df, epoch_id):
    rows = df.select("_table").take(1)
    if not rows:
        return
    table = rows[0][0]

    ups = df.filter("op != 'd'")
    dels = df.filter("op = 'd'")

    if not ups.isEmpty():
        view_name = f"cdc_updates_{table}"
        ups.drop("_table").createOrReplaceGlobalTempView(view_name)
        spark.sql(f"""
            MERGE INTO delta.`{DELTA_BASE}/{table}` AS t
            USING (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts_ms DESC) AS rn
                FROM global_temp.{view_name}
            ) AS s ON s.id = t.id AND s.rn = 1
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    if not dels.isEmpty():
        ids = [r.id for r in dels.select("id").distinct().collect()]
        if ids:
            spark.sql(f"DELETE FROM delta.`{DELTA_BASE}/{table}` WHERE id IN ({','.join(repr(i) for i in ids)})")

queries = []
for topic, table in CDC_TOPICS.items():
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic).option("kafka.group.id", f"spark-streaming-{table}")
        .option("startingOffsets", "earliest").option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )

    # Debezium flat JSON format parsing
    # Value is a JSON string. We define the schema to extract fields, op, and ts_ms
    json_schema = T.StructType(list(table_schemas[table].fields) + [
        T.StructField("__op", T.StringType()),
        T.StructField("__ts_ms", T.LongType())
    ])

    envelope = raw.select(
        from_json(F.col("value").cast("string"), json_schema).alias("payload"),
        F.col("timestamp").alias("kafka_ts")
    )

    cols = [F.col(f"payload.{f.name}").alias(f.name) for f in table_schemas[table].fields]
    final = envelope.select(
        *cols, "kafka_ts",
        F.col("payload.__op").alias("op"),
        F.col("payload.__ts_ms").cast("string").alias("ts_ms"),
    ).filter("id IS NOT NULL").withColumn("_table", F.lit(table))

    q = (final.writeStream.foreachBatch(upsert_to_delta)
         .outputMode("update").trigger(processingTime="30 seconds")
         .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table}")
         .start())
    queries.append(q)
    print(f"Stream started: {topic} -> {table}")

print("\nRegistering CDC tables in HMS...")
for table in CDC_TOPICS.values():
    extra = ["kafka_ts TIMESTAMP", "op VARCHAR", "ts_ms VARCHAR"]
    ddl = create_table_ddl(table, table_schemas[table], f"{DELTA_BASE}/{table}", extra_cols=extra)
    ok = trino(ddl)
    print(f"  [{'OK' if ok else 'WARN'}] CREATE TABLE staging.{table}")

print(f"\nAll {len(queries)} streams running (30s micro-batch). Press \u25a0 to stop.")
spark.streams.awaitAnyTermination()

