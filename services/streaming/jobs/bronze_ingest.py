# services/streaming/jobs/bronze_ingest.py
"""
Job de Spark Structured Streaming — Capa Bronze.
Lee raw-play-events desde Kafka y escribe en HDFS como Parquet.
No aplica transformaciones: datos exactamente como llegan.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType
)

# ── Schema de los eventos que vienen de Kafka ────────────────
PLAY_EVENT_SCHEMA = StructType([
    StructField("event_id",           StringType(), True),
    StructField("event_type",         StringType(), True),
    StructField("user_id",            StringType(), True),
    StructField("track_id",           StringType(), True),
    StructField("played_at",          StringType(), True),
    StructField("duration_played_ms", LongType(),   True),
    StructField("source",             StringType(), True),
    StructField("device_type",        StringType(), True),
    StructField("country",            StringType(), True),
    StructField("latitude",           DoubleType(), True),
    StructField("longitude",          DoubleType(), True),
])

KAFKA_BROKERS = "kafka:29092"          # Interno entre containers
HDFS_BASE     = "hdfs://namenode:9000"
BRONZE_PATH   = f"{HDFS_BASE}/data/bronze/play_events"


def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SoundWave-Bronze-Ingest")
        # JARs necesarios para leer de Kafka
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        # Caché local de JARs para no descargar en cada ejecución
        .config("spark.jars.ivy", "/tmp/.ivy")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def main():
    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")

    print("✅ Sesión Spark iniciada")
    print(f"   Leyendo de: kafka → raw-play-events")
    print(f"   Escribiendo en: {BRONZE_PATH}")

    # ── Leer desde Kafka ─────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", "raw-play-events")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )

    # ── Deserializar JSON ────────────────────────────────────
    parsed = (
        raw
        .select(
            from_json(col("value").cast("string"), PLAY_EVENT_SCHEMA).alias("d"),
            col("timestamp").alias("kafka_ts"),
        )
        .select(
            "d.*",
            "kafka_ts",
            current_timestamp().alias("bronze_at"),
        )
    )

    # ── Escribir en HDFS (Parquet, particionado por país) ────
    query = (
        parsed.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints")
        .partitionBy("country")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("🚀 Bronze job corriendo. Procesando cada 30 segundos...")
    print("   Ctrl+C para detener\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()