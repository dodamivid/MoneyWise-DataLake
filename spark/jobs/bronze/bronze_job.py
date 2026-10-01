"""Spark Job Bronze (Issue 5): ingesta cruda desde Kafka hacia Parquet.

Lee los topics de Debezium y guarda cada mensaje tal cual llegó, sin
interpretar el JSON. Cada corrida procesa lo nuevo y termina (availableNow).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_date

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_PATTERN = r"moneywise\.moneywise\..*"
BRONZE_PATH = os.getenv("BRONZE_PATH", "/home/jovyan/work/data/bronze")
CHECKPOINT_PATH = os.getenv("BRONZE_CHECKPOINT", "/home/jovyan/work/data/checkpoints/bronze")


def main():
    spark = SparkSession.builder.appName("moneywise-bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Leer de Kafka (streaming). startingOffsets solo aplica la primera vez:
    #    después Spark retoma desde el checkpoint.
    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Bronze = el mensaje tal cual + metadatos de Kafka. No se interpreta el JSON.
    bronze = crudo.select(
        regexp_extract(col("topic"), r"^moneywise\.moneywise\.(.+)$", 1).alias("tabla"),
        to_date(col("timestamp")).alias("fecha"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_ts"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
    )

    # 3. Escribir Parquet particionado por tabla y fecha; procesar lo pendiente y terminar.
    consulta = (
        bronze.writeStream.format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("tabla", "fecha")
        .trigger(availableNow=True)
        .start()
    )
    consulta.awaitTermination()
    print("Bronze: corrida terminada.")


if __name__ == "__main__":
    main()