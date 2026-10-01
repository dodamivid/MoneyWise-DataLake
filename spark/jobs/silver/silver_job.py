"""Spark Job Silver (Issue 6): de eventos crudos (Bronze) a tablas limpias.

Por cada tabla:
  1. Lee los eventos de Bronze y descarta los mensajes vacíos (tombstones).
  2. Abre el JSON de Debezium y saca op / before / after.
  3. Se queda con el ÚLTIMO evento de cada fila (por offset de Kafka).
  4. Quita las filas cuyo último evento fue un DELETE.
  5. Convierte los tipos (según common/schemas.py) y escribe Parquet.

Se reconstruye completo en cada corrida (overwrite): mismo Bronze, mismo Silver.
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Para poder hacer "from common.schemas import ..." al correr con spark-submit.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common.schemas import PK, TABLAS  # noqa: E402

BRONZE_PATH = os.getenv("BRONZE_PATH", "/home/jovyan/work/data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver")


def schema_evento(columnas):
    """Schema del evento Debezium. Todo se lee como texto; se convierte después."""
    fila = StructType([StructField(c, StringType()) for c in columnas])
    payload = StructType([
        StructField("op", StringType()),
        StructField("before", fila),
        StructField("after", fila),
    ])
    return StructType([StructField("payload", payload)])


def convertir(nombre, tipo):
    """Convierte una columna (texto crudo del evento) a su tipo final."""
    c = F.col(f"despues.{nombre}")
    if tipo == "string":
        return c
    if tipo == "boolean":
        # Según el conector llega como true/false o como 0/1.
        return F.lower(c).isin("true", "1")
    if tipo == "timestamp":
        # Debezium manda DATETIME como milisegundos desde 1970, sin zona horaria.
        return F.timestamp_millis(c.cast("long"))
    if tipo == "date":
        # DATE llega como días desde 1970 (o como texto ISO, por si acaso).
        return F.when(c.rlike(r"^-?\d+$"), F.date_from_unix_date(c.cast("int"))).otherwise(F.to_date(c))
    return c.cast(tipo)  # int, bigint, decimal(p,s)


def construir_silver(bronze, tabla, columnas):
    eventos = (
        bronze.filter((F.col("tabla") == tabla) & F.col("value").isNotNull())
        .select(
            F.col("offset"),
            F.from_json(F.col("value"), schema_evento(columnas.keys())).alias("e"),
        )
        .select(
            "offset",
            F.col("e.payload.op").alias("op"),
            F.col("e.payload.before").alias("antes"),
            F.col("e.payload.after").alias("despues"),
        )
        # La llave sale de "after"; en un DELETE "after" viene vacío y se usa "before".
        .withColumn("id_fila", F.coalesce(F.col(f"despues.{PK}"), F.col(f"antes.{PK}")).cast("long"))
    )

    # El evento con el offset más alto de cada fila es el estado vigente.
    ventana = Window.partitionBy("id_fila").orderBy(F.col("offset").desc())
    vigente = (
        eventos.withColumn("n", F.row_number().over(ventana))
        .filter("n = 1")
        .filter(F.col("op") != "d")  # la fila ya no existe en la fuente
    )

    return vigente.select(
        *[convertir(nombre, tipo).alias(nombre) for nombre, tipo in columnas.items()],
        F.col("offset").alias("_ultimo_offset"),
        F.col("op").alias("_ultima_op"),
    )


def main():
    spark = (
        SparkSession.builder.appName("moneywise-silver")
        .config("spark.sql.session.timeZone", "UTC")  # las fechas se interpretan como UTC
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.read.parquet(BRONZE_PATH)

    print("\n--- Silver ---")
    for tabla, columnas in TABLAS.items():
        destino = f"{SILVER_PATH}/{tabla}"
        construir_silver(bronze, tabla, columnas).write.mode("overwrite").parquet(destino)
        filas = spark.read.parquet(destino).count()
        print(f"{tabla:<22} {filas:>6} filas")
    print("Silver: corrida terminada.")


if __name__ == "__main__":
    main()