"""Spark Job de anomalías (Issue 8): gastos inusualmente altos, con el método IQR.

Para cada usuario y destino (la categoría del gasto) se calcula qué es "normal":
  Q1 y Q3 = cuartiles de los montos        IQR = Q3 - Q1
  atípico: monto > Q3 + 1.5 x IQR          extremo: monto > Q3 + 3 x IQR

Reglas:
  - Solo egresos activos (eliminado_en vacío) y solo montos altos.
  - Un grupo con menos de MIN_MOVIMIENTOS movimientos, o con IQR = 0, no se evalúa.
  - Un egreso sin destino se agrupa como "Sin destino" (id 0), igual que en Gold.
  - Sin datos personales: no se guarda la descripción.
Escribe data/gold/anomalias. Se reconstruye completo en cada corrida (overwrite).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/home/jovyan/work/data/gold")
MIN_MOVIMIENTOS = int(os.getenv("ANOM_MIN_MOVIMIENTOS", "8"))
K_ATIPICO = float(os.getenv("ANOM_K_ATIPICO", "1.5"))
K_EXTREMO = float(os.getenv("ANOM_K_EXTREMO", "3.0"))
MONEDA = "decimal(18,2)"


def egresos_activos(spark):
    egresos = spark.read.parquet(f"{SILVER_PATH}/egresos")
    return (
        egresos.filter(F.col("eliminado_en").isNull())
        .withColumn("destino_id", F.coalesce("destino_id", F.lit(0)))  # sin destino -> 0, como en Gold
        .withColumn("monto_num", F.col("monto").cast("double"))        # los cuartiles se calculan en double
    )


def estadisticas_por_grupo(egresos):
    """Qué es 'normal' para cada (usuario, destino): cuartiles e IQR."""
    return (
        egresos.groupBy("usuario_id", "destino_id")
        .agg(
            F.count("*").alias("n_grupo"),
            F.percentile("monto_num", 0.25).alias("q1"),
            F.percentile("monto_num", 0.75).alias("q3"),
        )
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn("limite_atipico", F.col("q3") + K_ATIPICO * F.col("iqr"))
        .withColumn("limite_extremo", F.col("q3") + K_EXTREMO * F.col("iqr"))
    )


def grupos_evaluables(estadisticas):
    return estadisticas.filter((F.col("n_grupo") >= MIN_MOVIMIENTOS) & (F.col("iqr") > 0))


def detectar(egresos, evaluables, destinos):
    nombres = destinos.select(F.col("id").alias("destino_id"), F.col("nombre").alias("_nombre"))

    def dinero(columna):
        return F.round(F.col(columna), 2).cast(MONEDA)

    return (
        egresos.join(evaluables, ["usuario_id", "destino_id"], "inner")
        .filter(F.col("monto_num") > F.col("limite_atipico"))  # solo montos altos
        .join(nombres, "destino_id", "left")
        .withColumn("destino", F.coalesce("_nombre", F.lit("Sin destino")))
        .withColumn(
            "severidad",
            F.when(F.col("monto_num") > F.col("limite_extremo"), "extremo").otherwise("atipico"),
        )
        .select(
            F.col("id").alias("egreso_id"),
            "usuario_id", "destino_id", "destino",
            F.col("fecha_inicio").cast("date").alias("fecha"),
            "monto", "n_grupo",
            dinero("q1").alias("q1"), dinero("q3").alias("q3"), dinero("iqr").alias("iqr"),
            dinero("limite_atipico").alias("limite_atipico"),
            dinero("limite_extremo").alias("limite_extremo"),
            (F.col("monto_num") - F.col("limite_atipico")).cast(MONEDA).alias("exceso_sobre_limite"),
            "severidad",
        )
        .orderBy(F.col("exceso_sobre_limite").desc())
    )


def main():
    spark = (
        SparkSession.builder.appName("moneywise-anomalias")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    egresos = egresos_activos(spark)
    destinos = spark.read.parquet(f"{SILVER_PATH}/destinos")
    estadisticas = estadisticas_por_grupo(egresos)
    evaluables = grupos_evaluables(estadisticas)

    anomalias = detectar(egresos, evaluables, destinos)
    destino = f"{GOLD_PATH}/anomalias"
    anomalias.coalesce(1).write.mode("overwrite").parquet(destino)
    resultado = spark.read.parquet(destino)

    total_grupos = estadisticas.count()
    n_evaluables = evaluables.count()
    pocos = estadisticas.filter(F.col("n_grupo") < MIN_MOVIMIENTOS).count()
    print("\n--- Anomalías ---")
    print(f"egresos activos revisados:        {egresos.count()}")
    print(f"grupos (usuario, destino):        {total_grupos}")
    print(f"  evaluados:                      {n_evaluables}")
    print(f"  omitidos por < {MIN_MOVIMIENTOS} movimientos:     {pocos}")
    print(f"  omitidos por IQR = 0:           {total_grupos - n_evaluables - pocos}")
    for severidad, n in sorted((r["severidad"], r["count"]) for r in resultado.groupBy("severidad").count().collect()):
        print(f"anomalías {severidad:<8}:             {n}")
    print(f"anomalías en total:               {resultado.count()}")
    print("Anomalías: corrida terminada.")


if __name__ == "__main__":
    main()