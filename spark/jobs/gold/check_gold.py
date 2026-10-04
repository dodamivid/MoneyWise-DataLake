"""Verificación de Gold: coherencia con Silver, matices y muestra para cuadrar con la app."""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/home/jovyan/work/data/gold")

spark = (
    SparkSession.builder.appName("check-gold")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

balance = spark.read.parquet(f"{GOLD_PATH}/balance_mensual")
categorias = spark.read.parquet(f"{GOLD_PATH}/gasto_por_categoria_mensual")
ingresos = spark.read.parquet(f"{SILVER_PATH}/ingresos")
egresos = spark.read.parquet(f"{SILVER_PATH}/egresos")
ing_activos = ingresos.filter(F.col("eliminado_en").isNull())
egr_activos = egresos.filter(F.col("eliminado_en").isNull())


def suma(df, columna):
    return df.agg(F.sum(columna)).collect()[0][0] or 0


def revisar(condicion, mensaje):
    print(("OK    " if condicion else "FALLA ") + mensaje)


print("--- Filas ---")
print("balance_mensual:             ", balance.count())
print("gasto_por_categoria_mensual: ", categorias.count())

print("\n--- Coherencia Gold vs Silver ---")
revisar(suma(balance, "total_ingresos") == suma(ing_activos, "monto"),
        "total de ingresos en Gold = ingresos activos en Silver")
revisar(suma(balance, "total_egresos") == suma(egr_activos, "monto"),
        "total de egresos en Gold = egresos activos en Silver")
revisar(suma(categorias, "total_egresos") == suma(balance, "total_egresos"),
        "total por categoría = total de egresos del balance")
revisar(balance.filter(F.col("balance") != F.col("total_ingresos") - F.col("total_egresos")).count() == 0,
        "balance = ingresos - egresos en todas las filas")
ultimo = balance.groupBy("usuario_id").agg(F.max_by("balance_acumulado", "mes").alias("ultimo"),
                                           F.sum("balance").alias("suma"))
revisar(ultimo.filter(F.col("ultimo") != F.col("suma")).count() == 0,
        "el último balance_acumulado de cada usuario = suma de sus balances")
revisar(balance.count() == balance.select("usuario_id", "mes").distinct().count(),
        "balance_mensual sin filas repetidas por (usuario, mes)")
revisar(categorias.count() == categorias.select("usuario_id", "mes", "tipo_id").distinct().count(),
        "categorías sin filas repetidas por (usuario, mes, categoría)")

print("\n--- Matices (explican diferencias posibles con la app) ---")
for nombre, df in [("ingresos", ing_activos), ("egresos", egr_activos)]:
    abarcan = df.filter(F.col("fecha_fin").isNotNull()
                        & (F.trunc(F.col("fecha_fin").cast("date"), "month")
                           != F.trunc(F.col("fecha_inicio").cast("date"), "month"))).count()
    print(f"{nombre}: activos={df.count()}  con fecha_fin={df.filter(F.col('fecha_fin').isNotNull()).count()}  "
          f"con frecuencia={df.filter(F.col('frecuencia_id').isNotNull()).count()}  "
          f"abarcan varios meses={abarcan}")

print("\n--- Muestra: balance_mensual (últimos meses por usuario) ---")
balance.orderBy("usuario_id", F.col("mes").desc()).show(8, truncate=False)

print("--- Muestra: gasto por categoría con variación ---")
categorias.orderBy("usuario_id", F.col("mes").desc(), "categoria").show(8, truncate=False)