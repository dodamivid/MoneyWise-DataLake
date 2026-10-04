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
por_destino = spark.read.parquet(f"{GOLD_PATH}/gasto_por_destino_mensual")
por_tipo = spark.read.parquet(f"{GOLD_PATH}/gasto_por_tipo_mensual")
ingresos = spark.read.parquet(f"{SILVER_PATH}/ingresos")
egresos = spark.read.parquet(f"{SILVER_PATH}/egresos")
ing_activos = ingresos.filter(F.col("eliminado_en").isNull())
egr_activos = egresos.filter(F.col("eliminado_en").isNull())


def suma(df, columna):
    return df.agg(F.sum(columna)).collect()[0][0] or 0


def revisar(condicion, mensaje):
    print(("OK    " if condicion else "FALLA ") + mensaje)


def sin_repetidos(df, llaves):
    return df.count() == df.select(*llaves).distinct().count()


print("--- Filas ---")
print("balance_mensual:           ", balance.count())
print("gasto_por_destino_mensual: ", por_destino.count())
print("gasto_por_tipo_mensual:    ", por_tipo.count())

print("\n--- Coherencia Gold vs Silver ---")
revisar(suma(balance, "total_ingresos") == suma(ing_activos, "monto"),
        "total de ingresos en Gold = ingresos activos en Silver")
revisar(suma(balance, "total_egresos") == suma(egr_activos, "monto"),
        "total de egresos en Gold = egresos activos en Silver")
revisar(suma(por_destino, "total_egresos") == suma(balance, "total_egresos"),
        "total por destino = total de egresos del balance")
revisar(suma(por_tipo, "total_egresos") == suma(balance, "total_egresos"),
        "total por tipo = total de egresos del balance")
revisar(balance.filter(F.col("balance") != F.col("total_ingresos") - F.col("total_egresos")).count() == 0,
        "balance = ingresos - egresos en todas las filas")
ultimo = balance.groupBy("usuario_id").agg(F.max_by("balance_acumulado", "mes").alias("ultimo"),
                                           F.sum("balance").alias("suma"))
revisar(ultimo.filter(F.col("ultimo") != F.col("suma")).count() == 0,
        "el último balance_acumulado de cada usuario = suma de sus balances")
revisar(sin_repetidos(balance, ["usuario_id", "mes"]),
        "balance_mensual sin filas repetidas por (usuario, mes)")
revisar(sin_repetidos(por_destino, ["usuario_id", "mes", "destino_id"]),
        "gasto por destino sin filas repetidas por (usuario, mes, destino)")
revisar(sin_repetidos(por_tipo, ["usuario_id", "mes", "tipo_id"]),
        "gasto por tipo sin filas repetidas por (usuario, mes, tipo)")

print("\n--- Matices (explican diferencias posibles con la app) ---")
for nombre, df in [("ingresos", ing_activos), ("egresos", egr_activos)]:
    abarcan = df.filter(F.col("fecha_fin").isNotNull()
                        & (F.trunc(F.col("fecha_fin").cast("date"), "month")
                           != F.trunc(F.col("fecha_inicio").cast("date"), "month"))).count()
    print(f"{nombre}: activos={df.count()}  con fecha_fin={df.filter(F.col('fecha_fin').isNotNull()).count()}  "
          f"con frecuencia={df.filter(F.col('frecuencia_id').isNotNull()).count()}  "
          f"abarcan varios meses={abarcan}")
print("egresos activos sin destino:", egr_activos.filter(F.col("destino_id").isNull()).count())

print("\n--- Muestra: balance_mensual (últimos meses por usuario) ---")
balance.orderBy("usuario_id", F.col("mes").desc()).show(6, truncate=False)

print("--- Muestra: gasto por DESTINO (categoría) con variación ---")
por_destino.orderBy("usuario_id", F.col("mes").desc(), "destino").show(8, truncate=False)

print("--- Muestra: gasto por TIPO (método de pago) con variación ---")
por_tipo.orderBy("usuario_id", F.col("mes").desc(), "tipo").show(5, truncate=False)

# Referencia para cuadrar con la app: CALL sp_dashboard_resumen(<usuario>, '<mes>-01 00:00:00', '<fin de mes> 23:59:59')
usuario_ref = int(os.getenv("CHECK_USUARIO", "3"))
mes_ref = os.getenv("CHECK_MES", "2026-08-01")
print(f"--- Referencia: gasto por destino, usuario {usuario_ref}, mes {mes_ref} (compara con tu app) ---")
(por_destino.filter((F.col("usuario_id") == usuario_ref) & (F.col("mes") == F.lit(mes_ref).cast("date")))
    .select("destino", "total_egresos").orderBy(F.col("total_egresos").desc()).show(truncate=False))