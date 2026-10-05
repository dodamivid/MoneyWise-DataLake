"""Verificación de anomalías: reglas internas y recálculo independiente en Python puro."""
import os
from collections import defaultdict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/home/jovyan/work/data/gold")
MIN_MOVIMIENTOS = int(os.getenv("ANOM_MIN_MOVIMIENTOS", "8"))
K_ATIPICO = float(os.getenv("ANOM_K_ATIPICO", "1.5"))
K_EXTREMO = float(os.getenv("ANOM_K_EXTREMO", "3.0"))

spark = (
    SparkSession.builder.appName("check-anomalias")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

anomalias = spark.read.parquet(f"{GOLD_PATH}/anomalias")
egresos = spark.read.parquet(f"{SILVER_PATH}/egresos")


def revisar(condicion, mensaje):
    print(("OK    " if condicion else "FALLA ") + mensaje)


print("--- Resumen ---")
print("anomalías:", anomalias.count())
anomalias.groupBy("severidad").count().show()
print("--- Dónde se concentran (usuario, destino) ---")
anomalias.groupBy("usuario_id", "destino").count().orderBy(F.col("count").desc()).show(8, truncate=False)

print("--- Reglas internas ---")
activos = egresos.filter(F.col("eliminado_en").isNull()).select(F.col("id").alias("egreso_id"))
revisar(anomalias.count() == anomalias.select("egreso_id").distinct().count(), "ningún egreso aparece dos veces")
revisar(anomalias.join(activos, "egreso_id", "left_anti").count() == 0, "todas son egresos activos (ninguna eliminada)")
revisar(anomalias.filter(F.col("n_grupo") < MIN_MOVIMIENTOS).count() == 0, f"todas vienen de grupos con {MIN_MOVIMIENTOS} o más movimientos")
revisar(anomalias.filter(F.col("iqr") <= 0).count() == 0, "ninguna viene de un grupo con IQR = 0")
revisar(anomalias.filter(F.col("monto") < F.col("limite_atipico")).count() == 0, "todas superan el límite de atípico")
revisar(anomalias.filter((F.col("severidad") == "extremo") & (F.col("monto") < F.col("limite_extremo"))).count() == 0,
        "las 'extremo' superan el límite de extremo")
revisar(anomalias.filter((F.col("severidad") == "atipico") & (F.col("monto") > F.col("limite_extremo"))).count() == 0,
        "las 'atipico' no superan el límite de extremo")
revisar("descripcion" not in anomalias.columns, "no se guarda la descripción (sin datos personales)")


# --- Recálculo independiente: sin Spark, solo Python, para comparar contra el job ---
def percentil(ordenados, p):
    """Percentil con interpolación lineal entre los dos valores vecinos."""
    posicion = (len(ordenados) - 1) * p
    abajo = int(posicion)
    arriba = min(abajo + 1, len(ordenados) - 1)
    return ordenados[abajo] + (ordenados[arriba] - ordenados[abajo]) * (posicion - abajo)


grupos = defaultdict(list)
for r in egresos.filter(F.col("eliminado_en").isNull()).select("id", "usuario_id", "destino_id", "monto").collect():
    grupos[(r["usuario_id"], r["destino_id"] or 0)].append((r["id"], float(r["monto"])))

esperadas = {}
for movimientos in grupos.values():
    montos = sorted(m for _, m in movimientos)
    if len(montos) < MIN_MOVIMIENTOS:
        continue
    q1, q3 = percentil(montos, 0.25), percentil(montos, 0.75)
    iqr = q3 - q1
    if iqr <= 0:
        continue
    for id_, monto in movimientos:
        if monto > q3 + K_ATIPICO * iqr:
            esperadas[id_] = "extremo" if monto > q3 + K_EXTREMO * iqr else "atipico"

obtenidas = {r["egreso_id"]: r["severidad"] for r in anomalias.select("egreso_id", "severidad").collect()}
print("\n--- Recálculo independiente (Python puro, sin Spark) ---")
revisar(esperadas == obtenidas, f"mismas anomalías y mismas severidades ({len(esperadas)} esperadas, {len(obtenidas)} del job)")
if esperadas != obtenidas:
    print("  solo en el recálculo:", sorted(set(esperadas) - set(obtenidas))[:10])
    print("  solo en el job:      ", sorted(set(obtenidas) - set(esperadas))[:10])

print("\n--- Las más grandes (para revisar a mano en Railway) ---")
(anomalias.select("egreso_id", "usuario_id", "destino", "fecha", "monto", "limite_atipico", "severidad")
    .orderBy(F.col("exceso_sobre_limite").desc()).show(10, truncate=False))