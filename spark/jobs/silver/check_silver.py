"""Verificación de Silver: compuertas de calidad (el DAG se detiene si alguna falla) y un vistazo a los datos."""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver")
TABLAS = ["usuarios", "destinos", "procedencias", "tipos_egreso", "tipos_ingreso",
          "frecuencias", "ingresos", "egresos", "inversiones", "metas", "fechas_corte_ahorro"]

spark = (
    SparkSession.builder.appName("check-silver")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

FALLAS = []


def revisar(condicion, mensaje):
    print(("OK    " if condicion else "FALLA ") + mensaje)
    if not condicion:
        FALLAS.append(mensaje)


datos = {t: spark.read.parquet(f"{SILVER_PATH}/{t}") for t in TABLAS}

print("--- Filas por tabla ---")
total = 0
conteos = {}
for t, df in datos.items():
    conteos[t] = df.count()
    total += conteos[t]
    print(f"{t:<22} {conteos[t]:>6}")
print(f"{'TOTAL':<22} {total:>6}")

print("\n--- Compuertas de calidad ---")
for t, df in datos.items():
    n = conteos[t]
    con_delete = df.filter(F.col("_ultima_op") == "d").count()
    unicos = df.select("id").distinct().count()
    motivos = []
    if n == 0:
        motivos.append("está vacía")
    if n != unicos:
        motivos.append(f"{n - unicos} fila(s) con id repetido")
    if con_delete:
        motivos.append(f"{con_delete} fila(s) con DELETE como último evento")
    revisar(not motivos, f"{t}: {n} filas" + (f" -> {', '.join(motivos)}" if motivos else ", ids únicos, sin DELETE como último evento"))
for t in ("ingresos", "egresos"):
    revisar(datos[t].filter(F.col("monto").isNull()).count() == 0, f"{t}: ningún monto nulo")

egresos = datos["egresos"]
print("\n--- Tipos de egresos ---")
egresos.printSchema()

print("--- Último evento que dejó cada fila (egresos) ---")
egresos.groupBy("_ultima_op").count().show()

print("--- Filas con borrado lógico (eliminado_en lleno) ---")
print("egresos:", egresos.filter(F.col("eliminado_en").isNotNull()).count())

print("--- Muestra ---")
egresos.select("id", "monto", "descripcion", "fecha_inicio", "creado_en", "eliminado_en").orderBy("id").show(3, truncate=False)

print("--- usuarios: booleanos, fecha y json ---")
datos["usuarios"].select("id", "activo", "fecha_nacimiento", "scopes").show(3, truncate=40)

if FALLAS:
    print(f"\n{len(FALLAS)} verificación(es) fallaron:")
    for falla in FALLAS:
        print(" -", falla)
    sys.exit(1)
print("\nTodas las verificaciones pasaron.")