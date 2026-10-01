"""Verificación de Silver: conteos, tipos y el caso del egreso de prueba (id 1749)."""
import os

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

print("--- Filas por tabla ---")
total = 0
for t in TABLAS:
    n = spark.read.parquet(f"{SILVER_PATH}/{t}").count()
    total += n
    print(f"{t:<22} {n:>6}")
print(f"{'TOTAL':<22} {total:>6}")

egresos = spark.read.parquet(f"{SILVER_PATH}/egresos")

print("\n--- Egreso de prueba (id 1749) ---")
n1749 = egresos.filter("id = 1749").count()
print("existe en Silver:", n1749 > 0, "(debe ser False: se borró en la prueba)")

print("\n--- Tipos de egresos ---")
egresos.printSchema()

print("--- Último evento que dejó cada fila (egresos) ---")
egresos.groupBy("_ultima_op").count().show()

print("--- Filas con borrado lógico (eliminado_en lleno) ---")
print("egresos:", egresos.filter(F.col("eliminado_en").isNotNull()).count())

print("--- Muestra ---")
egresos.select("id", "monto", "descripcion", "fecha_inicio", "creado_en", "eliminado_en").orderBy("id").show(3, truncate=False)

print("--- usuarios: booleanos, fecha y json ---")
spark.read.parquet(f"{SILVER_PATH}/usuarios").select("id", "activo", "fecha_nacimiento", "scopes").show(3, truncate=40)