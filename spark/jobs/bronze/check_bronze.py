from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("check-bronze").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("/home/jovyan/work/data/bronze")

print("--- Filas por tabla ---")
df.groupBy("tabla").count().orderBy("tabla").show()
print("TOTAL:", df.count())

print("--- Particiones por fecha de ingesta ---")
df.groupBy("fecha").count().orderBy("fecha").show()

print("--- Mensajes vacíos (tombstones de Debezium) ---")
print(df.filter("value IS NULL").count())

print("--- Cómo luce un registro crudo ---")
df.filter("tabla = 'egresos'").select("offset", "fecha", "value").show(2, truncate=100)