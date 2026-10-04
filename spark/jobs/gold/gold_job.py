"""Spark Job Gold (Issue 7): resúmenes de negocio para BI, a partir de Silver.

Tablas que produce (en data/gold/):
  balance_mensual              una fila por usuario y mes: ingresos, egresos, balance y balance acumulado
  gasto_por_categoria_mensual  una fila por usuario, mes y categoría: total y variación vs el mes anterior

Reglas (las mismas que usa la app en sp_dashboard_*):
  - Solo movimientos activos (eliminado_en vacío).
  - Cada movimiento cuenta una vez, sin repartir los recurrentes.
  - El mes sale de fecha_inicio.
Sin datos personales: solo usuario_id. Se reconstruye completo en cada corrida (overwrite).
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SILVER_PATH = os.getenv("SILVER_PATH", "/home/jovyan/work/data/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/home/jovyan/work/data/gold")
MONEDA = "decimal(18,2)"


def leer(spark, tabla):
    return spark.read.parquet(f"{SILVER_PATH}/{tabla}")


def movimientos_activos(df):
    """Solo los no eliminados, con su mes (primer día del mes de fecha_inicio)."""
    return df.filter(F.col("eliminado_en").isNull()).withColumn(
        "mes", F.trunc(F.col("fecha_inicio").cast("date"), "month")
    )


def balance_mensual(ingresos, egresos):
    ing = ingresos.groupBy("usuario_id", "mes").agg(
        F.sum("monto").alias("total_ingresos"), F.count("*").alias("n_ingresos")
    )
    egr = egresos.groupBy("usuario_id", "mes").agg(
        F.sum("monto").alias("total_egresos"), F.count("*").alias("n_egresos")
    )

    # full_outer: un mes puede tener solo ingresos o solo egresos; lo que falta cuenta como 0.
    base = (
        ing.join(egr, ["usuario_id", "mes"], "full_outer")
        .withColumn("total_ingresos", F.coalesce("total_ingresos", F.lit(0)).cast(MONEDA))
        .withColumn("total_egresos", F.coalesce("total_egresos", F.lit(0)).cast(MONEDA))
        .withColumn("n_ingresos", F.coalesce("n_ingresos", F.lit(0)))
        .withColumn("n_egresos", F.coalesce("n_egresos", F.lit(0)))
        .withColumn("balance", (F.col("total_ingresos") - F.col("total_egresos")).cast(MONEDA))
    )

    # Balance corrido: suma del balance de todos los meses hasta este, por usuario.
    corrido = Window.partitionBy("usuario_id").orderBy("mes").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    return base.withColumn("balance_acumulado", F.sum("balance").over(corrido).cast(MONEDA)).select(
        "usuario_id", "mes", "total_ingresos", "total_egresos", "balance",
        "n_ingresos", "n_egresos", "balance_acumulado",
    ).orderBy("usuario_id", "mes")


def gasto_por_categoria(egresos, tipos_egreso):
    categorias = tipos_egreso.select(
        F.col("id").alias("tipo_id"), F.col("nombre").alias("categoria_nombre")
    )
    mensual = (
        egresos.join(categorias, "tipo_id", "left")
        .withColumn("categoria", F.coalesce("categoria_nombre", F.lit("Sin tipo")))  # igual que la app
        .groupBy("usuario_id", "mes", "tipo_id", "categoria")
        .agg(F.sum("monto").cast(MONEDA).alias("total_egresos"), F.count("*").alias("n_egresos"))
    )

    # Mes anterior (calendario): cada fila se desplaza un mes hacia adelante y se une con la original.
    anterior = mensual.select(
        "usuario_id", "tipo_id",
        F.add_months("mes", 1).alias("mes"),
        F.col("total_egresos").alias("total_mes_anterior"),
    )
    return (
        mensual.join(anterior, ["usuario_id", "tipo_id", "mes"], "left")
        .withColumn(
            "variacion_pct",
            F.round(F.try_divide((F.col("total_egresos") - F.col("total_mes_anterior")) * 100,
                                 F.col("total_mes_anterior")), 2),
        )
        .select("usuario_id", "mes", "tipo_id", "categoria", "total_egresos", "n_egresos",
                "total_mes_anterior", "variacion_pct")
        .orderBy("usuario_id", "mes", "categoria")
    )


def escribir(df, tabla):
    destino = f"{GOLD_PATH}/{tabla}"
    df.coalesce(1).write.mode("overwrite").parquet(destino)  # tablas chicas: un solo archivo
    return spark_filas(destino)


def spark_filas(ruta):
    return SparkSession.getActiveSession().read.parquet(ruta).count()


def main():
    spark = (
        SparkSession.builder.appName("moneywise-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ingresos = movimientos_activos(leer(spark, "ingresos"))
    egresos = movimientos_activos(leer(spark, "egresos"))
    tipos_egreso = leer(spark, "tipos_egreso")

    print("\n--- Gold ---")
    print(f"{'balance_mensual':<30} {escribir(balance_mensual(ingresos, egresos), 'balance_mensual'):>6} filas")
    print(f"{'gasto_por_categoria_mensual':<30} "
          f"{escribir(gasto_por_categoria(egresos, tipos_egreso), 'gasto_por_categoria_mensual'):>6} filas")
    print("Gold: corrida terminada.")


if __name__ == "__main__":
    main()