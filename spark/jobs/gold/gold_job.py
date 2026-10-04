"""Spark Job Gold (Issue 7): resúmenes de negocio para BI, a partir de Silver.

Tablas que produce (en data/gold/):
  balance_mensual            por usuario y mes: ingresos, egresos, balance y balance acumulado
  gasto_por_destino_mensual  por usuario, mes y destino (la categoría del gasto: Renta, Alimentación...)
  gasto_por_tipo_mensual     por usuario, mes y tipo de egreso (el método de pago: Efectivo, Tarjeta...)
Las dos de gasto traen el total y la variación contra el mes calendario anterior.

Reglas (las mismas que usa la app en sp_dashboard_*):
  - Solo movimientos activos (eliminado_en vacío).
  - Cada movimiento cuenta una vez, sin repartir los recurrentes.
  - El mes sale de fecha_inicio.
  - Un egreso sin destino se muestra como "Sin destino" (id 0), igual que la app.
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


def gasto_mensual(egresos, catalogo, col_id, col_nombre, sin_nombre):
    """Gasto por usuario, mes y una dimensión (destino o tipo), con variación vs el mes anterior.

    col_id:     columna de egresos que apunta al catálogo (destino_id o tipo_id)
    col_nombre: cómo se llamará la columna con el nombre (destino o tipo)
    sin_nombre: texto cuando no hay valor o no existe en el catálogo
    """
    nombres = catalogo.select(F.col("id").alias(col_id), F.col("nombre").alias("_nombre"))
    mensual = (
        # Sin valor -> 0 (como la app). Así la unión con el mes anterior también funciona para
        # ese grupo: en un join, NULL nunca es igual a NULL.
        egresos.withColumn(col_id, F.coalesce(F.col(col_id), F.lit(0)))
        .join(nombres, col_id, "left")
        .withColumn(col_nombre, F.coalesce("_nombre", F.lit(sin_nombre)))
        .groupBy("usuario_id", "mes", col_id, col_nombre)
        .agg(F.sum("monto").cast(MONEDA).alias("total_egresos"), F.count("*").alias("n_egresos"))
    )

    # Mes anterior (calendario): cada fila se desplaza un mes hacia adelante y se une con la original.
    anterior = mensual.select(
        "usuario_id", col_id,
        F.add_months("mes", 1).alias("mes"),
        F.col("total_egresos").alias("total_mes_anterior"),
    )
    return (
        mensual.join(anterior, ["usuario_id", col_id, "mes"], "left")
        .withColumn(
            "variacion_pct",
            F.round(F.try_divide((F.col("total_egresos") - F.col("total_mes_anterior")) * 100,
                                 F.col("total_mes_anterior")), 2),
        )
        .select("usuario_id", "mes", col_id, col_nombre, "total_egresos", "n_egresos",
                "total_mes_anterior", "variacion_pct")
        .orderBy("usuario_id", "mes", col_nombre)
    )


def escribir(df, tabla):
    destino = f"{GOLD_PATH}/{tabla}"
    df.coalesce(1).write.mode("overwrite").parquet(destino)  # tablas chicas: un solo archivo
    return SparkSession.getActiveSession().read.parquet(destino).count()


def main():
    spark = (
        SparkSession.builder.appName("moneywise-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ingresos = movimientos_activos(leer(spark, "ingresos"))
    egresos = movimientos_activos(leer(spark, "egresos"))

    tablas = {
        "balance_mensual": balance_mensual(ingresos, egresos),
        "gasto_por_destino_mensual": gasto_mensual(egresos, leer(spark, "destinos"), "destino_id", "destino", "Sin destino"),
        "gasto_por_tipo_mensual": gasto_mensual(egresos, leer(spark, "tipos_egreso"), "tipo_id", "tipo", "Sin tipo"),
    }

    print("\n--- Gold ---")
    for nombre, df in tablas.items():
        print(f"{nombre:<28} {escribir(df, nombre):>6} filas")
    print("Gold: corrida terminada.")


if __name__ == "__main__":
    main()