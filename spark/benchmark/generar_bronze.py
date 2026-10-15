"""Genera un Bronze sintético de N filas para el benchmark.

Escribe eventos con la misma forma que los de Debezium (op / before / after) y las mismas columnas que
bronze_job.py (tabla, fecha, topic, partition, offset, kafka_ts, key, value), en una carpeta APARTE.
Así silver_job.py y gold_job.py corren sin cambios, y el lago real no se toca.

No usa Kafka, ni Debezium, ni MySQL, ni Railway.

Uso (dentro del contenedor de Spark):
  spark-submit generar_bronze.py --filas 1000000 --salida /home/jovyan/work/data/benchmark/1000000/bronze
"""
import argparse
import sys
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
import parametros as P  # noqa: E402



def ms(col):
    """Timestamp -> milisegundos desde 1970 (como los manda Debezium para DATETIME)."""
    return F.unix_millis(col)


def nulo(estructura):
    """Un NULL con el mismo tipo que `estructura` (Debezium manda "before": null en un INSERT)."""
    return F.when(F.lit(False), estructura)


def evento(df, tabla, op, k, antes, despues, ts):
    """Convierte las filas de `df` en filas de Bronze con un evento de tipo `op`.

    k:  0 creación, 1 actualización, 2 borrado. El offset es id*3+k, así que para una misma fila
        el evento posterior siempre tiene el offset más alto (lo que usa Silver para elegir el vigente).
    """
    payload = F.struct(F.lit(op).alias("op"), antes.alias("before"), despues.alias("after"))
    valor = F.to_json(F.struct(payload.alias("payload")), {"ignoreNullFields": "false"})
    return df.select(
        F.lit(tabla).alias("tabla"),
        F.to_date(ts).alias("fecha"),
        F.lit(f"moneywise.moneywise.{tabla}").alias("topic"),
        (F.col("id") % 3).cast("int").alias("partition"),
        (F.col("id") * 3 + k).alias("offset"),
        ts.alias("kafka_ts"),
        F.col("id").cast("string").alias("key"),
        valor.alias("value"),
    )


def movimientos(spark, tabla, filas, usuarios):
    """Eventos de ingresos o egresos: creación de todas las filas + actualizaciones + borrados."""
    cfg = P.MOVIMIENTOS[tabla]
    n = P.filas_de(tabla, filas)
    i = F.col("id")
    dia = F.expr("INTERVAL 1 DAY")

    inicio = F.expr(
        f"date_add(add_months(DATE'{P.INICIO}', cast(id % {P.MESES} as int)), cast(id % 28 as int))"
    ).cast("timestamp")
    base = spark.range(n).select(
        "id", inicio.alias("ts"), (F.lit(cfg["base"]) + (i * cfg["mult"]) % cfg["mod"]).alias("centavos")
    )

    if tabla == "egresos":
        destino = F.when(i % P.SIN_DESTINO_MOD == 0, F.lit(None).cast("int")).otherwise(
            (1 + (i * 7) % P.N_DESTINOS).cast("int")
        )
        propias = [
            (1 + i % P.N_TIPOS_EGRESO).cast("int").alias("tipo_id"),
            destino.alias("destino_id"),
        ]
    else:
        propias = [
            (1 + i % P.N_TIPOS_INGRESO).cast("int").alias("tipo_id"),
            (1 + i % P.N_PROCEDENCIAS).cast("int").alias("procedencia_id"),
        ]

    def fila(extra_centavos=0, actualizada=False, eliminada=False):
        centavos = (F.col("centavos") + extra_centavos).cast("decimal(14,2)")
        monto = (centavos / 100).cast("decimal(12,2)").cast("string")
        actualizado = F.col("ts") + dia if actualizada else F.col("ts")
        eliminado = ms(F.col("ts") + dia) if eliminada else F.lit(None).cast("long")
        return F.struct(
            i.alias("id"),
            (1 + i % usuarios).cast("int").alias("usuario_id"),
            *propias,
            (1 + i % P.N_FRECUENCIAS).cast("int").alias("frecuencia_id"),
            monto.alias("monto"),
            F.concat(F.lit(f"{tabla} sintetico "), i.cast("string")).alias("descripcion"),
            ms(F.col("ts")).alias("fecha_inicio"),
            F.lit(None).cast("long").alias("fecha_fin"),
            ms(F.col("ts")).alias("creado_en"),
            ms(actualizado).alias("actualizado_en"),
            eliminado.alias("eliminado_en"),
        )

    original = fila()
    ts_despues = F.col("ts") + dia
    partes = [
        evento(base, tabla, "c", 0, nulo(original), original, F.col("ts")),
        evento(
            base.filter(i % P.UPD_MOD == P.UPD_RES), tabla, "u", 1,
            original, fila(P.UPD_CENTAVOS, actualizada=True), ts_despues,
        ),
        evento(
            base.filter(i % P.ELIM_MOD == P.ELIM_RES), tabla, "u", 1,
            original, fila(actualizada=True, eliminada=True), ts_despues,
        ),
        evento(
            base.filter(i % P.DEL_MOD == P.DEL_RES), tabla, "d", 2,
            original, nulo(original), F.col("ts") + F.expr("INTERVAL 2 DAY"),
        ),
    ]
    return reduce(DataFrame.unionByName, partes)


def catalogo(spark, tabla, n):
    """Tabla chica (destinos, tipos, frecuencias...): solo eventos de creación."""
    base = spark.range(1, n + 1).select("id", F.lit(f"{P.INICIO} 00:00:00").cast("timestamp").alias("ts"))
    i = F.col("id")
    ahora = ms(F.col("ts"))
    fila = F.struct(
        i.alias("id"),
        F.lit(None).cast("int").alias("usuario_id"),
        F.concat(F.lit(f"{tabla} "), i.cast("string")).alias("nombre"),
        F.lit(1).alias("es_por_defecto"),
        ahora.alias("creado_en"),
        ahora.alias("actualizado_en"),
        F.lit(None).cast("long").alias("eliminado_en"),
    )
    return evento(base, tabla, "c", 0, nulo(fila), fila, F.col("ts"))


def usuarios_tabla(spark, n):
    base = spark.range(1, n + 1).select("id", F.lit(f"{P.INICIO} 00:00:00").cast("timestamp").alias("ts"))
    i = F.col("id")
    ahora = ms(F.col("ts"))
    fila = F.struct(
        i.alias("id"),
        F.concat(F.lit("Usuario "), i.cast("string")).alias("nombre"),
        F.lit("Apellido").alias("apellido_p"),
        F.lit("Sintetico").alias("apellido_m"),
        F.concat(F.lit("usuario"), i.cast("string"), F.lit("@ejemplo.test")).alias("correo"),
        F.lit(10000).alias("fecha_nacimiento"),  # días desde 1970
        F.lit('["read"]').alias("scopes"),
        F.lit(1).alias("activo"),
        ahora.alias("creado_en"),
        ahora.alias("actualizado_en"),
    )
    return evento(base, "usuarios", "c", 0, nulo(fila), fila, F.col("ts"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--filas", type=int, required=True, help="filas de la tabla egresos (ingresos tiene 1/5)")
    ap.add_argument("--salida", required=True, help="carpeta donde queda el Bronze sintético")
    ap.add_argument("--usuarios", type=int, default=1000, help="usuarios distintos (por defecto 1000)")
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("moneywise-benchmark-generar")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    partes = [
        movimientos(spark, "egresos", args.filas, args.usuarios),
        movimientos(spark, "ingresos", args.filas, args.usuarios),
        usuarios_tabla(spark, args.usuarios),
        catalogo(spark, "destinos", P.N_DESTINOS),
        catalogo(spark, "tipos_egreso", P.N_TIPOS_EGRESO),
        catalogo(spark, "tipos_ingreso", P.N_TIPOS_INGRESO),
        catalogo(spark, "procedencias", P.N_PROCEDENCIAS),
        catalogo(spark, "frecuencias", P.N_FRECUENCIAS),
    ]
    bronze = reduce(DataFrame.unionByName, partes)

    # Un archivo por (tabla, fecha), igual que la partición del Bronze real.
    bronze.repartition("tabla", "fecha").write.mode("overwrite").partitionBy("tabla", "fecha").parquet(args.salida)

    print("\n--- Bronze sintético ---")
    por_tabla = spark.read.parquet(args.salida).groupBy("tabla").count().orderBy("tabla").collect()
    total = 0
    for fila in por_tabla:
        print(f"{fila['tabla']:<22} {fila['count']:>12} eventos")
        total += fila["count"]
    print(f"BRONZE_EVENTOS {total}")
    print("Generador: terminado.")


if __name__ == "__main__":
    main()
