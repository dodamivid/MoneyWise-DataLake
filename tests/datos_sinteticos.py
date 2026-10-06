"""Lago sintético para el CI: un Silver inventado (con casos difíciles) y un Gold calculado con la referencia.

No usa datos reales ni Spark. Los Parquet que escribe tienen los mismos tipos que los de Spark, así que
pasan por el mismo lector y los mismos tests que los datos reales.
"""
import random
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import referencia
from calidad import CONTRATO

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "spark"))
from common.schemas import TABLAS  # noqa: E402  (solo diccionarios de tipos: no importa Spark)

TIPOS_ARROW = {
    "int32": pa.int32(), "int64": pa.int64(), "string": pa.string(), "date32[day]": pa.date32(),
    "decimal128(12, 2)": pa.decimal128(12, 2), "decimal128(18, 2)": pa.decimal128(18, 2),
    "decimal128(26, 2)": pa.decimal128(26, 2),
}


def _tipo_silver(texto):
    simples = {"int": pa.int32(), "bigint": pa.int64(), "string": pa.string(), "boolean": pa.bool_(),
               "timestamp": pa.timestamp("ns"), "date": pa.date32()}  # Spark guarda los timestamps como ns sin zona
    if texto.startswith("decimal"):
        precision, escala = texto[len("decimal("):-1].split(",")
        return pa.decimal128(int(precision), int(escala))
    return simples[texto]


def esquema_silver(tabla):
    campos = [(c, _tipo_silver(t)) for c, t in TABLAS[tabla].items()]
    return pa.schema(campos + [("_ultimo_offset", pa.int64()), ("_ultima_op", pa.string())])


def esquema_gold(tabla):
    return pa.schema([(c, TIPOS_ARROW[t]) for c, t in CONTRATO[tabla].items()])


# ------------------------------------------------------------------------------ Silver
def construir_silver(semilla=7):
    azar = random.Random(semilla)
    t0 = datetime(2026, 1, 1)
    auditoria = {"creado_en": t0, "actualizado_en": t0, "eliminado_en": None}
    silver = {t: [] for t in TABLAS}

    def agregar(tabla, **valores):
        fila = {c: None for c in TABLAS[tabla]}
        fila.update(valores)
        silver[tabla].append(fila)

    for u in (1, 2, 3):
        agregar("usuarios", id=u, nombre=f"U{u}", apellido_p="X", apellido_m="Y", correo=f"u{u}@x.mx",
                fecha_nacimiento=date(1995, 1, u), scopes="[]", activo=True, creado_en=t0, actualizado_en=t0)
    nombres = [(1, "Renta"), (2, "Servicios"), (3, "Transporte"), (4, "Alimentación")]
    for i, n in nombres:
        agregar("destinos", id=i, usuario_id=None, nombre=n, es_por_defecto=True, **auditoria)
    for u in (1, 2, 3):
        for k, n in enumerate(("Salud", "Mascotas")):
            agregar("destinos", id=5 + 2 * (u - 1) + k, usuario_id=u, nombre=n, es_por_defecto=False, **auditoria)
    for i, n in ((1, "Efectivo"), (2, "Transferencia"), (3, "Tarjeta")):
        agregar("tipos_egreso", id=i, usuario_id=None, nombre=n, es_por_defecto=True, **auditoria)
    for i, n in ((1, "Fijo"), (2, "Variable")):
        agregar("tipos_ingreso", id=i, usuario_id=None, nombre=n, es_por_defecto=True, **auditoria)
    agregar("procedencias", id=1, usuario_id=None, nombre="Sueldo", es_por_defecto=True, **auditoria)
    agregar("frecuencias", id=1, nombre="Mensual", **auditoria)

    def fecha(mes):
        return datetime(2026, mes, azar.randint(1, 28), azar.randint(0, 23), azar.randint(0, 59))

    n_ing = 0

    def ingreso(u, momento, monto, borrado=False):
        nonlocal n_ing
        n_ing += 1
        agregar("ingresos", id=n_ing, usuario_id=u, tipo_id=1, procedencia_id=1, monto=monto, descripcion="x",
                fecha_inicio=momento, **{**auditoria, "eliminado_en": t0 if borrado else None})

    for u in (1, 2, 3):
        for mes in range(1, 10):
            if u == 3 and mes in (3, 4):
                continue  # meses con solo egresos
            for _ in range(azar.choice((1, 2))):
                ingreso(u, fecha(mes), Decimal(azar.randint(500000, 1500000)) / 100, borrado=azar.random() < 0.08)
    ingreso(2, datetime(2025, 12, 31, 23, 59, 59), Decimal("777.77"))  # borde: último segundo del año anterior
    ingreso(2, datetime(2026, 1, 1, 0, 0, 0), Decimal("888.88"))       # borde: primer segundo del año

    n_egr = 0

    def egreso(u, momento, monto, destino, tipo=1, borrado=False):
        nonlocal n_egr
        n_egr += 1
        agregar("egresos", id=n_egr, usuario_id=u, tipo_id=tipo, destino_id=destino, monto=monto, descripcion="x",
                fecha_inicio=momento, **{**auditoria, "eliminado_en": t0 if borrado else None})

    for _ in range(330):
        u, mes = azar.choice((1, 2, 3)), azar.randint(1, 9)
        destino = azar.choice([1, 2, 3, 4, 5 + 2 * (u - 1), 6 + 2 * (u - 1), None])
        monto = Decimal(str(round(azar.lognormvariate(6.0, 0.7), 2))).quantize(Decimal("0.01"))
        egreso(u, fecha(mes), monto, destino, tipo=azar.choice((1, 2, 3)), borrado=azar.random() < 0.06)
    egreso(1, datetime(2026, 6, 10, 12), Decimal("321.00"), 77, tipo=99)           # destino y tipo que NO existen en el catálogo
    egreso(2, datetime(2026, 7, 15, 12), Decimal("90000.00"), 4)                   # claramente extremo
    egreso(3, datetime(2026, 3, 31, 23, 59, 59), Decimal("432.10"), 3, tipo=2)     # borde de mes
    egreso(3, datetime(2026, 4, 1, 0, 0, 0), Decimal("567.89"), 3, tipo=2)         # borde de mes

    for tabla, filas in silver.items():
        for i, fila in enumerate(filas):
            fila["_ultimo_offset"], fila["_ultima_op"] = i + 1, "r"
    return silver


def construir_gold(silver):
    return {**referencia.gold(silver), "anomalias": referencia.anomalias(silver)}


def construir(semilla=7):
    silver = construir_silver(semilla)
    return silver, construir_gold(silver)


# ------------------------------------------------------------------------------ disco
def _escribir(carpeta, esquema, filas):
    carpeta.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(filas, schema=esquema), carpeta / "part-00000.snappy.parquet")
    (carpeta / "_SUCCESS").touch()  # como deja Spark: el lector debe ignorar este archivo


def escribir_lago(base, silver, gold):
    base = Path(base)
    for tabla, filas in silver.items():
        _escribir(base / "silver" / tabla, esquema_silver(tabla), filas)
    for tabla, filas in gold.items():
        _escribir(base / "gold" / tabla, esquema_gold(tabla), filas)
