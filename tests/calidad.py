"""Chequeos de calidad de la capa Gold.

Cada función devuelve una lista de problemas en texto: vacía significa que todo está bien.
Así sirven tanto para los datos reales como para probar los propios chequeos con defectos inyectados
(tests/test_detecta_defectos.py). Las tablas llegan como listas de diccionarios {columna: valor}.
"""
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal

import referencia

CERO = Decimal("0")
TOLERANCIA = Decimal("0.01")  # el redondeo de Spark puede mover un centavo en columnas derivadas
MAX_EJEMPLOS = 5

# El contrato de Gold: columnas y tipos exactos que deja Spark (lo que el dashboard va a leer).
CONTRATO = {
    "balance_mensual": {
        "usuario_id": "int32", "mes": "date32[day]",
        "total_ingresos": "decimal128(18, 2)", "total_egresos": "decimal128(18, 2)", "balance": "decimal128(18, 2)",
        "n_ingresos": "int64", "n_egresos": "int64", "balance_acumulado": "decimal128(18, 2)",
    },
    "gasto_por_destino_mensual": {
        "usuario_id": "int32", "mes": "date32[day]", "destino_id": "int32", "destino": "string",
        "total_egresos": "decimal128(18, 2)", "n_egresos": "int64",
        "total_mes_anterior": "decimal128(18, 2)", "variacion_pct": "decimal128(26, 2)",
    },
    "gasto_por_tipo_mensual": {
        "usuario_id": "int32", "mes": "date32[day]", "tipo_id": "int32", "tipo": "string",
        "total_egresos": "decimal128(18, 2)", "n_egresos": "int64",
        "total_mes_anterior": "decimal128(18, 2)", "variacion_pct": "decimal128(26, 2)",
    },
    "anomalias": {
        "egreso_id": "int64", "usuario_id": "int32", "destino_id": "int32", "destino": "string",
        "fecha": "date32[day]", "monto": "decimal128(12, 2)", "n_grupo": "int64",
        "q1": "decimal128(18, 2)", "q3": "decimal128(18, 2)", "iqr": "decimal128(18, 2)",
        "limite_atipico": "decimal128(18, 2)", "limite_extremo": "decimal128(18, 2)",
        "exceso_sobre_limite": "decimal128(18, 2)", "severidad": "string",
    },
}

# La llave de cada tabla: no puede haber dos filas con la misma.
LLAVES = {
    "balance_mensual": ["usuario_id", "mes"],
    "gasto_por_destino_mensual": ["usuario_id", "mes", "destino_id"],
    "gasto_por_tipo_mensual": ["usuario_id", "mes", "tipo_id"],
    "anomalias": ["egreso_id"],
}

# Campos que nunca pueden venir vacíos. Solo estos dos pueden ser NULL: el primer mes no tiene mes anterior.
PUEDEN_SER_NULOS = {"total_mes_anterior", "variacion_pct"}
NO_NULOS = {tabla: [c for c in columnas if c not in PUEDEN_SER_NULOS] for tabla, columnas in CONTRATO.items()}


def _ejemplos(items):
    items = list(items)
    texto = "; ".join(str(i) for i in items[:MAX_EJEMPLOS])
    return texto + (f"; ... y {len(items) - MAX_EJEMPLOS} más" if len(items) > MAX_EJEMPLOS else "")


# ---------------------------------------------------------------------------- contrato
def problemas_de_contrato(esquema, esperado):
    """esquema y esperado: {columna: tipo en texto}."""
    problemas = []
    faltan, sobran = sorted(set(esperado) - set(esquema)), sorted(set(esquema) - set(esperado))
    if faltan:
        problemas.append(f"faltan columnas: {faltan}")
    if sobran:
        problemas.append(f"columnas que no están en el contrato: {sobran}")
    for columna in esperado:
        if columna in esquema and esquema[columna] != esperado[columna]:
            problemas.append(f"columna {columna}: tipo {esquema[columna]}, el contrato dice {esperado[columna]}")
    return problemas


# ------------------------------------------------------------------------ nulos y llaves
def problemas_de_nulos(filas, columnas):
    problemas = []
    for columna in columnas:
        posiciones = [i for i, f in enumerate(filas) if f.get(columna) is None]
        if posiciones:
            problemas.append(f"{columna}: {len(posiciones)} valores nulos (filas {_ejemplos(posiciones)})")
    return problemas


def problemas_de_duplicados(filas, llaves):
    cuenta = Counter(tuple(f[k] for k in llaves) for f in filas)
    repetidas = {clave: n for clave, n in cuenta.items() if n > 1}
    return [f"llave {tuple(llaves)} repetida: {_ejemplos(f'{k} x{n}' for k, n in repetidas.items())}"] if repetidas else []


def problemas_de_meses(filas):
    malos = [f["mes"] for f in filas if f["mes"] is not None and f["mes"].day != 1]
    return [f"{len(malos)} filas con un 'mes' que no es día 1: {_ejemplos(malos)}"] if malos else []


def problemas_de_negativos(filas, columnas):
    problemas = []
    for columna in columnas:
        negativos = [f[columna] for f in filas if f.get(columna) is not None and f[columna] < 0]
        if negativos:
            problemas.append(f"{columna}: {len(negativos)} valores negativos ({_ejemplos(negativos)})")
    return problemas


# --------------------------------------------------------------------------- coherencia
def problemas_de_balance(balance):
    problemas, corrido = [], defaultdict(lambda: CERO)
    en_orden = sorted(balance, key=lambda f: (f["usuario_id"], f["mes"] is not None, f["mes"] or date.min))
    for f in en_orden:
        esperado = f["total_ingresos"] - f["total_egresos"]
        if f["balance"] != esperado:
            problemas.append(f"usuario {f['usuario_id']} {f['mes']}: balance {f['balance']} y ingresos - egresos = {esperado}")
        corrido[f["usuario_id"]] += f["balance"]
        if f["balance_acumulado"] != corrido[f["usuario_id"]]:
            problemas.append(
                f"usuario {f['usuario_id']} {f['mes']}: balance_acumulado {f['balance_acumulado']}, "
                f"la suma de los balances hasta aquí es {corrido[f['usuario_id']]}"
            )
    return problemas


def problemas_de_totales_de_gasto(gasto, balance, nombre):
    """El gasto desglosado (por destino o por tipo) debe sumar lo mismo que el balance de cada usuario y mes."""
    suma = defaultdict(lambda: [CERO, 0])
    for f in gasto:
        suma[(f["usuario_id"], f["mes"])][0] += f["total_egresos"]
        suma[(f["usuario_id"], f["mes"])][1] += f["n_egresos"]
    esperado = {(f["usuario_id"], f["mes"]): [f["total_egresos"], f["n_egresos"]] for f in balance if f["n_egresos"] or f["total_egresos"]}
    diferencias = []
    for clave in sorted(set(suma) | set(esperado), key=lambda k: (k[0], k[1] or date.min)):
        if list(suma.get(clave, [CERO, 0])) != list(esperado.get(clave, [CERO, 0])):
            diferencias.append(f"usuario {clave[0]} {clave[1]}: {nombre} suma {tuple(suma.get(clave, [CERO, 0]))}, el balance dice {tuple(esperado.get(clave, [CERO, 0]))}")
    return [f"{len(diferencias)} meses no cuadran con el balance: {_ejemplos(diferencias)}"] if diferencias else []


def problemas_de_variacion(gasto, col_id):
    """total_mes_anterior debe ser el total del mes calendario anterior y variacion_pct su cambio porcentual."""
    por_clave = {(f["usuario_id"], f[col_id], f["mes"]): f["total_egresos"] for f in gasto}
    malos = []
    for f in gasto:
        anterior = por_clave.get((f["usuario_id"], f[col_id], referencia.mes_anterior(f["mes"]))) if f["mes"] is not None else None
        esperada = None if anterior is None or anterior == 0 else referencia.redondear((f["total_egresos"] - anterior) * 100 / anterior)
        if f["total_mes_anterior"] != anterior or f["variacion_pct"] != esperada:
            malos.append(
                f"usuario {f['usuario_id']} {f['mes']} {col_id}={f[col_id]}: mes anterior {f['total_mes_anterior']} "
                f"(esperado {anterior}), variación {f['variacion_pct']} (esperada {esperada})"
            )
    return [f"{len(malos)} filas con el mes anterior o la variación mal calculados: {_ejemplos(malos)}"] if malos else []


def _canonica(fila, columnas):
    return tuple((c, fila[c]) for c in columnas)


def problemas_gold_vs_silver(gold, silver):
    """Recalcula Gold desde Silver en Python puro y lo compara, fila por fila, con lo que dejó Spark."""
    problemas = []
    for tabla, filas_ref in referencia.gold(silver).items():
        columnas = list(CONTRATO[tabla])
        reales = Counter(_canonica(f, columnas) for f in gold[tabla])
        esperadas = Counter(_canonica(f, columnas) for f in filas_ref)
        faltan, sobran = esperadas - reales, reales - esperadas
        if faltan or sobran:
            problemas.append(
                f"{tabla}: no coincide con el recálculo desde Silver "
                f"(faltan {sum(faltan.values())} filas, sobran {sum(sobran.values())}). "
                f"Ejemplo faltante: {dict(next(iter(faltan))) if faltan else '-'}. "
                f"Ejemplo sobrante: {dict(next(iter(sobran))) if sobran else '-'}"
            )
    return problemas


# ---------------------------------------------------------------------------- anomalías
def problemas_de_anomalias(anomalias):
    problemas = []
    for f in anomalias:
        id_ = f["egreso_id"]
        if f["severidad"] not in ("atipico", "extremo"):
            problemas.append(f"egreso {id_}: severidad '{f['severidad']}' no válida")
            continue
        if f["n_grupo"] < referencia.MIN_MOVIMIENTOS:
            problemas.append(f"egreso {id_}: viene de un grupo de solo {f['n_grupo']} movimientos")
        if f["iqr"] <= 0:
            problemas.append(f"egreso {id_}: viene de un grupo con IQR = {f['iqr']}")
        if f["limite_extremo"] < f["limite_atipico"]:
            problemas.append(f"egreso {id_}: el límite extremo es menor que el atípico")
        if f["monto"] < f["limite_atipico"]:
            problemas.append(f"egreso {id_}: monto {f['monto']} no supera el límite atípico {f['limite_atipico']}")
        if f["severidad"] == "extremo" and f["monto"] < f["limite_extremo"]:
            problemas.append(f"egreso {id_}: es 'extremo' pero {f['monto']} no supera {f['limite_extremo']}")
        if f["severidad"] == "atipico" and f["monto"] > f["limite_extremo"]:
            problemas.append(f"egreso {id_}: es 'atipico' pero {f['monto']} supera el límite extremo {f['limite_extremo']}")
        if abs(f["exceso_sobre_limite"] - (f["monto"] - f["limite_atipico"])) > TOLERANCIA:
            problemas.append(f"egreso {id_}: exceso_sobre_limite {f['exceso_sobre_limite']} no es monto - límite")
    problemas += problemas_de_duplicados(anomalias, ["egreso_id"])
    return problemas


def problemas_anomalias_vs_silver(anomalias, silver):
    """Cada anomalía debe ser un egreso activo de Silver, y el conjunto debe coincidir con el recálculo."""
    problemas = []
    egresos = {f["id"]: f for f in referencia.activos(silver["egresos"])}
    for a in anomalias:
        f = egresos.get(a["egreso_id"])
        if f is None:
            problemas.append(f"egreso {a['egreso_id']}: no existe como egreso activo en Silver")
        elif (f["usuario_id"], f["monto"]) != (a["usuario_id"], a["monto"]):
            problemas.append(f"egreso {a['egreso_id']}: usuario o monto distintos a los de Silver")
    reales = {a["egreso_id"]: a["severidad"] for a in anomalias}
    esperadas = referencia.severidades(silver)
    faltan = sorted(set(esperadas) - set(reales))
    sobran = sorted(set(reales) - set(esperadas))
    cambian = sorted(i for i in set(reales) & set(esperadas) if reales[i] != esperadas[i])
    if faltan:
        problemas.append(f"{len(faltan)} anomalías que el recálculo encuentra y la tabla no tiene: {_ejemplos(faltan)}")
    if sobran:
        problemas.append(f"{len(sobran)} anomalías en la tabla que el recálculo no encuentra: {_ejemplos(sobran)}")
    if cambian:
        problemas.append(f"{len(cambian)} anomalías con otra severidad que en el recálculo: {_ejemplos(cambian)}")
    return problemas


# ------------------------------------------------------------------------------ fuente
def problemas_conteos_vs_fuente(lago, fuente):
    """lago y fuente: {tabla: número de filas}."""
    return [f"{t}: el lago tiene {lago[t]} filas y la fuente {fuente[t]}" for t in fuente if t in lago and lago[t] != fuente[t]]


def problemas_totales_vs_fuente(balance, ingresos_fuente, egresos_fuente):
    """Gold contra la fuente. *_fuente: {(usuario_id, mes): (total, cantidad)} de los movimientos activos."""
    problemas = []
    for nombre, fuente, col_total, col_n in (
        ("ingresos", ingresos_fuente, "total_ingresos", "n_ingresos"),
        ("egresos", egresos_fuente, "total_egresos", "n_egresos"),
    ):
        lago = {(f["usuario_id"], f["mes"]): (f[col_total], f[col_n]) for f in balance if f[col_n] or f[col_total]}
        diferencias = []
        for clave in sorted(set(lago) | set(fuente), key=lambda k: (k[0], k[1] or date.min)):
            if lago.get(clave, (CERO, 0)) != fuente.get(clave, (CERO, 0)):
                diferencias.append(f"usuario {clave[0]} {clave[1]}: lago {lago.get(clave, (CERO, 0))}, fuente {fuente.get(clave, (CERO, 0))}")
        if diferencias:
            problemas.append(f"{nombre}: {len(diferencias)} (usuario, mes) no cuadran con la fuente: {_ejemplos(diferencias)}")
    return problemas


# ---------------------------------------------------------------------------- BigQuery
TIPOS_BIGQUERY = {"int32": "INTEGER", "int64": "INTEGER", "string": "STRING", "date32[day]": "DATE"}
ALIAS_BIGQUERY = {"INT64": "INTEGER", "FLOAT64": "FLOAT", "BOOL": "BOOLEAN"}


def tipo_en_bigquery(tipo_arrow):
    """Cómo debe verse en BigQuery cada tipo del contrato de Gold (los decimales, como NUMERIC)."""
    return "NUMERIC" if tipo_arrow.startswith("decimal128") else TIPOS_BIGQUERY[tipo_arrow]


def problemas_de_esquema_bigquery(tabla, esquema):
    """esquema: {columna: tipo en BigQuery}. Debe tener las columnas y tipos del contrato de Gold."""
    esperado = {c: tipo_en_bigquery(t) for c, t in CONTRATO[tabla].items()}
    return problemas_de_contrato({c: ALIAS_BIGQUERY.get(t, t) for c, t in esquema.items()}, esperado)


def problemas_lago_vs_bigquery(tabla, filas_lago, filas_bigquery):
    """La tabla de BigQuery debe ser idéntica, fila por fila, al Gold del lago."""
    columnas = list(CONTRATO[tabla])
    lago = Counter(_canonica(f, columnas) for f in filas_lago)
    almacen = Counter(_canonica(f, columnas) for f in filas_bigquery)
    faltan, sobran = lago - almacen, almacen - lago
    if not (faltan or sobran):
        return []
    return [
        f"{tabla}: BigQuery no coincide con el lago (le faltan {sum(faltan.values())} filas y le sobran {sum(sobran.values())}). "
        f"Ejemplo faltante: {dict(next(iter(faltan))) if faltan else '-'}. "
        f"Ejemplo sobrante: {dict(next(iter(sobran))) if sobran else '-'}"
    ]
