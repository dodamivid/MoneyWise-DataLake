"""Implementación de REFERENCIA de Gold y de anomalías, en Python puro (sin Spark).

Sirve para dos cosas:
  1. Recalcular Gold desde Silver y compararlo con lo que dejó Spark: un segundo cálculo, independiente.
  2. Fabricar un lago sintético para el CI, sin datos reales y sin Spark.

Replica las reglas de spark/jobs/gold/gold_job.py y anomalies/anomalias_job.py. Los montos van como
Decimal (exactos); solo los cuartiles de las anomalías usan float, igual que el job.
"""
from datetime import date
from decimal import Decimal, ROUND_HALF_UP

CERO = Decimal("0")
CENTAVO = Decimal("0.01")
MIN_MOVIMIENTOS = 8
K_ATIPICO = 1.5
K_EXTREMO = 3.0


def mes_de(fecha):
    """Primer día del mes de una fecha (o None)."""
    return date(fecha.year, fecha.month, 1) if fecha is not None else None


def mes_anterior(mes):
    return date(mes.year - 1, 12, 1) if mes.month == 1 else date(mes.year, mes.month - 1, 1)


def activos(filas):
    """Solo los movimientos no eliminados (eliminado_en vacío)."""
    return [f for f in filas if f["eliminado_en"] is None]


def redondear(valor):
    """Redondeo a centavos, mitad hacia arriba, como el round de Spark."""
    return valor.quantize(CENTAVO, rounding=ROUND_HALF_UP)


def _orden_mes(mes):
    return (mes is not None, mes or date.min)  # Spark ordena los NULL primero


# ----------------------------------------------------------------------------- Gold
def balance_mensual(silver):
    ing, egr = {}, {}
    for acumulador, filas in ((ing, activos(silver["ingresos"])), (egr, activos(silver["egresos"]))):
        for f in filas:
            total_n = acumulador.setdefault((f["usuario_id"], mes_de(f["fecha_inicio"])), [CERO, 0])
            if f["monto"] is not None:
                total_n[0] += f["monto"]
            total_n[1] += 1

    filas, corrido = [], {}
    for usuario, mes in sorted(set(ing) | set(egr), key=lambda k: (k[0], *_orden_mes(k[1]))):
        total_ing, n_ing = ing.get((usuario, mes), (CERO, 0))
        total_egr, n_egr = egr.get((usuario, mes), (CERO, 0))
        balance = total_ing - total_egr
        corrido[usuario] = corrido.get(usuario, CERO) + balance
        filas.append({
            "usuario_id": usuario, "mes": mes, "total_ingresos": total_ing, "total_egresos": total_egr,
            "balance": balance, "n_ingresos": n_ing, "n_egresos": n_egr, "balance_acumulado": corrido[usuario],
        })
    return filas


def gasto_mensual(silver, catalogo, col_id, col_nombre, sin_nombre):
    """Gasto por usuario, mes y una dimensión (destino o tipo), con variación contra el mes anterior."""
    nombres = {f["id"]: f["nombre"] for f in silver[catalogo]}
    totales = {}
    for f in activos(silver["egresos"]):
        ident = f[col_id] if f[col_id] is not None else 0
        nombre = nombres.get(ident)
        if nombre is None:
            nombre = sin_nombre
        total_n = totales.setdefault((f["usuario_id"], mes_de(f["fecha_inicio"]), ident, nombre), [CERO, 0])
        if f["monto"] is not None:
            total_n[0] += f["monto"]
        total_n[1] += 1

    por_clave = {(u, i, m): total for (u, m, i, _), (total, _n) in totales.items()}
    filas = []
    for (u, m, ident, nombre), (total, n) in sorted(
        totales.items(), key=lambda kv: (kv[0][0], *_orden_mes(kv[0][1]), kv[0][3])
    ):
        anterior = por_clave.get((u, ident, mes_anterior(m))) if m is not None else None
        variacion = None if anterior is None or anterior == 0 else redondear((total - anterior) * 100 / anterior)
        filas.append({
            "usuario_id": u, "mes": m, col_id: ident, col_nombre: nombre, "total_egresos": total,
            "n_egresos": n, "total_mes_anterior": anterior, "variacion_pct": variacion,
        })
    return filas


def gold(silver):
    return {
        "balance_mensual": balance_mensual(silver),
        "gasto_por_destino_mensual": gasto_mensual(silver, "destinos", "destino_id", "destino", "Sin destino"),
        "gasto_por_tipo_mensual": gasto_mensual(silver, "tipos_egreso", "tipo_id", "tipo", "Sin tipo"),
    }


# ------------------------------------------------------------------------ anomalías
def percentil(ordenados, p):
    """Percentil con interpolación lineal entre los dos valores vecinos."""
    posicion = (len(ordenados) - 1) * p
    abajo = int(posicion)
    arriba = min(abajo + 1, len(ordenados) - 1)
    return ordenados[abajo] + (ordenados[arriba] - ordenados[abajo]) * (posicion - abajo)


def _a_decimal(valor):
    return redondear(Decimal(repr(valor)))


def anomalias(silver):
    """Gastos inusualmente altos por (usuario, destino), con el método IQR. Devuelve las filas completas."""
    nombres = {f["id"]: f["nombre"] for f in silver["destinos"]}
    grupos = {}
    for f in activos(silver["egresos"]):
        grupos.setdefault((f["usuario_id"], f["destino_id"] or 0), []).append(f)

    filas = []
    for (usuario, destino), movimientos in grupos.items():
        montos = sorted(float(f["monto"]) for f in movimientos if f["monto"] is not None)
        if len(movimientos) < MIN_MOVIMIENTOS or not montos:
            continue
        q1, q3 = percentil(montos, 0.25), percentil(montos, 0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        limite_atipico, limite_extremo = q3 + K_ATIPICO * iqr, q3 + K_EXTREMO * iqr
        nombre = nombres.get(destino)
        for f in movimientos:
            if f["monto"] is None or float(f["monto"]) <= limite_atipico:
                continue
            monto = float(f["monto"])
            filas.append({
                "egreso_id": f["id"], "usuario_id": usuario, "destino_id": destino,
                "destino": nombre if nombre is not None else "Sin destino",
                "fecha": f["fecha_inicio"].date() if f["fecha_inicio"] is not None else None,
                "monto": f["monto"], "n_grupo": len(movimientos),
                "q1": _a_decimal(q1), "q3": _a_decimal(q3), "iqr": _a_decimal(iqr),
                "limite_atipico": _a_decimal(limite_atipico), "limite_extremo": _a_decimal(limite_extremo),
                "exceso_sobre_limite": _a_decimal(monto - limite_atipico),
                "severidad": "extremo" if monto > limite_extremo else "atipico",
            })
    return filas


def severidades(silver):
    """{id del egreso: 'atipico' | 'extremo'} según el recálculo."""
    return {f["egreso_id"]: f["severidad"] for f in anomalias(silver)}
