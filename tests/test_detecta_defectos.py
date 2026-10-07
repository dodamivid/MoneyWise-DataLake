"""Pruebas de las pruebas: se inyecta un defecto a un lago sano y el chequeo correspondiente DEBE detectarlo.

Un chequeo que nunca falla no demuestra nada. Estos tests corren siempre sobre un lago sintético en memoria
(no dependen de tus datos), así que protegen a los chequeos de romperse sin que nadie lo note.
"""
import copy
import sys
import types
from datetime import date
from decimal import Decimal

import pytest

import calidad as c
import datos_sinteticos
import fuente
import referencia

UNO = Decimal("1.00")


@pytest.fixture(scope="module")
def sano():
    silver = datos_sinteticos.construir_silver()
    gold = datos_sinteticos.construir_gold(silver)
    esquemas = {t: dict(columnas) for t, columnas in c.CONTRATO.items()}
    return silver, gold, esquemas


def cambiar(filas, indice, **valores):
    copia = copy.deepcopy(filas)
    copia[indice].update(valores)
    return copia


def sin_fila(filas, indice):
    return filas[:indice] + filas[indice + 1:]


def primero(filas, condicion):
    return next(i for i, f in enumerate(filas) if condicion(f))


def con_esquema(esquemas, tabla, **cambios):
    nuevo = dict(esquemas[tabla])
    for columna, tipo in cambios.items():
        if tipo is None:
            nuevo.pop(columna)
        else:
            nuevo[columna] = tipo
    return nuevo


def _esquema_bigquery(tabla, **cambios):
    """El esquema que BigQuery debería mostrar para una tabla de Gold ya cargada (con cambios para inyectar defectos)."""
    esquema = {columna: c.tipo_en_bigquery(tipo) for columna, tipo in c.CONTRATO[tabla].items()}
    for columna, tipo in cambios.items():
        if tipo is None:
            esquema.pop(columna)
        else:
            esquema[columna] = tipo
    return esquema


def lago_sano(silver, gold, esquemas):
    """Todos los chequeos sobre el lago sin tocar. Debe dar cero problemas."""
    problemas = []
    for tabla in c.CONTRATO:
        problemas += c.problemas_de_contrato(esquemas[tabla], c.CONTRATO[tabla])
        problemas += c.problemas_de_nulos(gold[tabla], c.NO_NULOS[tabla])
        problemas += c.problemas_de_duplicados(gold[tabla], c.LLAVES[tabla])
    for tabla in ("balance_mensual", "gasto_por_destino_mensual", "gasto_por_tipo_mensual"):
        problemas += c.problemas_de_meses(gold[tabla])
    problemas += c.problemas_de_balance(gold["balance_mensual"])
    problemas += c.problemas_de_totales_de_gasto(gold["gasto_por_destino_mensual"], gold["balance_mensual"], "destino")
    problemas += c.problemas_de_totales_de_gasto(gold["gasto_por_tipo_mensual"], gold["balance_mensual"], "tipo")
    problemas += c.problemas_de_variacion(gold["gasto_por_destino_mensual"], "destino_id")
    problemas += c.problemas_de_variacion(gold["gasto_por_tipo_mensual"], "tipo_id")
    problemas += c.problemas_gold_vs_silver(gold, silver)
    problemas += c.problemas_de_anomalias(gold["anomalias"])
    problemas += c.problemas_anomalias_vs_silver(gold["anomalias"], silver)
    for tabla in c.CONTRATO:
        problemas += c.problemas_de_esquema_bigquery(tabla, _esquema_bigquery(tabla))
        problemas += c.problemas_lago_vs_bigquery(tabla, gold[tabla], copy.deepcopy(gold[tabla]))
    return problemas


def test_el_lago_sano_no_tiene_ningun_problema(sano):
    assert lago_sano(*sano) == []


def test_el_lago_sintetico_tiene_los_casos_dificiles(sano):
    """Si el lago sintético no tuviera estos casos, los tests de defectos serían vacíos."""
    silver, gold, _ = sano
    assert any(f["destino"] == "Sin destino" for f in gold["gasto_por_destino_mensual"])
    assert any(f["tipo"] == "Sin tipo" for f in gold["gasto_por_tipo_mensual"])
    assert any(f["n_ingresos"] == 0 for f in gold["balance_mensual"])
    assert any(f["variacion_pct"] is not None and f["variacion_pct"] < 0 for f in gold["gasto_por_destino_mensual"])
    assert {f["severidad"] for f in gold["anomalias"]} == {"atipico", "extremo"}
    assert any(f["eliminado_en"] is not None for f in silver["egresos"])


# Cada caso: (nombre, función que recibe (silver, gold, esquemas) y devuelve los problemas que el chequeo encuentra)
CASOS = {
    "columna renombrada": lambda s, g, e: c.problemas_de_contrato(
        con_esquema(e, "balance_mensual", balance=None, total="decimal128(18, 2)"), c.CONTRATO["balance_mensual"]),
    "tipo cambiado de decimal a double": lambda s, g, e: c.problemas_de_contrato(
        con_esquema(e, "balance_mensual", total_ingresos="double"), c.CONTRATO["balance_mensual"]),
    "columna extra": lambda s, g, e: c.problemas_de_contrato(
        con_esquema(e, "anomalias", descripcion="string"), c.CONTRATO["anomalias"]),
    "usuario_id nulo": lambda s, g, e: c.problemas_de_nulos(
        cambiar(g["balance_mensual"], 0, usuario_id=None), c.NO_NULOS["balance_mensual"]),
    "destino nulo": lambda s, g, e: c.problemas_de_nulos(
        cambiar(g["gasto_por_destino_mensual"], 0, destino=None), c.NO_NULOS["gasto_por_destino_mensual"]),
    "total_egresos nulo": lambda s, g, e: c.problemas_de_nulos(
        cambiar(g["gasto_por_tipo_mensual"], 3, total_egresos=None), c.NO_NULOS["gasto_por_tipo_mensual"]),
    "fila duplicada en el balance": lambda s, g, e: c.problemas_de_duplicados(
        g["balance_mensual"] + [g["balance_mensual"][0]], c.LLAVES["balance_mensual"]),
    "anomalía repetida": lambda s, g, e: c.problemas_de_duplicados(
        g["anomalias"] + [g["anomalias"][0]], c.LLAVES["anomalias"]),
    "mes que no es día 1": lambda s, g, e: c.problemas_de_meses(
        cambiar(g["balance_mensual"], 0, mes=date(2026, 1, 15))),
    "conteo negativo": lambda s, g, e: c.problemas_de_negativos(
        cambiar(g["balance_mensual"], 0, n_egresos=-1), ["n_egresos"]),
    "balance mal calculado": lambda s, g, e: c.problemas_de_balance(
        cambiar(g["balance_mensual"], 0, balance=g["balance_mensual"][0]["balance"] + UNO)),
    "acumulado mal calculado": lambda s, g, e: c.problemas_de_balance(
        cambiar(g["balance_mensual"], 2, balance_acumulado=g["balance_mensual"][2]["balance_acumulado"] + UNO)),
    "el gasto por destino no suma lo del balance": lambda s, g, e: c.problemas_de_totales_de_gasto(
        cambiar(g["gasto_por_destino_mensual"], 0, total_egresos=g["gasto_por_destino_mensual"][0]["total_egresos"] + UNO),
        g["balance_mensual"], "destino"),
    "al gasto por tipo le falta una fila": lambda s, g, e: c.problemas_de_totales_de_gasto(
        sin_fila(g["gasto_por_tipo_mensual"], 0), g["balance_mensual"], "tipo"),
    "variación alterada": lambda s, g, e: c.problemas_de_variacion(
        cambiar(g["gasto_por_destino_mensual"], primero(g["gasto_por_destino_mensual"], lambda f: f["variacion_pct"] is not None),
                variacion_pct=Decimal("999.99")), "destino_id"),
    "mes anterior alterado": lambda s, g, e: c.problemas_de_variacion(
        cambiar(g["gasto_por_tipo_mensual"], primero(g["gasto_por_tipo_mensual"], lambda f: f["total_mes_anterior"] is not None),
                total_mes_anterior=UNO), "tipo_id"),
    "severidad inventada": lambda s, g, e: c.problemas_de_anomalias(
        cambiar(g["anomalias"], 0, severidad="grave")),
    "monto por debajo del límite atípico": lambda s, g, e: c.problemas_de_anomalias(
        cambiar(g["anomalias"], 0, monto=g["anomalias"][0]["limite_atipico"] - UNO)),
    "'extremo' que no llega al límite extremo": lambda s, g, e: c.problemas_de_anomalias(
        cambiar(g["anomalias"], primero(g["anomalias"], lambda f: f["severidad"] == "atipico"), severidad="extremo")),
    "anomalía de un grupo con muy pocos movimientos": lambda s, g, e: c.problemas_de_anomalias(
        cambiar(g["anomalias"], 0, n_grupo=3)),
    "Gold no refleja un egreso que Silver ya no tiene": lambda s, g, e: c.problemas_gold_vs_silver(
        g, {**s, "egresos": sin_fila(s["egresos"], primero(s["egresos"], lambda f: f["eliminado_en"] is None))}),
    "Gold no refleja un ingreso nuevo de Silver": lambda s, g, e: c.problemas_gold_vs_silver(
        g, {**s, "ingresos": s["ingresos"] + [
            {**s["ingresos"][primero(s["ingresos"], lambda f: f["eliminado_en"] is None)],
             "id": 99999, "eliminado_en": None, "monto": Decimal("123.45")}]}),  # un ingreso ACTIVO que Gold no tiene
    "Gold cuenta un egreso borrado lógicamente": lambda s, g, e: c.problemas_gold_vs_silver(
        g, {**s, "egresos": cambiar(s["egresos"], primero(s["egresos"], lambda f: f["eliminado_en"] is None), eliminado_en=date(2026, 9, 1))}),
    "anomalía de un egreso que no existe": lambda s, g, e: c.problemas_anomalias_vs_silver(
        g["anomalias"] + [{**g["anomalias"][0], "egreso_id": 999999}], s),
    "falta una anomalía que el recálculo encuentra": lambda s, g, e: c.problemas_anomalias_vs_silver(
        sin_fila(g["anomalias"], 0), s),
    "severidad distinta a la del recálculo": lambda s, g, e: c.problemas_anomalias_vs_silver(
        cambiar(g["anomalias"], primero(g["anomalias"], lambda f: f["severidad"] == "atipico"), severidad="extremo"), s),
    "BigQuery: un decimal quedó como FLOAT": lambda s, g, e: c.problemas_de_esquema_bigquery(
        "balance_mensual", _esquema_bigquery("balance_mensual", total_ingresos="FLOAT")),
    "BigQuery: falta una columna": lambda s, g, e: c.problemas_de_esquema_bigquery(
        "anomalias", _esquema_bigquery("anomalias", severidad=None)),
    "BigQuery: le falta una fila al almacén": lambda s, g, e: c.problemas_lago_vs_bigquery(
        "gasto_por_destino_mensual", g["gasto_por_destino_mensual"], sin_fila(g["gasto_por_destino_mensual"], 0)),
    "BigQuery: un monto distinto al del lago": lambda s, g, e: c.problemas_lago_vs_bigquery(
        "balance_mensual", g["balance_mensual"],
        cambiar(g["balance_mensual"], 0, total_egresos=g["balance_mensual"][0]["total_egresos"] + UNO)),
    "BigQuery: una fila repetida": lambda s, g, e: c.problemas_lago_vs_bigquery(
        "anomalias", g["anomalias"], g["anomalias"] + [g["anomalias"][0]]),
    "la fuente tiene una fila más": lambda s, g, e: c.problemas_conteos_vs_fuente(
        {"egresos": len(s["egresos"])}, {"egresos": len(s["egresos"]) + 1}),
    "la fuente tiene otro total": lambda s, g, e: c.problemas_totales_vs_fuente(
        g["balance_mensual"], _fuente(g, "ingresos"), {**_fuente(g, "egresos"), **{next(iter(_fuente(g, "egresos"))): (UNO, 1)}}),
    "la fuente tiene un mes que el lago no": lambda s, g, e: c.problemas_totales_vs_fuente(
        g["balance_mensual"], {**_fuente(g, "ingresos"), (1, date(2030, 1, 1)): (UNO, 1)}, _fuente(g, "egresos")),
}


def _fuente(gold, tipo):
    """Lo que la fuente 'diría' si coincidiera exactamente con el balance (para construir el caso de defecto)."""
    total, n = ("total_ingresos", "n_ingresos") if tipo == "ingresos" else ("total_egresos", "n_egresos")
    return {(f["usuario_id"], f["mes"]): (f[total], f[n]) for f in gold["balance_mensual"] if f[n] or f[total]}


@pytest.mark.parametrize("defecto", list(CASOS))
def test_el_chequeo_detecta_el_defecto(sano, defecto):
    problemas = CASOS[defecto](*copy.deepcopy(sano))
    assert problemas, f"El defecto '{defecto}' pasó sin que ningún chequeo lo notara"


def test_la_fuente_sana_no_da_problemas(sano):
    silver, gold, _ = sano
    assert c.problemas_totales_vs_fuente(gold["balance_mensual"], _fuente(gold, "ingresos"), _fuente(gold, "egresos")) == []
    assert c.problemas_conteos_vs_fuente({"egresos": 5}, {"egresos": 5}) == []


def test_railway_solo_lee_usa_tls_y_cuenta_en_utc(monkeypatch):
    """La conexión a Railway: solo SELECT, con TLS, sobre la base correcta y con el mes calculado en UTC."""
    sentencias, opciones = [], {}

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            sentencias.append(sql)
            self._sql = sql

        def fetchall(self):
            if "GROUP BY" in self._sql:
                return [(1, "2026-01-01", Decimal("10.00"), 2), (2, None, None, 1)]
            return [(5,)]

    class Conexion:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    def conectar(**kwargs):
        opciones.update(kwargs)
        return Conexion()

    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=conectar))
    for variable, valor in zip(fuente.VARIABLES, ("host.ejemplo", "3306", "debezium", "secreta")):
        monkeypatch.setenv(variable, valor)
    conexion = fuente.FuenteRailway()
    for tabla in fuente.TABLAS:
        assert conexion.conteo(tabla) == 5
    totales = conexion.totales_por_mes("egresos")
    conexion.cerrar()

    assert totales == {(1, date(2026, 1, 1)): (Decimal("10.00"), 2), (2, None): (None, 1)}
    assert sentencias and all(s.lstrip().upper().startswith("SELECT") for s in sentencias)
    prohibidas = ("INSERT", "UPDATE ", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REPLACE")
    assert not [s for s in sentencias if any(p in s.upper() for p in prohibidas)]
    assert opciones["ssl"], "debe activar TLS"
    assert opciones["database"] == "moneywise" and opciones["port"] == 3306
    assert "time_zone" in opciones["init_command"] and "+00:00" in opciones["init_command"]
    with pytest.raises(ValueError):
        conexion_ = fuente.FuenteRailway()
        conexion_.conteo("auth_tokens")  # la tabla de tokens no está en la lista permitida
