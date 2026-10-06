"""Coherencia de Gold: que las cuentas cuadren entre tablas y contra un recálculo desde Silver."""
import pytest

from calidad import (problemas_anomalias_vs_silver, problemas_de_anomalias, problemas_de_balance,
                     problemas_de_totales_de_gasto, problemas_de_variacion, problemas_gold_vs_silver)

GASTOS = [("gasto_por_destino_mensual", "destino_id"), ("gasto_por_tipo_mensual", "tipo_id")]


def verificar(problemas):
    assert not problemas, "\n" + "\n".join(problemas)


def test_balance_es_ingresos_menos_egresos_y_el_acumulado_es_la_suma_corrida(lago):
    verificar(problemas_de_balance(lago.gold["balance_mensual"]))


@pytest.mark.parametrize("tabla,col_id", GASTOS)
def test_el_gasto_desglosado_suma_lo_mismo_que_el_balance(lago, tabla, col_id):
    verificar(problemas_de_totales_de_gasto(lago.gold[tabla], lago.gold["balance_mensual"], tabla))


@pytest.mark.parametrize("tabla,col_id", GASTOS)
def test_la_variacion_contra_el_mes_anterior_esta_bien_calculada(lago, tabla, col_id):
    verificar(problemas_de_variacion(lago.gold[tabla], col_id))


def test_gold_coincide_con_un_recalculo_desde_silver(lago):
    """Segundo cálculo, en Python puro: cada fila de las 3 tablas de Gold debe salir igual."""
    verificar(problemas_gold_vs_silver(lago.gold, lago.silver))


def test_las_anomalias_cumplen_sus_reglas(lago):
    verificar(problemas_de_anomalias(lago.gold["anomalias"]))


def test_las_anomalias_coinciden_con_un_recalculo_desde_silver(lago):
    verificar(problemas_anomalias_vs_silver(lago.gold["anomalias"], lago.silver))
