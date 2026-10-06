"""Contrato de Gold: columnas y tipos exactos, sin nulos en los campos clave, sin llaves repetidas."""
import pytest

from calidad import (CONTRATO, LLAVES, NO_NULOS, problemas_de_contrato, problemas_de_duplicados,
                     problemas_de_meses, problemas_de_negativos, problemas_de_nulos)

TABLAS = list(CONTRATO)
CON_MES = ["balance_mensual", "gasto_por_destino_mensual", "gasto_por_tipo_mensual"]
CONTEOS_Y_TOTALES = {
    "balance_mensual": ["total_ingresos", "total_egresos", "n_ingresos", "n_egresos"],
    "gasto_por_destino_mensual": ["total_egresos", "n_egresos"],
    "gasto_por_tipo_mensual": ["total_egresos", "n_egresos"],
}


def verificar(problemas):
    assert not problemas, "\n" + "\n".join(problemas)


@pytest.mark.parametrize("tabla", TABLAS)
def test_columnas_y_tipos_del_contrato(lago, tabla):
    verificar(problemas_de_contrato(lago.esquemas[tabla], CONTRATO[tabla]))


@pytest.mark.parametrize("tabla", CON_MES)
def test_la_tabla_tiene_filas(lago, tabla):
    assert lago.gold[tabla], f"{tabla} está vacía"


@pytest.mark.parametrize("tabla", TABLAS)
def test_sin_nulos_en_campos_clave(lago, tabla):
    verificar(problemas_de_nulos(lago.gold[tabla], NO_NULOS[tabla]))


@pytest.mark.parametrize("tabla", TABLAS)
def test_sin_llaves_repetidas(lago, tabla):
    verificar(problemas_de_duplicados(lago.gold[tabla], LLAVES[tabla]))


@pytest.mark.parametrize("tabla", CON_MES)
def test_el_mes_siempre_es_dia_uno(lago, tabla):
    verificar(problemas_de_meses(lago.gold[tabla]))


@pytest.mark.parametrize("tabla", CON_MES)
def test_sin_totales_ni_conteos_negativos(lago, tabla):
    verificar(problemas_de_negativos(lago.gold[tabla], CONTEOS_Y_TOTALES[tabla]))
