"""Cuadre del lago contra la base REAL en Railway (solo lectura).

No corren por defecto. Se usan justo después de correr el pipeline:   pytest -m fuente
Comparan contra la base EN VIVO: si alguien escribió desde la última corrida del pipeline habrá diferencias
que no son un error del lago (corre el pipeline otra vez y repite).
"""
import pytest

from calidad import problemas_conteos_vs_fuente, problemas_totales_vs_fuente
from fuente import CON_MONTO_Y_FECHA, TABLAS

pytestmark = pytest.mark.fuente


def verificar(problemas):
    assert not problemas, (
        "\n" + "\n".join(problemas)
        + "\nSi hubo cambios en la app desde la última corrida del pipeline, córrelo otra vez y repite."
    )


@pytest.mark.parametrize("tabla", TABLAS)
def test_las_filas_de_cada_tabla_coinciden_con_la_fuente(lago, fuente, tabla):
    if tabla not in lago.silver:
        pytest.skip(f"Silver no tiene la tabla {tabla}")
    verificar(problemas_conteos_vs_fuente({tabla: len(lago.silver[tabla])}, {tabla: fuente.conteo(tabla)}))


def test_los_totales_por_usuario_y_mes_coinciden_con_la_fuente(lago, fuente):
    ingresos, egresos = (fuente.totales_por_mes(t) for t in CON_MONTO_Y_FECHA)
    verificar(problemas_totales_vs_fuente(lago.gold["balance_mensual"], ingresos, egresos))
