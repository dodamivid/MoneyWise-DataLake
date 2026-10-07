"""Cuadre de BigQuery contra el lago (solo lectura).

No corren por defecto. Se usan después de que el DAG cargó Gold:   pytest -m bigquery
Cada tabla de BigQuery debe tener el contrato de Gold y ser idéntica, fila por fila, al Parquet del lago.
"""
import os

import pytest

from calidad import CONTRATO, problemas_de_esquema_bigquery, problemas_lago_vs_bigquery

pytestmark = pytest.mark.bigquery
TABLAS = list(CONTRATO)


def verificar(problemas):
    assert not problemas, (
        "\n" + "\n".join(problemas)
        + "\nSi el DAG no ha corrido desde el último cambio en Gold, córrelo (o corre cargar_bigquery.py) y repite."
    )


@pytest.fixture(scope="session")
def bigquery():
    if not os.getenv("BQ_DATASET"):
        pytest.skip("Falta BQ_DATASET en el .env")
    llave = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not os.getenv("BIGQUERY_EMULATOR_HOST") and not (llave and os.path.exists(llave)):
        pytest.skip("No encuentro la llave de la service account (GOOGLE_APPLICATION_CREDENTIALS en el .env)")
    import cargar_bigquery

    return cargar_bigquery.crear_cliente()


def _tabla(tabla):
    proyecto = os.getenv("GCP_PROJECT_ID")
    return f"{proyecto + '.' if proyecto else ''}{os.environ['BQ_DATASET']}.{tabla}"


@pytest.mark.parametrize("tabla", TABLAS)
def test_las_columnas_y_tipos_en_bigquery_son_los_del_contrato(bigquery, tabla):
    esquema = {campo.name: campo.field_type for campo in bigquery.get_table(_tabla(tabla)).schema}
    verificar(problemas_de_esquema_bigquery(tabla, esquema))


@pytest.mark.parametrize("tabla", TABLAS)
def test_la_tabla_de_bigquery_es_identica_al_lago(lago, bigquery, tabla):
    filas = [dict(fila.items()) for fila in bigquery.list_rows(_tabla(tabla))]
    verificar(problemas_lago_vs_bigquery(tabla, lago.gold[tabla], filas))
