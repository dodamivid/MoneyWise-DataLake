"""Pruebas del script gcp/cargar_bigquery.py con un cliente de BigQuery falso (sin red ni credenciales)."""
import types

import pytest

pytest.importorskip("google.cloud.bigquery")
import cargar_bigquery as cb  # noqa: E402  (gcp/ está en el path por conftest.py)

AMBIENTE = {"GCS_BUCKET": "mi-bucket", "BQ_DATASET": "moneywise_gold", "GCP_PROJECT_ID": "mi-proyecto", "BQ_LOCATION": "us-central1"}
TABLAS = list(cb.TABLAS)


class TrabajoFalso:
    def __init__(self, filas, error=None):
        self.output_rows, self._error = filas, error

    def result(self):
        if self._error:
            raise self._error
        return self


class ClienteFalso:
    """Registra lo que se le pide y responde como BigQuery (con opciones para fallar)."""

    def __init__(self, columnas=None, filas_tabla=None, filas_trabajo=10, error_en=None):
        self.cargas, self.columnas, self.filas_tabla = [], columnas or {}, filas_tabla
        self.filas_trabajo, self.error_en = filas_trabajo, error_en

    def load_table_from_uri(self, origen, destino, job_config=None, location=None):
        self.cargas.append({"origen": origen, "destino": destino, "config": job_config, "ubicacion": location})
        return TrabajoFalso(self.filas_trabajo, self.error_en if len(self.cargas) == 1 else None)

    def get_table(self, destino):
        tabla = destino.split(".")[-1]
        nombres = self.columnas.get(tabla, cb.TABLAS[tabla])
        n = self.filas_trabajo if self.filas_tabla is None else self.filas_tabla
        return types.SimpleNamespace(schema=[types.SimpleNamespace(name=c) for c in nombres], num_rows=n)


@pytest.fixture
def ambiente(monkeypatch):
    for variable in ("GCS_BUCKET", "BQ_DATASET", "GCP_PROJECT_ID", "BQ_LOCATION", "BQ_DRY_RUN"):
        monkeypatch.delenv(variable, raising=False)
    for variable, valor in AMBIENTE.items():
        monkeypatch.setenv(variable, valor)


def sin_cliente(monkeypatch):
    def prohibido():
        raise AssertionError("no debía crear un cliente")

    monkeypatch.setattr(cb, "crear_cliente", prohibido)


def test_dry_run_muestra_el_plan_y_no_crea_cliente(ambiente, monkeypatch, capsys):
    sin_cliente(monkeypatch)
    assert cb.main(["--dry-run"]) == 0
    salida = capsys.readouterr().out
    assert "gs://mi-bucket/gold/balance_mensual/data.parquet" in salida
    assert "mi-proyecto.moneywise_gold.anomalias" in salida and "Dry-run: no se cargó nada" in salida


@pytest.mark.parametrize("falta", ["GCS_BUCKET", "BQ_DATASET"])
def test_sin_configurar_sale_con_99_para_que_airflow_lo_omita(ambiente, monkeypatch, falta):
    sin_cliente(monkeypatch)
    monkeypatch.setenv(falta, "")
    assert cb.main([]) == cb.SALIDA_NO_CONFIGURADO == 99


def test_un_dataset_con_guiones_se_rechaza_antes_de_tocar_la_nube(ambiente, monkeypatch, capsys):
    sin_cliente(monkeypatch)
    monkeypatch.setenv("BQ_DATASET", "moneywise-gold")
    assert cb.main([]) == 1
    assert "no es válido" in capsys.readouterr().err


def test_carga_las_4_tablas_con_la_configuracion_correcta(ambiente):
    cliente = ClienteFalso()
    assert cb.main([], cliente=cliente) == 0
    assert [c["origen"] for c in cliente.cargas] == [f"gs://mi-bucket/gold/{t}/data.parquet" for t in TABLAS]
    assert [c["destino"] for c in cliente.cargas] == [f"mi-proyecto.moneywise_gold.{t}" for t in TABLAS]
    for carga in cliente.cargas:
        assert carga["config"].source_format == "PARQUET"
        assert carga["config"].write_disposition == "WRITE_TRUNCATE"  # reemplaza la tabla completa, de forma atómica
        assert list(carga["config"].decimal_target_types) == ["NUMERIC"]  # que ningún decimal cambie de tipo en silencio
        assert carga["ubicacion"] == "us-central1"


def test_sin_proyecto_usa_el_del_cliente(ambiente, monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "")
    cliente = ClienteFalso()
    assert cb.main([], cliente=cliente) == 0
    assert cliente.cargas[0]["destino"] == "moneywise_gold.balance_mensual"


def test_si_las_columnas_no_son_las_esperadas_falla_y_dice_cuales(ambiente, capsys):
    cliente = ClienteFalso(columnas={"balance_mensual": cb.TABLAS["balance_mensual"][:-1] + ["otra"]})
    assert cb.main([], cliente=cliente) == 1
    error = capsys.readouterr().err
    assert "balance_acumulado" in error and "otra" in error


def test_si_las_filas_de_la_tabla_no_son_las_que_informo_el_trabajo_falla(ambiente, capsys):
    assert cb.main([], cliente=ClienteFalso(filas_trabajo=10, filas_tabla=11)) == 1
    assert "informó" in capsys.readouterr().err


def test_un_rechazo_de_bigquery_se_explica_y_no_sigue_con_las_demas_tablas(ambiente, capsys):
    BadRequest = type("BadRequest", (Exception,), {})
    cliente = ClienteFalso(error_en=BadRequest("Parquet corrupto"))
    assert cb.main([], cliente=cliente) == 1
    assert len(cliente.cargas) == 1  # no deja el almacén con unas tablas nuevas y otras viejas
    error = capsys.readouterr().err
    assert "BadRequest" in error and "Parquet corrupto" in error and "rechazó" in error


@pytest.mark.parametrize("nombre,pista", [("NotFound", "BQ_DATASET"), ("Forbidden", "BigQuery Data Editor"),
                                          ("DefaultCredentialsError", "GOOGLE_APPLICATION_CREDENTIALS")])
def test_los_errores_comunes_traen_una_pista_concreta(ambiente, capsys, nombre, pista):
    cliente = ClienteFalso(error_en=type(nombre, (Exception,), {})("detalle"))
    assert cb.main([], cliente=cliente) == 1
    assert pista in capsys.readouterr().err


def test_no_imprime_credenciales(ambiente, monkeypatch, capsys):
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/ruta/secreta/llave.json")
    cb.main([], cliente=ClienteFalso())
    captura = capsys.readouterr()
    assert "llave.json" not in captura.out + captura.err and "private_key" not in captura.out + captura.err
