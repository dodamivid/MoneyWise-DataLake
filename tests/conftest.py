"""Configuración de pytest: de dónde salen los datos y cómo se cargan.

  pytest                      lee los Parquet reales de data/ (corre el pipeline antes)
  pytest --datos=sintetico    usa un lago inventado, sin datos reales (es lo que corre el CI)
  pytest -m fuente            además compara contra la base real en Railway (solo lectura)
"""
import os
from dataclasses import dataclass
from pathlib import Path

import pytest

import datos_sinteticos
from calidad import CONTRATO

RAIZ = Path(__file__).resolve().parent.parent
TABLAS_SILVER = ("usuarios", "destinos", "procedencias", "tipos_egreso", "tipos_ingreso", "frecuencias",
                 "ingresos", "egresos", "inversiones", "metas", "fechas_corte_ahorro")
SILVER_NECESARIAS = ("ingresos", "egresos", "destinos", "tipos_egreso")


def _cargar_env():
    """Lee RAILWAY_MYSQL_* del .env (sin pisar lo que ya esté en el entorno)."""
    ruta = RAIZ / ".env"
    if not ruta.exists():
        return
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            clave, _, valor = linea.partition("=")
            os.environ.setdefault(clave.strip(), valor.strip().strip('"').strip("'"))


_cargar_env()


def pytest_addoption(parser):
    parser.addoption("--datos", choices=("real", "sintetico"), default="real",
                     help="real: Parquet de data/; sintetico: un lago inventado (para el CI)")
    parser.addoption("--datos-dir", default=None,
                     help="carpeta con silver/ y gold/ (por defecto data/ del repo, o la variable DATOS_DIR)")


def pytest_report_header(config):
    if config.getoption("--datos") == "sintetico":
        return "datos: sintéticos (lago inventado)"
    return f"datos: reales ({_carpeta_de_datos(config)})"


def _carpeta_de_datos(config):
    return Path(config.getoption("--datos-dir") or os.getenv("DATOS_DIR") or RAIZ / "data")


@dataclass(repr=False)
class Lago:
    gold: dict
    silver: dict
    esquemas: dict  # esquemas de Gold: {tabla: {columna: tipo}}

    def __repr__(self):  # corto: si no, pytest imprime todos los datos al fallar un test
        return f"Lago(gold={list(self.gold)}, silver={list(self.silver)})"


def leer_parquet(ruta):
    """Lee una carpeta de Parquet (ignora _SUCCESS y .crc). Devuelve (filas como diccionarios, esquema)."""
    import pyarrow as pa
    import pyarrow.dataset as ds

    tabla = ds.dataset(str(ruta), format="parquet").to_table()
    esquema = {campo.name: str(campo.type) for campo in tabla.schema}
    columnas = []
    for campo in tabla.schema:
        columna = tabla[campo.name]
        if pa.types.is_timestamp(campo.type) and campo.type.unit == "ns":  # Spark los guarda en nanosegundos
            columna = columna.cast(pa.timestamp("us", tz=campo.type.tz), safe=False)
        columnas.append(columna)
    return pa.table(columnas, names=tabla.schema.names).to_pylist(), esquema


@pytest.fixture(scope="session")
def lago(request, tmp_path_factory):
    if request.config.getoption("--datos") == "sintetico":
        base = tmp_path_factory.mktemp("lago")
        silver, gold = datos_sinteticos.construir()
        datos_sinteticos.escribir_lago(base, silver, gold)  # pasa por disco, igual que los datos reales
    else:
        base = _carpeta_de_datos(request.config)
    faltan = [f"gold/{t}" for t in CONTRATO if not (base / "gold" / t).is_dir()]
    faltan += [f"silver/{t}" for t in SILVER_NECESARIAS if not (base / "silver" / t).is_dir()]
    if faltan:
        pytest.skip(f"No hay datos en {base} (faltan {len(faltan)} carpetas, p. ej. {faltan[0]}). Corre el pipeline o usa --datos=sintetico.")
    gold, esquemas = {}, {}
    for tabla in CONTRATO:
        gold[tabla], esquemas[tabla] = leer_parquet(base / "gold" / tabla)
    silver = {t: leer_parquet(base / "silver" / t)[0] for t in TABLAS_SILVER if (base / "silver" / t).is_dir()}
    return Lago(gold=gold, silver=silver, esquemas=esquemas)


@pytest.fixture(scope="session")
def fuente():
    from fuente import FuenteRailway, faltan_variables

    faltan = faltan_variables()
    if faltan:
        pytest.skip(f"Faltan en el .env: {', '.join(faltan)}")
    conexion = FuenteRailway()
    yield conexion
    conexion.cerrar()
