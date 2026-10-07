"""Carga la capa Gold de Cloud Storage a tablas nativas de BigQuery (Issue 13, parte 1).

Para cada tabla de Gold lanza un trabajo de carga desde gs://<bucket>/gold/<tabla>/data.parquet hacia
<proyecto>.<dataset>.<tabla> con WRITE_TRUNCATE: la tabla se reemplaza completa en una sola actualización
atómica, así que un dashboard nunca ve una tabla a medias. Los decimales de Parquet se fuerzan a NUMERIC:
si alguno no cupiera, la carga FALLA en lugar de cambiar de tipo en silencio.

Uso:
  python gcp/cargar_bigquery.py --dry-run   # muestra qué cargaría; no toca la nube ni pide credenciales
  python gcp/cargar_bigquery.py             # carga y verifica

Variables de entorno:
  GCS_BUCKET                      bucket donde subir_gold.py dejó Gold (si falta, termina con código 99)
  BQ_DATASET                      dataset de destino, p. ej. moneywise_gold (si falta, termina con código 99)
  BQ_LOCATION                     región del dataset (debe ser la del bucket, p. ej. us-central1); opcional
  GCP_PROJECT_ID                  proyecto; opcional (si falta, el de las credenciales)
  GOOGLE_APPLICATION_CREDENTIALS  ruta al JSON de la service account (nunca va escrito en el código)

Códigos de salida: 0 todo bien · 1 error · 99 no está configurado (Airflow lo marca "skipped").
"""
import argparse
import os
import re
import sys
import time

PREFIJO = "gold"
NOMBRE_OBJETO = "data.parquet"
SALIDA_NO_CONFIGURADO = 99

# Columnas que debe tener cada tabla en BigQuery (el contrato completo, con tipos, lo verifica pytest).
TABLAS = {
    "balance_mensual": ["usuario_id", "mes", "total_ingresos", "total_egresos", "balance",
                        "n_ingresos", "n_egresos", "balance_acumulado"],
    "gasto_por_destino_mensual": ["usuario_id", "mes", "destino_id", "destino", "total_egresos",
                                  "n_egresos", "total_mes_anterior", "variacion_pct"],
    "gasto_por_tipo_mensual": ["usuario_id", "mes", "tipo_id", "tipo", "total_egresos",
                               "n_egresos", "total_mes_anterior", "variacion_pct"],
    "anomalias": ["egreso_id", "usuario_id", "destino_id", "destino", "fecha", "monto", "n_grupo", "q1", "q3",
                  "iqr", "limite_atipico", "limite_extremo", "exceso_sobre_limite", "severidad"],
}
DATASET_VALIDO = re.compile(r"^[A-Za-z0-9_]{1,1024}$")


class ErrorDeCarga(Exception):
    pass


def plan_de_carga(bucket, proyecto, dataset):
    destino = f"{proyecto}.{dataset}" if proyecto else dataset
    return [
        {"tabla": tabla, "columnas": columnas,
         "origen": f"gs://{bucket}/{PREFIJO}/{tabla}/{NOMBRE_OBJETO}", "destino": f"{destino}.{tabla}"}
        for tabla, columnas in TABLAS.items()
    ]


def config_de_carga():
    from google.cloud import bigquery  # se importa aquí para que --dry-run no necesite la librería

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        decimal_target_types=[bigquery.DecimalTargetType.NUMERIC],
    )


def cargar_y_verificar(cliente, plan, ubicacion=None):
    for item in plan:
        inicio = time.monotonic()
        trabajo = cliente.load_table_from_uri(item["origen"], item["destino"], job_config=config_de_carga(),
                                              location=ubicacion)
        trabajo.result()  # espera a que termine; lanza un error si BigQuery rechazó la carga
        tabla = cliente.get_table(item["destino"])
        columnas = [campo.name for campo in tabla.schema]
        if sorted(columnas) != sorted(item["columnas"]):
            raise ErrorDeCarga(
                f"{item['destino']}: las columnas cargadas no son las esperadas "
                f"(faltan {sorted(set(item['columnas']) - set(columnas))}, "
                f"sobran {sorted(set(columnas) - set(item['columnas']))})"
            )
        if tabla.num_rows != trabajo.output_rows:
            raise ErrorDeCarga(
                f"{item['destino']}: la tabla tiene {tabla.num_rows} filas y el trabajo de carga informó {trabajo.output_rows}"
            )
        print(f"OK    {item['destino']}  {tabla.num_rows} filas  ({time.monotonic() - inicio:.1f} s)")


def crear_cliente():
    from google.cloud import bigquery

    proyecto = os.getenv("GCP_PROJECT_ID") or None
    if os.getenv("BIGQUERY_EMULATOR_HOST"):  # emulador local: no hay credenciales reales
        from google.auth.credentials import AnonymousCredentials

        return bigquery.Client(project=proyecto or "proyecto-local", credentials=AnonymousCredentials())
    return bigquery.Client(project=proyecto)


PISTAS = {
    "DefaultCredentialsError": "No se encontraron credenciales válidas. Revisa que GOOGLE_APPLICATION_CREDENTIALS "
                               "apunte al JSON de la service account y que ese archivo exista.",
    "RefreshError": "Google rechazó la llave de la service account (¿fue eliminada, o el archivo está dañado?).",
    "NotFound": "No existe el dataset (o el archivo en Cloud Storage). Revisa BQ_DATASET, que el dataset esté en la "
                "misma región que el bucket, y que subir_gold.py ya haya publicado Gold.",
    "Forbidden": "La cuenta no tiene permiso. Necesita 'BigQuery Job User' en el proyecto y 'BigQuery Data Editor' "
                 "sobre el dataset, además de leer el bucket.",
    "Unauthorized": "Las credenciales no fueron aceptadas por Google Cloud.",
    "BadRequest": "BigQuery rechazó la carga (archivo, tipos o ubicación). El mensaje de arriba dice por qué.",
}


def explicar(error):
    pista = PISTAS.get(type(error).__name__)
    return f"{type(error).__name__}: {error}" + (f"\n      -> {pista}" if pista else "")


def main(argv=None, cliente=None):
    parser = argparse.ArgumentParser(description="Carga la capa Gold de Cloud Storage a BigQuery.")
    parser.add_argument("--dry-run", action="store_true", help="solo muestra qué se cargaría")
    args = parser.parse_args(argv)
    dry_run = args.dry_run or os.getenv("BQ_DRY_RUN") == "1"
    bucket = os.getenv("GCS_BUCKET", "").strip()
    dataset = os.getenv("BQ_DATASET", "").strip()
    proyecto = os.getenv("GCP_PROJECT_ID", "").strip()
    ubicacion = os.getenv("BQ_LOCATION", "").strip() or None

    if dataset and not DATASET_VALIDO.match(dataset):
        print(f"ERROR BQ_DATASET='{dataset}' no es válido: solo letras, números y guiones bajos (sin guiones).", file=sys.stderr)
        return 1

    plan = plan_de_carga(bucket or "<GCS_BUCKET>", proyecto, dataset or "<BQ_DATASET>")
    print(f"Destino: {proyecto + '.' if proyecto else ''}{dataset or '<BQ_DATASET>'}" + (f"  (ubicación {ubicacion})" if ubicacion else ""))
    for item in plan:
        print(f"  {item['origen']}\n      -> {item['destino']}")

    if dry_run:
        print("Dry-run: no se cargó nada.")
        return 0
    if not bucket or not dataset:
        print("GCS_BUCKET o BQ_DATASET no están configurados: se omite la carga a BigQuery.")
        return SALIDA_NO_CONFIGURADO

    try:
        cliente = cliente or crear_cliente()
        cargar_y_verificar(cliente, plan, ubicacion)
    except ErrorDeCarga as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 1
    except Exception as error:  # errores de red, permisos, credenciales, rechazo de BigQuery...
        print(f"ERROR {explicar(error)}", file=sys.stderr)
        return 1
    print(f"Gold cargado en BigQuery: {proyecto + '.' if proyecto else ''}{dataset} ({len(plan)} tablas).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
