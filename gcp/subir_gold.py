"""Sube la capa Gold a Cloud Storage (Issue 10).

Cada tabla de Gold es una carpeta de Spark con UN archivo Parquet cuyo nombre lleva un código
aleatorio distinto en cada corrida. Aquí se sube con un nombre fijo, gold/<tabla>/data.parquet:
cada corrida sobrescribe la anterior, el bucket no acumula versiones viejas y quien lea los datos
nunca ve un archivo a medias (sobrescribir un objeto es atómico).

Uso:
  python gcp/subir_gold.py --dry-run   # muestra qué subiría; no toca la nube ni pide credenciales
  python gcp/subir_gold.py             # sube, verifica tamaño y MD5, y publica el manifiesto

Variables de entorno:
  GCS_BUCKET                      nombre del bucket (si falta, termina con código 99 = "omitir")
  GCP_PROJECT_ID                  opcional
  GOOGLE_APPLICATION_CREDENTIALS  ruta al JSON de la service account (nunca va escrito en el código)
  GOLD_DIR                        carpeta de Gold (por defecto <repo>/data/gold)

Códigos de salida: 0 todo bien · 1 error · 99 GCS_BUCKET no configurado (Airflow lo marca "skipped").
"""
import argparse
import base64
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

TABLAS = ["balance_mensual", "gasto_por_destino_mensual", "gasto_por_tipo_mensual", "anomalias"]
PREFIJO = "gold"
NOMBRE_OBJETO = "data.parquet"
TIPO_PARQUET = "application/vnd.apache.parquet"
SALIDA_NO_CONFIGURADO = 99

RAIZ = Path(__file__).resolve().parent.parent
GOLD_DIR = Path(os.getenv("GOLD_DIR", str(RAIZ / "data" / "gold")))


class ErrorDeSubida(Exception):
    pass


def archivo_de_tabla(carpeta):
    """El único .parquet de la carpeta de una tabla (ignora _SUCCESS y los .crc ocultos de Hadoop)."""
    carpeta = Path(carpeta)
    if not carpeta.is_dir():
        raise ErrorDeSubida(f"No existe la carpeta {carpeta}: ¿ya corrió el job de Gold?")
    archivos = sorted(p for p in carpeta.glob("*.parquet") if not p.name.startswith("."))
    if len(archivos) != 1:
        raise ErrorDeSubida(
            f"{carpeta.name}: se esperaba 1 archivo .parquet y hay {len(archivos)}: {[a.name for a in archivos]}"
        )
    if archivos[0].stat().st_size == 0:
        raise ErrorDeSubida(f"{carpeta.name}: el archivo {archivos[0].name} está vacío")
    return archivos[0]


def huella_md5(ruta):
    """MD5 en base64, el mismo formato con el que Cloud Storage informa la huella de un objeto."""
    md5 = hashlib.md5()
    with Path(ruta).open("rb") as f:
        for bloque in iter(lambda: f.read(1024 * 1024), b""):
            md5.update(bloque)
    return base64.b64encode(md5.digest()).decode()


def plan_de_subida(gold_dir):
    plan = []
    for tabla in TABLAS:
        origen = archivo_de_tabla(Path(gold_dir) / tabla)
        plan.append({
            "tabla": tabla,
            "origen": origen,
            "objeto": f"{PREFIJO}/{tabla}/{NOMBRE_OBJETO}",
            "bytes": origen.stat().st_size,
            "md5": huella_md5(origen),
        })
    return plan


def subir_y_verificar(bucket, plan):
    for item in plan:
        blob = bucket.blob(item["objeto"])
        blob.upload_from_filename(str(item["origen"]), content_type=TIPO_PARQUET)
        blob.reload()
        if blob.size != item["bytes"] or blob.md5_hash != item["md5"]:
            raise ErrorDeSubida(
                f"{item['objeto']}: lo que quedó en la nube no coincide con el archivo local "
                f"(bytes {blob.size} vs {item['bytes']}, md5 {blob.md5_hash} vs {item['md5']})"
            )
        print(f"OK    {item['objeto']}  {item['bytes']} bytes  md5={item['md5']}")
    manifiesto = {
        "generado_en_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tablas": {i["tabla"]: {"objeto": i["objeto"], "bytes": i["bytes"], "md5_base64": i["md5"]} for i in plan},
    }
    # El manifiesto se publica al final: si existe, todas las tablas ya están arriba y verificadas.
    bucket.blob(f"{PREFIJO}/_manifest.json").upload_from_string(
        json.dumps(manifiesto, indent=2, ensure_ascii=False), content_type="application/json"
    )
    print(f"OK    {PREFIJO}/_manifest.json  (generado {manifiesto['generado_en_utc']} UTC)")


def crear_cliente():
    from google.cloud import storage  # se importa aquí para que --dry-run no necesite la librería

    return storage.Client(project=os.getenv("GCP_PROJECT_ID") or None)


PISTAS = {
    "DefaultCredentialsError": "No se encontraron credenciales válidas. Revisa que GOOGLE_APPLICATION_CREDENTIALS "
                               "apunte al JSON de la service account y que ese archivo exista.",
    "RefreshError": "Google rechazó la llave de la service account (¿fue eliminada, o el archivo está dañado?).",
    "NotFound": "El bucket no existe o su nombre está mal escrito. Revisa GCS_BUCKET.",
    "Forbidden": "La cuenta no tiene permiso en ese bucket. Debe tener el rol Storage Object Admin sobre el bucket.",
    "Unauthorized": "Las credenciales no fueron aceptadas por Google Cloud.",
}


def explicar(error):
    pista = PISTAS.get(type(error).__name__)
    return f"{type(error).__name__}: {error}" + (f"\n      -> {pista}" if pista else "")


def main(argv=None, cliente=None):
    parser = argparse.ArgumentParser(description="Sube la capa Gold a Cloud Storage.")
    parser.add_argument("--dry-run", action="store_true", help="solo muestra qué se subiría")
    args = parser.parse_args(argv)
    dry_run = args.dry_run or os.getenv("GCS_DRY_RUN") == "1"
    bucket_nombre = os.getenv("GCS_BUCKET", "").strip()

    try:
        plan = plan_de_subida(GOLD_DIR)
    except ErrorDeSubida as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 1

    print(f"Gold local: {GOLD_DIR}")
    print(f"Destino:    gs://{bucket_nombre or '<GCS_BUCKET>'}/{PREFIJO}/")
    for item in plan:
        print(f"  {item['tabla']:<28} {item['origen'].name[:34]:<34} {item['bytes']:>8} bytes -> {item['objeto']}")

    if dry_run:
        print("Dry-run: no se subió nada.")
        return 0
    if not bucket_nombre:
        print("GCS_BUCKET no está configurado: se omite la subida a la nube.")
        return SALIDA_NO_CONFIGURADO

    try:
        cliente = cliente or crear_cliente()
        subir_y_verificar(cliente.bucket(bucket_nombre), plan)
    except ErrorDeSubida as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 1
    except Exception as error:  # errores de red, permisos, credenciales...
        print(f"ERROR {explicar(error)}", file=sys.stderr)
        return 1
    print(f"Gold publicado en gs://{bucket_nombre}/{PREFIJO}/ ({len(plan)} tablas).")
    return 0


if __name__ == "__main__":
    sys.exit(main())