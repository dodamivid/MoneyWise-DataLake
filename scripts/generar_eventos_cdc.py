"""Genera eventos CDC reales usando la API de MoneyWise (sin tocar la base de datos directamente).

Con un usuario de prueba hace, en este orden:
  1. crea 6 egresos de montos chicos      -> 6 eventos INSERT en Debezium
  2. cambia el monto de 3 de ellos        -> 3 eventos UPDATE
  3. elimina los 6                        -> 6 eventos DELETE (o UPDATE, si la API hace borrado lógico)
Cada cambio queda en MySQL (Railway), Debezium lo captura y llega a Bronze como evento real. Sirven para
medir el retraso de captura (Issue 15): el snapshot inicial no sirve, porque no son cambios en vivo.

Guarda en data/latencia/eventos_prueba.json el id de cada egreso y la hora de cada operación, para que
la medición use exactamente estos eventos y no el tráfico de otras personas.

Variables de entorno:
  MWI_API_KEY   (obligatoria) el valor de x-api-key: Railway -> Variables. No la escribas en ningún archivo.
  MWI_API_URL   (opcional) por defecto la API de producción.

Uso, desde la raíz del repo:
  set MWI_API_KEY=...        (cmd de Windows; solo vale para esa ventana)
  python scripts\\generar_eventos_cdc.py

Solo usa la librería estándar de Python.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

URL_POR_DEFECTO = "https://moneywise-integracion-production.up.railway.app"
RAIZ = Path(__file__).resolve().parents[1]
SALIDA = RAIZ / "data" / "latencia" / "eventos_prueba.json"

USUARIO = {
    "nombre": "Latencia",
    "apellidoP": "Prueba",
    "apellidoM": "Cdc",
    "correo": "latencia.prueba@moneywise.test",
    "fechaN": "2000-01-01",
    "contrasena": "LatenciaMW2026!",
}
SCOPES = "egresos:leer,egresos:escribir,catalogos:leer"
MONTOS = [5.00, 7.50, 12.25, 3.10, 9.99, 4.40]
MONTOS_NUEVOS = [6.00, 8.50, 13.25]  # para los 3 primeros
PAUSA = 1.0  # segundos entre operaciones: así las horas de cada evento se distinguen


def ahora():
    """Hora UTC con milisegundos, como ISO-8601 con Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class Api:
    def __init__(self, base, clave):
        self.base = base.rstrip("/")
        self.clave = clave

    def llamar(self, metodo, ruta, cuerpo=None, usuario_id=None):
        cabeceras = {"x-api-key": self.clave, "Content-Type": "application/json"}
        if usuario_id is not None:
            cabeceras["x-mw-user"] = str(usuario_id)
            cabeceras["x-mw-scopes"] = SCOPES
        datos = json.dumps(cuerpo).encode("utf-8") if cuerpo is not None else None
        peticion = urllib.request.Request(self.base + ruta, data=datos, headers=cabeceras, method=metodo)
        try:
            with urllib.request.urlopen(peticion, timeout=30) as r:
                return r.status, json.loads(r.read().decode("utf-8") or "null")
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode("utf-8") or "null")
            except ValueError:
                return e.code, None


def asegurar_usuario(api):
    """Registra al usuario de prueba; si ya existe, inicia sesión para obtener su id."""
    estado, cuerpo = api.llamar("POST", "/api/v1/auth/registro", USUARIO)
    if estado in (200, 201) and cuerpo and cuerpo.get("ok"):
        return cuerpo["data"]["usuarioId"], True
    estado, cuerpo = api.llamar(
        "POST", "/api/v1/auth/acceso", {"correo": USUARIO["correo"], "contrasena": USUARIO["contrasena"]}
    )
    if estado == 200 and cuerpo and cuerpo.get("ok"):
        return cuerpo["data"]["usuario"]["usuarioId"], False
    sys.exit(f"No pude registrar ni iniciar sesión con el usuario de prueba (HTTP {estado}): {cuerpo}")


def primer_tipo_egreso(api, usuario_id):
    estado, cuerpo = api.llamar("GET", "/api/v1/catalogos/tipos-egreso?tamanoPagina=50", usuario_id=usuario_id)
    if estado != 200 or not cuerpo or not cuerpo.get("data"):
        sys.exit(f"No pude leer los tipos de egreso (HTTP {estado}): {cuerpo}")
    return cuerpo["data"][0]["tipoEgresoId"]


def registrar(operaciones, op, egreso_id, enviado, estado):
    operaciones.append({"op": op, "egresoId": egreso_id, "enviado": enviado, "respondido": ahora(), "http": estado})
    print(f"  {op:<10} egreso {egreso_id}  HTTP {estado}")


def main():
    sys.stdout.reconfigure(errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--si", action="store_true", help="no pedir confirmación")
    args = ap.parse_args()

    clave = os.getenv("MWI_API_KEY")
    if not clave:
        sys.exit("Falta la variable MWI_API_KEY (el valor de x-api-key, en Railway -> Variables).")
    base = os.getenv("MWI_API_URL", URL_POR_DEFECTO)

    print(f"API: {base}")
    print("Esto va a CREAR, EDITAR y ELIMINAR 6 egresos de un usuario de prueba en la base real (vía la API).")
    if not args.si and input("Escribe SI para continuar: ").strip() != "SI":
        sys.exit("Cancelado: no se escribió nada.")

    api = Api(base, clave)
    usuario_id, nuevo = asegurar_usuario(api)
    print(f"Usuario de prueba: id {usuario_id} ({'creado ahora' if nuevo else 'ya existía'})")
    tipo_id = primer_tipo_egreso(api, usuario_id)

    operaciones, creados = [], []
    try:
        print("Creando egresos...")
        for i, monto in enumerate(MONTOS, start=1):
            enviado = ahora()
            estado, cuerpo = api.llamar(
                "POST", "/api/v1/egresos",
                {"tipoId": tipo_id, "monto": monto, "descripcion": f"prueba latencia {i}", "fechaInicio": enviado[:19] + "Z"},
                usuario_id,
            )
            if estado != 201 or not cuerpo or not cuerpo.get("ok"):
                sys.exit(f"Falló la creación del egreso {i} (HTTP {estado}): {cuerpo}")
            egreso_id = cuerpo["data"]["egresoId"]
            creados.append(egreso_id)
            registrar(operaciones, "crear", egreso_id, enviado, estado)
            time.sleep(PAUSA)

        print("Editando 3 egresos...")
        for egreso_id, monto in zip(creados[:3], MONTOS_NUEVOS):
            enviado = ahora()
            estado, cuerpo = api.llamar("PATCH", f"/api/v1/egresos/{egreso_id}", {"monto": monto}, usuario_id)
            if estado != 200:
                sys.exit(f"Falló la edición del egreso {egreso_id} (HTTP {estado}): {cuerpo}")
            registrar(operaciones, "actualizar", egreso_id, enviado, estado)
            time.sleep(PAUSA)
    finally:
        # Pase lo que pase, no dejar egresos de prueba vivos en la base.
        print("Eliminando los egresos de prueba...")
        for egreso_id in creados:
            enviado = ahora()
            estado, _ = api.llamar("DELETE", f"/api/v1/egresos/{egreso_id}", usuario_id=usuario_id)
            registrar(operaciones, "eliminar", egreso_id, enviado, estado)
            time.sleep(PAUSA)

        SALIDA.parent.mkdir(parents=True, exist_ok=True)
        SALIDA.write_text(
            json.dumps({"api": base, "usuarioId": usuario_id, "operaciones": operaciones}, indent=2),
            encoding="utf-8",
        )
        print(f"\nGuardado en {SALIDA}")

    print("Listo. Los eventos tardan unos segundos en llegar a Kafka; la medición se hace después.")


if __name__ == "__main__":
    main()
