"""Genera los secretos de Airflow y los escribe en .env, sin mostrarlos.

  python scripts/generar_secretos.py

- Si .env no existe, lo crea a partir de .env.example.
- Solo llena los secretos que están vacíos o que faltan: lo que ya tiene valor no se toca,
  así que se puede correr cuantas veces haga falta sin cambiar contraseñas existentes.
- Nunca imprime los valores; solo dice cuáles generó.
Solo usa la biblioteca estándar de Python.
"""
import base64
import os
import re
import secrets
import shutil
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
ENV = RAIZ / ".env"
PLANTILLA = RAIZ / ".env.example"

# nombre -> cómo se genera
SECRETOS = {
    "AIRFLOW_FERNET_KEY": lambda: base64.urlsafe_b64encode(os.urandom(32)).decode(),  # clave Fernet válida: 32 bytes en base64
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_hex(32),
    "AIRFLOW_DB_PASSWORD": lambda: secrets.token_hex(16),  # solo hexadecimal: va dentro de una URL de conexión
    "AIRFLOW_ADMIN_PASSWORD": lambda: secrets.token_urlsafe(12),
}


def llenar(texto, nombre, valor):
    """Devuelve (texto nuevo, True) si puso el valor; (texto, False) si ya tenía uno."""
    patron = re.compile(rf"^{re.escape(nombre)}=(.*)$", re.M)
    existente = patron.search(texto)
    if existente is None:  # la clave no está en el archivo: se agrega al final
        return texto.rstrip("\n") + f"\n{nombre}={valor}\n", True
    if existente.group(1).strip():
        return texto, False
    return patron.sub(lambda _: f"{nombre}={valor}", texto, count=1), True


def main():
    if not ENV.exists():
        if not PLANTILLA.exists():
            print("No existe .env ni .env.example: corre este script desde un clon completo del repo.", file=sys.stderr)
            return 1
        shutil.copy(PLANTILLA, ENV)
        print("Creé .env a partir de .env.example.")
    texto = ENV.read_text(encoding="utf-8")
    generados, omitidos = [], []
    for nombre, generar in SECRETOS.items():
        texto, puesto = llenar(texto, nombre, generar())
        (generados if puesto else omitidos).append(nombre)
    ENV.write_text(texto, encoding="utf-8")
    for nombre in generados:
        print(f"  generado: {nombre}")
    for nombre in omitidos:
        print(f"  ya tenía valor, no se tocó: {nombre}")
    if "AIRFLOW_ADMIN_PASSWORD" in generados:
        print("La contraseña de la interfaz de Airflow (usuario admin) está en .env, en AIRFLOW_ADMIN_PASSWORD.")
    return 0


if __name__ == "__main__":
    sys.exit(main())