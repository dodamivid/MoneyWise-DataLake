"""Consumer de verificación del CDC (Issue 4).

Lee los topics que publica Debezium y muestra, en una línea por evento,
qué pasó en MySQL: operación, tabla, id de la fila y campos que cambiaron.
"""
import argparse
import json
import os
import sys
from collections import Counter

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "moneywise")
DB = "moneywise"
GROUP_ID = "moneywise-verify"

# Lo que significa el campo "op" de un evento de Debezium
OPERACIONES = {"r": "SNAPSHOT", "c": "INSERT", "u": "UPDATE", "d": "DELETE"}


def describir(payload):
    """Convierte el payload de un evento en (operación, tabla, texto)."""
    op = OPERACIONES.get(payload["op"], payload["op"])
    tabla = payload["source"]["table"]
    antes = payload.get("before")
    despues = payload.get("after")

    fila = despues or antes or {}
    id_fila = fila.get("id", "?")

    detalle = ""
    if payload["op"] == "u" and antes and despues:
        cambios = {
            campo: f"{antes.get(campo)} -> {valor}"
            for campo, valor in despues.items()
            if antes.get(campo) != valor
        }
        detalle = str(cambios)
    elif payload["op"] in ("c", "d"):
        detalle = str(fila)

    return op, tabla, f"{op:<8} {tabla}#{id_fila}  {detalle}".rstrip()


def resumen(conteo):
    print("\n--- Resumen ---")
    if not conteo:
        print("No llegó ningún evento.")
        return
    for (tabla, op), n in sorted(conteo.items()):
        print(f"{tabla:<22} {op:<9} {n}")
    print(f"{'TOTAL':<32} {sum(conteo.values())}")


def main():
    parser = argparse.ArgumentParser(description="Muestra los eventos CDC de MoneyWise")
    parser.add_argument("--from-now", action="store_true",
                        help="solo eventos nuevos (ignora el historial)")
    parser.add_argument("--verbose", action="store_true",
                        help="imprime también cada evento del snapshot")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "latest" if args.from_now else "earliest",
        "enable.auto.commit": False,   # no guarda offsets: cada corrida empieza limpia
    })
    consumer.subscribe([f"^{PREFIX}\\.{DB}\\..*"])

    print(f"Escuchando {PREFIX}.{DB}.* en {BOOTSTRAP}  (Ctrl+C para salir)")
    conteo = Counter()
    esperando = False

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if conteo and not esperando:
                    print("... sin mensajes nuevos, esperando cambios ...")
                    esperando = True
                continue
            if msg.error():
                print(f"[ERROR] {msg.error()}", file=sys.stderr)
                continue
            if msg.value() is None:   # tombstone: Debezium lo manda tras un DELETE
                continue

            esperando = False
            op, tabla, texto = describir(json.loads(msg.value())["payload"])
            conteo[(tabla, op)] += 1

            if op != "SNAPSHOT" or args.verbose:
                print(texto)
            elif sum(conteo.values()) % 500 == 0:
                print(f"... {sum(conteo.values())} eventos leídos ...")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        resumen(conteo)


if __name__ == "__main__":
    main()