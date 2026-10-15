"""Mide cuánto tarda Debezium en capturar los cambios hechos por generar_eventos_cdc.py (Issue 15).

Lee los eventos que ya llegaron a Bronze, se queda con los de los egresos de prueba (los ids están en
data/latencia/eventos_prueba.json) y calcula, para cada operación (crear / actualizar / eliminar):

  A. desde que se ENVIÓ la petición a la API hasta que Debezium emitió el evento (ts_ms).
     Incluye lo que tarda la API, el commit en MySQL y la captura. Todos los relojes son los de tu PC
     (el script y Debezium corren en ella), así que es la medida más confiable.
  B. desde que la API RESPONDIÓ hasta que Debezium emitió el evento. Puede salir un poco negativa, porque
     Debezium a veces emite antes de que la respuesta llegue de vuelta a tu PC.
  C. dentro de Debezium: ts_ms (reloj de Debezium) menos source.ts_ms (reloj del servidor MySQL en
     Railway). Se reporta porque es la métrica estándar de retraso de CDC, pero mezcla dos relojes
     distintos: un desfase entre ellos se suma o se resta al resultado (por eso puede dar negativos).

El reloj de Debezium es el del contenedor de Docker (en Windows vive dentro de una máquina virtual de WSL2), que
puede ir adelantado o atrasado respecto al reloj de tu PC. Por eso el script mide ese desfase (con `date` dentro
del contenedor) y reporta A y B también corregidas. El desfase se mide al correr el script, no cuando ocurrieron
los eventos: si la PC se suspendió entre una cosa y otra, la corrección puede no ser exacta.

No usa Spark ni Kafka: solo lee los Parquet de Bronze. Antes hay que haber corrido bronze_job.py para que
los eventos lleguen a data/bronze. Requiere pyarrow (viene en requirements-dev.txt).

Uso, desde la raíz del repo:
  python scripts\\medir_latencia.py
"""
import argparse
import json
import math
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
ENTRADA = RAIZ / "data" / "latencia" / "eventos_prueba.json"
SALIDA = RAIZ / "data" / "latencia" / "resultados.json"
OPERACIONES_CDC = ("c", "u", "d")


def a_ms(iso):
    """'2026-10-14T20:01:02.345Z' -> milisegundos desde 1970."""
    return round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def percentil(valores, p):
    """Percentil por rango más cercano (con pocos datos, p95 coincide con el máximo)."""
    ordenados = sorted(valores)
    return ordenados[max(0, math.ceil(p / 100 * len(ordenados)) - 1)]


def resumen(valores):
    if not valores:
        return None
    return {
        "n": len(valores),
        "min_ms": min(valores),
        "mediana_ms": round(statistics.median(valores)),
        "p95_ms": percentil(valores, 95),
        "max_ms": max(valores),
    }


def desfase_contenedor(contenedor, muestras=7):
    """Reloj del contenedor menos reloj de la PC, en ms. None si no se pudo medir.

    Cada muestra pregunta la hora al contenedor y la compara con la mitad del viaje de ida y vuelta; se queda
    con la mediana de las 3 muestras de viaje más corto (las más precisas).
    """
    try:
        tomadas = []
        for _ in range(muestras):
            t0 = time.time() * 1000
            r = subprocess.run(["docker", "exec", contenedor, "date", "-u", "+%s%3N"],
                               capture_output=True, text=True, timeout=30)
            t1 = time.time() * 1000
            if r.returncode != 0:
                return None
            tomadas.append((t1 - t0, int(r.stdout.strip()) - (t0 + t1) / 2))
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None
    tomadas.sort()
    return round(statistics.median(o for _, o in tomadas[:3]))


def leer_eventos_bronze(bronze, ids):
    """Eventos CDC de egresos en Bronze cuyo id está en `ids`: {id: [evento, ...]} ordenados por offset."""
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(bronze), format="parquet", partitioning="hive")
    tabla = dataset.to_table(filter=ds.field("tabla") == "egresos", columns=["offset", "value"])
    por_id = {}
    for fila in tabla.to_pylist():
        if not fila["value"]:  # mensaje vacío (tombstone que Kafka manda tras un DELETE)
            continue
        payload = json.loads(fila["value"]).get("payload") or {}
        if payload.get("op") not in OPERACIONES_CDC:
            continue  # 'r' = lectura del snapshot inicial: no es un cambio en vivo
        fila_datos = payload.get("after") or payload.get("before") or {}
        try:
            egreso_id = int(fila_datos.get("id"))
        except (TypeError, ValueError):
            continue
        if egreso_id in ids:
            por_id.setdefault(egreso_id, []).append({
                "offset": fila["offset"],
                "op": payload["op"],
                "emitido_ms": payload.get("ts_ms"),
                "binlog_ms": (payload.get("source") or {}).get("ts_ms"),
            })
    for eventos in por_id.values():
        eventos.sort(key=lambda e: e["offset"])
    return por_id


def emparejar(operaciones, por_id):
    """Une cada operación de la API con su evento (en orden, por egreso). Devuelve (pares, sin_evento)."""
    por_egreso = {}
    for op in operaciones:
        por_egreso.setdefault(op["egresoId"], []).append(op)
    pares, sin_evento = [], []
    for egreso_id, ops in por_egreso.items():
        eventos = por_id.get(egreso_id, [])
        for i, op in enumerate(ops):
            if i < len(eventos):
                pares.append((op, eventos[i]))
            else:
                sin_evento.append(op)
    return pares, sin_evento


def main():
    sys.stdout.reconfigure(errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bronze", default=str(RAIZ / "data" / "bronze"))
    ap.add_argument("--eventos", default=str(ENTRADA))
    ap.add_argument("--contenedor", default="mw-connect", help="contenedor de Debezium, para medir su desfase de reloj")
    args = ap.parse_args()

    ruta = Path(args.eventos)
    if not ruta.exists():
        sys.exit(f"No encuentro {ruta}. Corre antes: python scripts\\generar_eventos_cdc.py")
    registro = json.loads(ruta.read_text(encoding="utf-8"))
    operaciones = [o for o in registro["operaciones"] if 200 <= o["http"] < 300]
    ids = {o["egresoId"] for o in operaciones}

    por_id = leer_eventos_bronze(args.bronze, ids)
    pares, sin_evento = emparejar(operaciones, por_id)
    if not pares:
        sys.exit(
            "No hay eventos de los egresos de prueba en Bronze todavía. Corre bronze_job.py para traerlos "
            "de Kafka (el comando está en el README) y vuelve a intentar."
        )

    desfase = desfase_contenedor(args.contenedor)
    medidas = {"A_envio_a_emision": [], "B_respuesta_a_emision": [], "C_binlog_a_emision": [],
               "A_corregida": [], "B_corregida": []}
    detalle = []
    for op, ev in pares:
        if ev["emitido_ms"] is None:
            continue
        a = ev["emitido_ms"] - a_ms(op["enviado"])
        b = ev["emitido_ms"] - a_ms(op["respondido"])
        medidas["A_envio_a_emision"].append(a)
        medidas["B_respuesta_a_emision"].append(b)
        if desfase is not None:
            medidas["A_corregida"].append(a - desfase)
            medidas["B_corregida"].append(b - desfase)
        c = None
        if ev["binlog_ms"] is not None:
            c = ev["emitido_ms"] - ev["binlog_ms"]
            medidas["C_binlog_a_emision"].append(c)
        detalle.append({"op": op["op"], "egresoId": op["egresoId"], "evento": ev["op"], "A_ms": a, "B_ms": b, "C_ms": c})

    nombres = {
        "A_envio_a_emision": "A. petición enviada -> evento emitido por Debezium",
        "B_respuesta_a_emision": "B. respuesta de la API -> evento emitido",
        "C_binlog_a_emision": "C. binlog de MySQL -> evento emitido (mezcla 2 relojes)",
        "A_corregida": "A corregida por el desfase del reloj del contenedor",
        "B_corregida": "B corregida por el desfase del reloj del contenedor",
    }
    resultados = {k: resumen(v) for k, v in medidas.items()}

    print(f"\nOperaciones de la API: {len(operaciones)}  |  con evento en Bronze: {len(pares)}  |  sin evento: {len(sin_evento)}")
    if desfase is None:
        print(f"\nNo pude medir el desfase del reloj de '{args.contenedor}' (¿Docker apagado?): A y B salen sin corregir.")
    else:
        print(f"Reloj del contenedor '{args.contenedor}' menos reloj de la PC: {desfase:+d} ms")
    print(f"\n{'medida':<58}{'n':>3}{'mín':>8}{'mediana':>9}{'p95':>8}{'máx':>8}   (ms)")
    for clave, titulo in nombres.items():
        r = resultados[clave]
        if r:
            print(f"{titulo:<58}{r['n']:>3}{r['min_ms']:>8}{r['mediana_ms']:>9}{r['p95_ms']:>8}{r['max_ms']:>8}")
    if sin_evento:
        print("\nOperaciones sin evento todavía (¿falta correr bronze_job.py, o Debezium estaba apagado?):")
        for op in sin_evento:
            print(f"  {op['op']} egreso {op['egresoId']}")
    if any((r or {}).get("min_ms", 0) < 0 for r in (resultados["C_binlog_a_emision"],)):
        print("\nNota: la medida C tiene valores negativos: el reloj de MySQL (Railway) va adelantado respecto al de "
              "Debezium. Para citar un número usa la medida A.")
    print(f"\nCon n = {len(pares)} el p95 casi coincide con el máximo: es una muestra chica, no una garantía.")

    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    SALIDA.write_text(json.dumps({"desfase_contenedor_ms": desfase, "resultados": resultados, "detalle": detalle, "sin_evento": sin_evento},
                                 indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Guardado en {SALIDA}")


if __name__ == "__main__":
    main()