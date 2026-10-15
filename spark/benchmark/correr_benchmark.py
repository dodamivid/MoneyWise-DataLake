"""Corre el benchmark de Silver y Gold con datos sintéticos y mide cuánto tarda cada etapa.

Para cada tamaño:
  1. genera un Bronze sintético en data/benchmark/<filas>/bronze   (no cuenta en la medición)
  2. corre silver_job.py y gold_job.py SIN modificarlos, apuntados a esa carpeta con variables de entorno
  3. mide el tiempo de cada uno, el tamaño en disco y cuántas filas salieron
  4. comprueba que Gold tenga exactamente lo esperado (verificar.py)
Tu lago real (data/bronze, data/silver, data/gold) no se toca, y MySQL/Railway tampoco.

Uso, desde la raíz del repo, con el entorno de Docker arriba (contenedor mw-spark):
  python spark/benchmark/correr_benchmark.py
  python spark/benchmark/correr_benchmark.py --filas 100000 1000000 --driver-memory 6g
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

AQUI = Path(__file__).resolve().parent
RAIZ = AQUI.parents[1]
sys.path.insert(0, str(AQUI))

SPARK_SUBMIT = "/usr/local/spark/bin/spark-submit"
TRABAJO = "/home/jovyan/work"  # carpeta montada en el contenedor (docker-compose.yml)


def docker(contenedor, *args, entorno=None):
    cmd = ["docker", "exec"]
    for k, v in (entorno or {}).items():
        cmd += ["-e", f"{k}={v}"]
    cmd += [contenedor, *args]
    inicio = time.perf_counter()
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return r, time.perf_counter() - inicio


def exigir(r, etapa, memoria):
    if r.returncode == 0:
        return
    print(f"\n[ERROR] Falló la etapa '{etapa}'. Últimas líneas:\n")
    print("\n".join((r.stderr or r.stdout).strip().splitlines()[-25:]))
    print(
        f"\nSi el mensaje habla de memoria (OutOfMemory / Java heap), sube --driver-memory (ahora {memoria}) "
        "o baja --filas."
    )
    sys.exit(1)


def tamano_mb(carpeta):
    return sum(f.stat().st_size for f in Path(carpeta).rglob("*") if f.is_file()) / 1e6


def filas_de_salida(texto, tabla):
    m = re.search(rf"^{tabla}\s+(\d+) filas", texto, re.M)
    return int(m.group(1)) if m else None


def info_maquina(contenedor):
    cpu, _ = docker(contenedor, "nproc")
    mem, _ = docker(contenedor, "sh", "-c", "grep MemTotal /proc/meminfo")
    m = re.search(r"(\d+)", mem.stdout or "")
    return {
        "cpus_contenedor": int((cpu.stdout or "0").strip() or 0),
        "ram_contenedor_gb": round(int(m.group(1)) / 1024 / 1024, 1) if m else None,
    }


def correr(filas, args):
    host = RAIZ / "data" / "benchmark" / str(filas)
    cont = f"{TRABAJO}/data/benchmark/{filas}"
    if host.exists():
        shutil.rmtree(host, ignore_errors=True)
    mem = args.driver_memory

    print(f"\n=== {filas:,} filas de egresos ===")
    print("1/4 Generando Bronze sintético...")
    r, t_gen = docker(
        args.contenedor, SPARK_SUBMIT, "--driver-memory", mem,
        f"{TRABAJO}/spark/benchmark/generar_bronze.py",
        "--filas", str(filas), "--usuarios", str(args.usuarios), "--salida", f"{cont}/bronze",
    )
    exigir(r, "generar", mem)
    eventos = int(re.search(r"BRONZE_EVENTOS (\d+)", r.stdout).group(1))
    print(f"    {eventos:,} eventos en {t_gen:.0f} s (no cuenta en la medición)")

    entorno = {"BRONZE_PATH": f"{cont}/bronze", "SILVER_PATH": f"{cont}/silver", "GOLD_PATH": f"{cont}/gold"}

    print("2/4 Silver...")
    r, t_silver = docker(
        args.contenedor, SPARK_SUBMIT, "--driver-memory", mem,
        f"{TRABAJO}/spark/jobs/silver/silver_job.py", entorno=entorno,
    )
    exigir(r, "silver", mem)
    egresos_silver = filas_de_salida(r.stdout, "egresos")
    print(f"    {t_silver:.0f} s")

    print("3/4 Gold...")
    r, t_gold = docker(
        args.contenedor, SPARK_SUBMIT, "--driver-memory", mem,
        f"{TRABAJO}/spark/jobs/gold/gold_job.py", entorno=entorno,
    )
    exigir(r, "gold", mem)
    print(f"    {t_gold:.0f} s")

    cuadra = None
    problemas = []
    if not args.sin_verificar:
        print("4/4 Verificando Gold contra el cálculo independiente (Python puro, puede tardar)...")
        from verificar import verificar

        cuadra, problemas = verificar(host / "gold", filas, args.usuarios)
        print("    " + ("CUADRA" if cuadra else "NO CUADRA"))
        for p in problemas:
            print("     -", p)
    else:
        print("4/4 Verificación omitida (--sin-verificar).")

    total = t_silver + t_gold
    resultado = {
        "filas_egresos": filas,
        "eventos_bronze": eventos,
        "egresos_silver": egresos_silver,
        "segundos_generar": round(t_gen, 1),
        "segundos_silver": round(t_silver, 1),
        "segundos_gold": round(t_gold, 1),
        "segundos_total": round(total, 1),
        "eventos_por_segundo": round(eventos / total),
        "mb_bronze": round(tamano_mb(host / "bronze")),
        "mb_silver": round(tamano_mb(host / "silver")),
        "mb_gold": round(tamano_mb(host / "gold"), 1),
        "gold_cuadra": cuadra,
        "problemas": problemas,
    }
    if not args.conservar:
        shutil.rmtree(host, ignore_errors=True)
    return resultado


def tabla_markdown(resultados):
    filas = [
        "| Egresos | Eventos en Bronze | Silver | Gold | Total | Eventos/s | Bronze (MB) | Gold cuadra |",
        "|---:|---:|---:|---:|---:|---:|---:|:---:|",
    ]
    for r in resultados:
        ok = {True: "sí", False: "**NO**", None: "-"}[r["gold_cuadra"]]
        filas.append(
            f"| {r['filas_egresos']:,} | {r['eventos_bronze']:,} | {r['segundos_silver']:.0f} s | "
            f"{r['segundos_gold']:.0f} s | {r['segundos_total']:.0f} s | {r['eventos_por_segundo']:,} | "
            f"{r['mb_bronze']:,} | {ok} |"
        )
    return "\n".join(filas)


def main():
    sys.stdout.reconfigure(errors="replace")  # la consola de Windows no siempre acepta todos los caracteres
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--filas", type=int, nargs="+", default=[1_000_000, 5_000_000],
                    help="tamaños a probar, en filas de egresos (por defecto 1000000 5000000)")
    ap.add_argument("--usuarios", type=int, default=1000)
    ap.add_argument("--driver-memory", default="4g", help="memoria de Spark (por defecto 4g)")
    ap.add_argument("--contenedor", default="mw-spark")
    ap.add_argument("--sin-verificar", action="store_true", help="omite la comprobación de Gold")
    ap.add_argument("--conservar", action="store_true", help="no borra data/benchmark/<filas> al terminar")
    args = ap.parse_args()

    ok, _ = docker(args.contenedor, "true")
    if ok.returncode != 0:
        sys.exit(f"No encuentro el contenedor '{args.contenedor}' corriendo. Levántalo con: docker compose up -d")

    maquina = info_maquina(args.contenedor)
    print(f"Contenedor {args.contenedor}: {maquina['cpus_contenedor']} CPUs, {maquina['ram_contenedor_gb']} GB de RAM")

    resultados = [correr(n, args) for n in args.filas]

    salida = RAIZ / "data" / "benchmark" / "resultados.json"
    salida.parent.mkdir(parents=True, exist_ok=True)
    salida.write_text(json.dumps({"maquina": maquina, "driver_memory": args.driver_memory,
                                  "resultados": resultados}, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n" + tabla_markdown(resultados))
    print(f"\nContenedor: {maquina['cpus_contenedor']} CPUs, {maquina['ram_contenedor_gb']} GB; "
          f"Spark con --driver-memory {args.driver_memory}.")
    print(f"Resultados guardados en {salida}")
    if any(r["gold_cuadra"] is False for r in resultados):
        sys.exit("\nAlgún Gold NO cuadra: revisa los detalles de arriba antes de publicar números.")


if __name__ == "__main__":
    main()
