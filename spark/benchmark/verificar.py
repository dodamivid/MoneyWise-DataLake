"""Comprueba que Gold del benchmark contiene exactamente lo que debe, sin usar Spark.

Los datos sintéticos salen de fórmulas sobre el id (ver parametros.py), así que aquí se calculan los
totales esperados con Python puro y se comparan con los Parquet que dejaron silver_job.py y gold_job.py.
Si el procesamiento perdiera, duplicara o mezclara filas a gran escala, esto lo detecta.

Requiere pyarrow (ya está en requirements-dev.txt). Se corre en tu máquina, no en el contenedor.
"""
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import parametros as P  # noqa: E402


def _vivos(tabla, filas):
    """Genera (id, centavos) de las filas que Gold debe contar: ni borradas ni con borrado lógico."""
    cfg = P.MOVIMIENTOS[tabla]
    base, mult, mod = cfg["base"], cfg["mult"], cfg["mod"]
    for i in range(P.filas_de(tabla, filas)):
        if i % P.DEL_MOD == P.DEL_RES or i % P.ELIM_MOD == P.ELIM_RES:
            continue
        centavos = base + (i * mult) % mod
        if i % P.UPD_MOD == P.UPD_RES:
            centavos += P.UPD_CENTAVOS
        yield i, centavos


def esperado(filas, usuarios):
    """Totales esperados: {tabla_gold: {clave: (centavos, n)}}."""
    balance = {"egresos": {}, "ingresos": {}}
    destino, tipo = {}, {}

    def sumar(dic, clave, centavos):
        acum = dic.get(clave)
        if acum is None:
            dic[clave] = [centavos, 1]
        else:
            acum[0] += centavos
            acum[1] += 1

    for t in ("egresos", "ingresos"):
        for i, c in _vivos(t, filas):
            u = 1 + i % usuarios
            m = i % P.MESES
            sumar(balance[t], (u, m), c)
            if t == "egresos":
                sumar(destino, (u, m, P.destino_de(i)), c)
                sumar(tipo, (u, m, P.tipo_egreso_de(i)), c)
    return {"balance": balance, "destino": destino, "tipo": tipo}


def _mes_idx(fecha):
    ini_anio, ini_mes = int(P.INICIO[:4]), int(P.INICIO[5:7])
    return (fecha.year - ini_anio) * 12 + (fecha.month - ini_mes)


def _centavos(d):
    return int(d.scaleb(2))


def _leer(gold, tabla):
    return pq.read_table(str(Path(gold) / tabla)).to_pylist()


def verificar(gold, filas, usuarios):
    """Devuelve (ok, lista_de_problemas)."""
    esp = esperado(filas, usuarios)
    problemas = []

    def comparar(nombre, obtenido, esperado_):
        if set(obtenido) != set(esperado_):
            faltan = len(set(esperado_) - set(obtenido))
            sobran = len(set(obtenido) - set(esperado_))
            problemas.append(f"{nombre}: faltan {faltan} grupos y sobran {sobran}")
            return
        malos = [k for k in obtenido if obtenido[k] != tuple(esperado_[k])]
        if malos:
            k = malos[0]
            problemas.append(
                f"{nombre}: {len(malos)} grupos con cifras distintas; p. ej. {k}: "
                f"Gold {obtenido[k]} contra esperado {tuple(esperado_[k])}"
            )

    # balance_mensual: totales y conteos de ingresos y egresos por (usuario, mes)
    ing, egr = {}, {}
    for f in _leer(gold, "balance_mensual"):
        k = (f["usuario_id"], _mes_idx(f["mes"]))
        if f["n_ingresos"]:
            ing[k] = (_centavos(f["total_ingresos"]), f["n_ingresos"])
        if f["n_egresos"]:
            egr[k] = (_centavos(f["total_egresos"]), f["n_egresos"])
    comparar("balance_mensual (ingresos)", ing, esp["balance"]["ingresos"])
    comparar("balance_mensual (egresos)", egr, esp["balance"]["egresos"])

    # gasto por destino y por tipo
    dest = {
        (f["usuario_id"], _mes_idx(f["mes"]), f["destino_id"]): (_centavos(f["total_egresos"]), f["n_egresos"])
        for f in _leer(gold, "gasto_por_destino_mensual")
    }
    comparar("gasto_por_destino_mensual", dest, esp["destino"])
    tip = {
        (f["usuario_id"], _mes_idx(f["mes"]), f["tipo_id"]): (_centavos(f["total_egresos"]), f["n_egresos"])
        for f in _leer(gold, "gasto_por_tipo_mensual")
    }
    comparar("gasto_por_tipo_mensual", tip, esp["tipo"])

    return not problemas, problemas


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Verifica un Gold del benchmark.")
    ap.add_argument("--gold", required=True)
    ap.add_argument("--filas", type=int, required=True)
    ap.add_argument("--usuarios", type=int, default=1000)
    a = ap.parse_args()
    ok, probs = verificar(a.gold, a.filas, a.usuarios)
    print("CUADRA" if ok else "NO CUADRA")
    for p in probs:
        print(" -", p)
    sys.exit(0 if ok else 1)
