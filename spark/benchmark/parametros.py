"""Reglas del benchmark, compartidas por el generador y el verificador.

Los datos sintéticos NO son aleatorios: cada valor sale de una fórmula sobre el id de la fila. Por eso el
verificador puede calcular, por su cuenta y sin Spark, qué debe contener Gold, y compararlo con lo que
Silver y Gold produjeron de verdad.

Qué le pasa a cada fila de egresos / ingresos (según su id):
  - todas nacen con un evento de creación ('c');
  - id % 10 == 3   -> luego un UPDATE que sube el monto 5.00;
  - id % 20 == 11  -> luego un UPDATE que llena eliminado_en (borrado lógico: Gold la ignora);
  - id % 50 == 7   -> luego un DELETE (la fila deja de existir: Silver la quita).
Las tres reglas nunca coinciden en la misma fila (7, 11 y 3 son restos distintos módulo 10), así que cada
fila sufre como máximo una.
"""

INICIO = "2025-01-01"   # primer mes de los datos
MESES = 21              # los datos cubren 21 meses (2025-01 a 2026-09)

UPD_MOD, UPD_RES, UPD_CENTAVOS = 10, 3, 500
ELIM_MOD, ELIM_RES = 20, 11
DEL_MOD, DEL_RES = 50, 7

SIN_DESTINO_MOD = 33    # id % 33 == 0 -> egreso sin destino (Gold lo agrupa en "Sin destino", id 0)

N_DESTINOS = 10
N_TIPOS_EGRESO = 4
N_TIPOS_INGRESO = 4
N_PROCEDENCIAS = 6
N_FRECUENCIAS = 5

# monto en centavos = base + (id * mult) % mod. 'divisor': ingresos tiene 1/5 de las filas que egresos.
MOVIMIENTOS = {
    "egresos": {"base": 1000, "mult": 7919, "mod": 500_000, "divisor": 1},
    "ingresos": {"base": 2000, "mult": 104_729, "mod": 800_000, "divisor": 5},
}


def filas_de(tabla, filas_egresos):
    """Cuántas filas tiene la tabla cuando egresos tiene `filas_egresos`."""
    return filas_egresos // MOVIMIENTOS[tabla]["divisor"]


def destino_de(i):
    """destino_id de un egreso (0 = sin destino)."""
    return 0 if i % SIN_DESTINO_MOD == 0 else 1 + (i * 7) % N_DESTINOS


def tipo_egreso_de(i):
    return 1 + i % N_TIPOS_EGRESO
