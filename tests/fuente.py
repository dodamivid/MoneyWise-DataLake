"""Acceso de SOLO LECTURA a la base real en Railway, para cuadrar el lago contra la fuente.

Usa el mismo usuario que Debezium (RAILWAY_MYSQL_*), que solo tiene permisos de lectura y replicación.
Todas las consultas son SELECT con nombres de tabla fijos, y TLS activado (como `ssl.mode=required` del conector).
"""
import os
from datetime import date

VARIABLES = ("RAILWAY_MYSQL_HOST", "RAILWAY_MYSQL_PORT", "RAILWAY_MYSQL_USER", "RAILWAY_MYSQL_PASSWORD")
BASE_DE_DATOS = "moneywise"
TABLAS = ("usuarios", "destinos", "procedencias", "tipos_egreso", "tipos_ingreso", "frecuencias",
          "ingresos", "egresos", "inversiones", "metas", "fechas_corte_ahorro")
CON_MONTO_Y_FECHA = ("ingresos", "egresos")


def faltan_variables():
    return [v for v in VARIABLES if not os.getenv(v)]


class FuenteRailway:
    def __init__(self):
        import pymysql  # se importa aquí: los tests que no usan la fuente no la necesitan

        self._conexion = pymysql.connect(
            host=os.environ["RAILWAY_MYSQL_HOST"],
            port=int(os.environ["RAILWAY_MYSQL_PORT"]),
            user=os.environ["RAILWAY_MYSQL_USER"],
            password=os.environ["RAILWAY_MYSQL_PASSWORD"],
            database=BASE_DE_DATOS,
            ssl={"check_hostname": False},  # TLS obligatorio, sin verificar el certificado (igual que el conector)
            connect_timeout=15,
            read_timeout=60,
            init_command="SET @@session.time_zone = '+00:00'",  # los meses se cuentan en UTC, como en Silver
        )

    def _consultar(self, sql):
        if not sql.lstrip().upper().startswith("SELECT"):
            raise RuntimeError("Solo se permiten consultas SELECT")
        with self._conexion.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def conteo(self, tabla):
        """Cuántas filas tiene la tabla en la fuente."""
        if tabla not in TABLAS:
            raise ValueError(f"Tabla desconocida: {tabla}")
        return int(self._consultar(f"SELECT COUNT(*) FROM {tabla}")[0][0])

    def totales_por_mes(self, tabla):
        """{(usuario_id, mes): (total, cantidad)} de los movimientos activos, con el mes de fecha_inicio."""
        if tabla not in CON_MONTO_Y_FECHA:
            raise ValueError(f"Tabla desconocida: {tabla}")
        filas = self._consultar(
            f"SELECT usuario_id, DATE_FORMAT(fecha_inicio, '%Y-%m-01'), SUM(monto), COUNT(*) "
            f"FROM {tabla} WHERE eliminado_en IS NULL "
            f"GROUP BY usuario_id, DATE_FORMAT(fecha_inicio, '%Y-%m-01')"
        )
        return {(u, date.fromisoformat(mes) if mes else None): (total, int(n)) for u, mes, total, n in filas}

    def cerrar(self):
        self._conexion.close()
