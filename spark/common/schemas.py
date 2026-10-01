"""Tipos de cada tabla para la capa Silver.

Para cada tabla: columna -> tipo final en Silver. Los eventos de Debezium traen
los valores en formatos propios (fechas en milisegundos, montos como texto,
booleanos como 0/1...) y silver_job.py los convierte usando este diccionario.

Tipos soportados: int, bigint, string, boolean, timestamp, date, decimal(p,s).

Fuera de aquí, a propósito:
- usuarios.password_hash: Debezium lo excluye desde el conector (column.exclude.list).
- auth_tokens: no se incluye en table.include.list del conector.
"""

# Todas las tablas del lake usan "id" como llave primaria.
PK = "id"

# Columnas de auditoría que comparten casi todas las tablas.
_AUDITORIA = {
    "creado_en": "timestamp",
    "actualizado_en": "timestamp",
    "eliminado_en": "timestamp",
}

# Catálogos: destinos, procedencias, tipos_egreso y tipos_ingreso tienen la misma forma.
_CATALOGO = {
    "id": "int",
    "usuario_id": "int",
    "nombre": "string",
    "es_por_defecto": "boolean",
    **_AUDITORIA,
}

TABLAS = {
    "usuarios": {
        "id": "int",
        "nombre": "string",
        "apellido_p": "string",
        "apellido_m": "string",
        "correo": "string",
        "fecha_nacimiento": "date",
        "scopes": "string",  # JSON guardado como texto
        "activo": "boolean",
        "creado_en": "timestamp",
        "actualizado_en": "timestamp",
    },
    "destinos": _CATALOGO,
    "procedencias": _CATALOGO,
    "tipos_egreso": _CATALOGO,
    "tipos_ingreso": _CATALOGO,
    "frecuencias": {
        "id": "int",
        "nombre": "string",
        **_AUDITORIA,
    },
    "ingresos": {
        "id": "bigint",
        "usuario_id": "int",
        "tipo_id": "int",
        "procedencia_id": "int",
        "frecuencia_id": "int",
        "monto": "decimal(12,2)",
        "descripcion": "string",
        "fecha_inicio": "timestamp",
        "fecha_fin": "timestamp",
        **_AUDITORIA,
    },
    "egresos": {
        "id": "bigint",
        "usuario_id": "int",
        "tipo_id": "int",
        "destino_id": "int",
        "frecuencia_id": "int",
        "monto": "decimal(12,2)",
        "descripcion": "string",
        "fecha_inicio": "timestamp",
        "fecha_fin": "timestamp",
        **_AUDITORIA,
    },
    "inversiones": {
        "id": "bigint",
        "usuario_id": "int",
        "destino_id": "int",
        "monto": "decimal(12,2)",
        "objetivo": "string",
        "tasa_interes": "decimal(5,2)",
        "fecha_inicio": "timestamp",
        "fecha_fin": "timestamp",
        **_AUDITORIA,
    },
    "metas": {
        "id": "bigint",
        "usuario_id": "int",
        "nombre": "string",
        "monto_objetivo": "decimal(12,2)",
        "ahorro_real": "decimal(12,2)",
        "activa": "boolean",
        "fecha_inicio": "timestamp",
        "fecha_fin": "timestamp",
        **_AUDITORIA,
    },
    "fechas_corte_ahorro": {
        "id": "int",
        "usuario_id": "int",
        "fecha_corte": "timestamp",
        "creado_en": "timestamp",
    },
}