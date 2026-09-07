#!/usr/bin/env bash
#
# Crea las 13 issues de MoneyWise-DataLake en el repo de GitHub actual.
#
# Requisitos:
#   - gh CLI instalado y autenticado:  gh auth login
#   - Ejecutarse dentro del repo con remote de GitHub configurado
#       (o exportar GH_REPO=owner/repo)
#
# Uso:
#   bash scripts/create_issues.sh
#
set -euo pipefail

command -v gh >/dev/null 2>&1 || { echo "ERROR: gh CLI no encontrado. Instala GitHub CLI y corre 'gh auth login'."; exit 1; }
gh auth status >/dev/null 2>&1 || { echo "ERROR: gh no autenticado. Corre 'gh auth login'."; exit 1; }

create () {
  local title="$1"
  local body="$2"
  echo ">> Creando: ${title}"
  gh issue create --title "${title}" --body "${body}"
}

create "Issue 1 — Setup del repo y docker-compose base" \
"Crear docker-compose.yml levantando: MySQL (binlog-format=ROW, server-id configurado), Zookeeper, Kafka, Kafka Connect (con plugin de Debezium). Debe levantar con un solo \`docker-compose up\`. Incluir .env.example.

**Arquitectura:** Infraestructura — MySQL, Zookeeper, Kafka, Kafka Connect (Debezium)."

create "Issue 2 — Fuente MySQL con datos reales" \
"Reutilizar/adaptar el schema de MoneyWise-Integracion (usuarios, ingresos, egresos, metas) como fuente. Seed con datos de prueba realistas. Verificar binlog activo y accesible.

**Arquitectura:** Fuente OLTP — MySQL + binlog."

create "Issue 3 — Conector CDC con Debezium" \
"Configurar el conector MySQL de Debezium vía API REST de Kafka Connect (connectors/mysql-source.json). Verificar que INSERT/UPDATE/DELETE en MySQL generen eventos en topics de Kafka por tabla.

**Arquitectura:** CDC — Debezium → Kafka (topics por tabla)."

create "Issue 4 — Script de verificación de eventos" \
"Consumer simple de Kafka (Python, confluent-kafka o kafka-python) que imprima eventos llegando, para validar captura antes de meter Spark.

**Arquitectura:** CDC — consumer de validación de Kafka."

create "Issue 5 — Spark Job: Bronze (ingesta cruda)" \
"PySpark Structured Streaming leyendo topics de Kafka, escribiendo a data/bronze/ en Parquet, particionado por fecha/tabla, sin transformar.

**Arquitectura:** Data Lake — capa Bronze."

create "Issue 6 — Spark Job: Silver (limpieza y estandarización)" \
"Job que lee Bronze, limpia (tipos, nulos, dedup por CDC quedándose con el evento más reciente por PK), escribe a data/silver/.

**Arquitectura:** Data Lake — capa Silver."

create "Issue 7 — Spark Job: Gold (capa curada para BI)" \
"Agregaciones de negocio: totales por usuario/mes, balance, tendencias de gasto por categoría. Escribe a data/gold/.

**Arquitectura:** Data Lake — capa Gold."

create "Issue 8 — Detección de anomalías" \
"Módulo sobre Silver/Gold: detección estadística (z-score o IQR) de transacciones atípicas. Output a tabla/topic \"anomalias\".

**Arquitectura:** Data Lake — anomalías sobre Silver/Gold."

create "Issue 9 — Orquestación con Airflow" \
"DAG encadenando: espera de nuevos datos → Spark Bronze → Silver → Gold → detección de anomalías, con reintentos y alertas básicas.

**Arquitectura:** Orquestación — DAG Bronze→Silver→Gold→anomalías."

create "Issue 10 — Salida a Cloud (GCP)" \
"Al final del DAG, subir capa Gold a bucket de GCS (free tier). Autenticación vía service account, credenciales en .env, nunca hardcodeadas.

**Arquitectura:** Nube — GCP Cloud Storage (capa Gold)."

create "Issue 11 — Documentación y diagrama de arquitectura" \
"README con diagrama Mermaid del flujo completo e instrucciones de cómo correr todo local.

**Arquitectura:** Transversal — documentación."

create "Issue 12 — Tests de calidad de datos" \
"Data quality checks (pytest, opcionalmente great_expectations): Gold sin nulos en campos clave, totales cuadrando contra la fuente.

**Arquitectura:** Transversal — QA sobre capa Gold."

create "Issue 13 — Dashboard de BI conectado a Gold" \
"Conectar Gold (Parquet) a Power BI o Metabase. Mostrar balance por usuario/mes, gastos por categoría en el tiempo, anomalías resaltadas. Documentar conexión con capturas.

**DEPENDE de que el Issue 7 esté completo.**

**Arquitectura:** Business Intelligence."

echo ""
echo "Listo. 13 issues creadas."
