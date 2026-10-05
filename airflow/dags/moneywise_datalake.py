"""DAG moneywise_datalake (Issue 9): orquesta el pipeline completo del data lake.

  revisar_infra -> hay_datos_nuevos -> bronze -> silver -> check_silver
                -> gold -> check_gold -> anomalias -> check_anomalias

- revisar_infra:    el conector de Debezium está RUNNING y Spark (contenedor) responde.
- hay_datos_nuevos: compara hasta dónde llegó Kafka con hasta dónde leyó Bronze. Si no hay
                    nada nuevo, se salta el resto. Con el parámetro `forzar` corre igual.
- Los jobs son los mismos spark-submit que se corrían a mano, vía `docker exec mw-spark`.
- Los check_* son compuertas de calidad: si algún chequeo falla, el DAG se detiene.
- Reintentos: 2, con 2 minutos de espera. Cada reintento y cada falla se anota en el
  archivo de alertas (logs/alertas.log) y queda visible en la interfaz de Airflow.
"""
import json
import os
import subprocess
import urllib.request
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, get_current_context, task

ZONA = "America/Chihuahua"
SPARK = os.getenv("SPARK_CONTAINER", "mw-spark")
SUBMIT = f"docker exec {SPARK} /usr/local/spark/bin/spark-submit"
TRABAJO = "/home/jovyan/work"
PAQUETE_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
CONECTOR = os.getenv("CONNECTOR_NAME", "moneywise-mysql-source")
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
PREFIJO_TOPICS = "moneywise.moneywise."
CHECKPOINT_BRONZE = Path(os.getenv("BRONZE_CHECKPOINT_DIR", "/opt/airflow/data/checkpoints/bronze"))
ARCHIVO_ALERTAS = Path(os.getenv("AIRFLOW_ALERTS_FILE", "/opt/airflow/logs/alertas.log"))
ESPERA_REINTENTO = timedelta(minutes=float(os.getenv("MW_REINTENTO_MINUTOS", "2")))


# ---------------------------------------------------------------- alertas
def _anotar(tipo, context):
    ti = context.get("task_instance")
    linea = (
        f"{pendulum.now(ZONA).isoformat(timespec='seconds')} {tipo} "
        f"dag={getattr(ti, 'dag_id', '?')} tarea={getattr(ti, 'task_id', '?')} "
        f"intento={getattr(ti, 'try_number', '?')} corrida={context.get('run_id', '?')} "
        f"error={context.get('exception')}"
    )
    print(linea)
    try:
        ARCHIVO_ALERTAS.parent.mkdir(parents=True, exist_ok=True)
        with ARCHIVO_ALERTAS.open("a", encoding="utf-8") as f:
            f.write(linea + "\n")
    except OSError as error:  # una alerta nunca debe romper la tarea
        print(f"No se pudo escribir el archivo de alertas: {error}")


def alerta_fallo(context):
    _anotar("FALLO", context)


def alerta_reintento(context):
    _anotar("REINTENTO", context)


# ------------------------------------------------- "¿hay datos nuevos?"
def offsets_de_kafka():
    """Dónde va cada topic de MoneyWise en Kafka: {(topic, partición): siguiente offset}."""
    from confluent_kafka import Consumer, TopicPartition

    consumidor = Consumer({"bootstrap.servers": KAFKA, "group.id": "airflow-compuerta", "enable.auto.commit": False})
    try:
        metadatos = consumidor.list_topics(timeout=15)
        resultado = {}
        for topic, info in metadatos.topics.items():
            if not topic.startswith(PREFIJO_TOPICS):
                continue
            for particion in info.partitions:
                _, alto = consumidor.get_watermark_offsets(TopicPartition(topic, particion), timeout=15)
                resultado[(topic, particion)] = alto
        return resultado
    finally:
        consumidor.close()


def offsets_de_bronze(directorio=CHECKPOINT_BRONZE):
    """Hasta dónde leyó Bronze, según el último lote confirmado de su checkpoint de Spark."""
    commits, offsets = Path(directorio) / "commits", Path(directorio) / "offsets"
    if not commits.is_dir():
        return {}  # Bronze nunca ha corrido
    lotes = sorted(int(p.name) for p in commits.iterdir() if p.name.isdigit())
    if not lotes:
        return {}
    for linea in (offsets / str(lotes[-1])).read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea.startswith("{"):
            continue
        try:
            datos = json.loads(linea)
        except ValueError:
            continue
        # La línea de offsets tiene {topic: {partición: offset}}; la de metadatos mezcla otros tipos.
        if datos and all(isinstance(v, dict) for v in datos.values()):
            return {(t, int(p)): int(o) for t, parts in datos.items() for p, o in parts.items()}
    return {}


def datos_pendientes(kafka, bronze):
    """Mensajes que están en Kafka y Bronze todavía no leyó: {(topic, partición): cuántos}."""
    return {clave: alto - bronze.get(clave, 0) for clave, alto in kafka.items() if alto > bronze.get(clave, 0)}


# --------------------------------------------------------------------- DAG
with DAG(
    dag_id="moneywise_datalake",
    description="Bronze -> Silver -> Gold -> anomalías, con compuertas de calidad, reintentos y alertas",
    schedule="0 3 * * *",  # todos los días a las 3:00 (hora de Chihuahua)
    start_date=pendulum.datetime(2026, 10, 1, tz=ZONA),
    catchup=False,
    max_active_runs=1,  # Silver y Gold se reescriben completos: nunca dos corridas a la vez
    params={"forzar": Param(False, type="boolean", description="Correr aunque no haya datos nuevos en Kafka")},
    default_args={
        "retries": 2,
        "retry_delay": ESPERA_REINTENTO,
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": alerta_fallo,
        "on_retry_callback": alerta_reintento,
    },
    tags=["moneywise", "datalake"],
    doc_md=__doc__,
) as dag:

    @task
    def revisar_infra():
        url = f"{CONNECT_URL}/connectors/{CONECTOR}/status"
        with urllib.request.urlopen(url, timeout=15) as respuesta:
            estado = json.load(respuesta)
        conector = estado["connector"]["state"]
        tareas = [t["state"] for t in estado.get("tasks", [])]
        if conector != "RUNNING" or not tareas or any(t != "RUNNING" for t in tareas):
            raise RuntimeError(f"El conector {CONECTOR} no está sano: conector={conector}, tareas={tareas}")
        print(f"Conector {CONECTOR}: RUNNING ({len(tareas)} tarea).")

        prueba = subprocess.run(["docker", "exec", SPARK, "true"], capture_output=True, text=True)
        if prueba.returncode != 0:
            raise RuntimeError(f"Airflow no puede ejecutar comandos en {SPARK}: {prueba.stderr.strip()}")
        print(f"Spark ({SPARK}) responde.")

    @task.short_circuit
    def hay_datos_nuevos():
        if get_current_context()["params"].get("forzar"):
            print("Parámetro forzar=True: se corre aunque no haya datos nuevos.")
            return True
        pendientes = datos_pendientes(offsets_de_kafka(), offsets_de_bronze())
        if pendientes:
            print(f"Hay {sum(pendientes.values())} mensajes nuevos en {len(pendientes)} topics:")
            for (topic, particion), n in sorted(pendientes.items()):
                print(f"  {topic}[{particion}]: {n}")
            return True
        print("No hay datos nuevos en Kafka desde la última corrida de Bronze: se salta el resto.")
        return False

    bronze = BashOperator(
        task_id="bronze",
        bash_command=f"{SUBMIT} --packages {PAQUETE_KAFKA} --conf spark.jars.ivy=/tmp/ivy {TRABAJO}/spark/jobs/bronze/bronze_job.py",
    )
    silver = BashOperator(task_id="silver", bash_command=f"{SUBMIT} {TRABAJO}/spark/jobs/silver/silver_job.py")
    check_silver = BashOperator(task_id="check_silver", bash_command=f"{SUBMIT} {TRABAJO}/spark/jobs/silver/check_silver.py")
    gold = BashOperator(task_id="gold", bash_command=f"{SUBMIT} {TRABAJO}/spark/jobs/gold/gold_job.py")
    check_gold = BashOperator(task_id="check_gold", bash_command=f"{SUBMIT} {TRABAJO}/spark/jobs/gold/check_gold.py")
    anomalias = BashOperator(task_id="anomalias", bash_command=f"{SUBMIT} {TRABAJO}/anomalies/anomalias_job.py")
    check_anomalias = BashOperator(task_id="check_anomalias", bash_command=f"{SUBMIT} {TRABAJO}/anomalies/check_anomalias.py")

    (
        revisar_infra()
        >> hay_datos_nuevos()
        >> bronze >> silver >> check_silver
        >> gold >> check_gold
        >> anomalias >> check_anomalias
    )