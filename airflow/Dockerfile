# Imagen de Airflow para MoneyWise: Airflow oficial + cliente de Docker + librerías de Kafka y Google Cloud.
#  - El cliente de Docker permite que las tareas corran `docker exec mw-spark ...`.
#  - confluent-kafka lo usa la tarea que revisa si hay datos nuevos en Kafka.
#  - google-cloud-storage lo usa gcp/subir_gold.py para publicar Gold en Cloud Storage.
#  - google-cloud-bigquery lo usa gcp/cargar_bigquery.py para cargar Gold en BigQuery.
FROM docker:28-cli AS docker-cli

FROM apache/airflow:3.3.2
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
# Se vuelve a fijar la versión de Airflow (igual que en el FROM) para que pip no toque sus dependencias.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" confluent-kafka==2.15.1 google-cloud-storage==3.16.0 google-cloud-bigquery==3.46.1
