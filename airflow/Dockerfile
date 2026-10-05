# Imagen de Airflow para MoneyWise: Airflow oficial + cliente de Docker + confluent-kafka.
#  - El cliente de Docker permite que las tareas corran `docker exec mw-spark ...`.
#  - confluent-kafka lo usa la tarea que revisa si hay datos nuevos en Kafka.
FROM docker:28-cli AS docker-cli

FROM apache/airflow:3.3.2
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
RUN pip install --no-cache-dir confluent-kafka==2.15.1