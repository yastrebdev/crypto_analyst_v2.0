FROM apache/airflow:3.1.8
ENV PYTHONPATH=/opt/airflow
USER root
RUN pip install dbt-postgres \
    clickhouse-driver==0.2.10 \
    kafka-python-ng \
    great-expectations \
    transformers \
    vaderSentiment
#    torch

USER airflow