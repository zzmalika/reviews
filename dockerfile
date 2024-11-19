FROM apache/airflow:2.10.3

USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

USER root

