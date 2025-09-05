FROM apache/airflow:2.7.1

RUN pip install --no-cache-dir \
    pandas==1.5.3 \
    psycopg2-binary==2.9.7 \
    sqlalchemy==1.4.48