FROM bitnami/spark:3.5

USER root

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    redis==5.0.3 \
    python-dotenv==1.0.0 \
    loguru==0.7.2 \
    numpy==1.26.4 \
    pandas==2.2.1

WORKDIR /app
COPY . .

USER 1001
