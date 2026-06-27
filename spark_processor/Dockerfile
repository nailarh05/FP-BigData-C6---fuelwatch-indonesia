FROM python:3.11-slim

# PySpark butuh JRE.
# CATATAN: openjdk-17 sempat dipakai, tapi base image python:3.11-slim sekarang
# pakai Debian "trixie" yang sudah tidak menyediakan paket openjdk-17 — diganti
# openjdk-21 (PySpark 3.5.x resmi mendukung Java 21, sudah ditest jalan lancar).
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "main.py"]
