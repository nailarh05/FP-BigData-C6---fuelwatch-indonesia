FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir psycopg2-binary python-dotenv pandas==2.1.4 pyarrow==14.0.2
COPY seed_data.py .
CMD ["python", "seed_data.py"]
