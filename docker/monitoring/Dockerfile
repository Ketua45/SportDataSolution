FROM python:3.11-slim

RUN pip install --no-cache-dir \
    boto3==1.37.24 \
    pyarrow==19.0.1 \
    prometheus-client==0.21.1 \
    python-dotenv==1.1.0

COPY docker/monitoring/pipeline_exporter.py /app/pipeline_exporter.py

CMD ["python", "/app/pipeline_exporter.py"]
