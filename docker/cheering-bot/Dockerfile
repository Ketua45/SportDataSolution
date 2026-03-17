FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka==2.8.0 \
    slack-sdk==3.34.0 \
    sqlalchemy==2.0.40 \
    psycopg2-binary==2.9.10 \
    python-dotenv==1.1.0

COPY code/python/cheering_crowd_bot.py .

CMD ["python", "cheering_crowd_bot.py"]
