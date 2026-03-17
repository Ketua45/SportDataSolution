# eclipse-temurin:17-jre-jammy = Ubuntu 22.04 + Java 17 préinstallé et JAVA_HOME configuré
# Evite d'installer openjdk via apt depuis l'image python:slim (sources parfois instables)
FROM eclipse-temurin:17-jre-jammy

# Python 3.11 + pip (disponible dans les dépôts Ubuntu 22.04)
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3.11 \
        python3-pip \
        procps \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.11 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Dépendances Python
RUN pip install --no-cache-dir \
    pyspark==4.1.1 \
    python-dotenv==1.1.0 \
    googlemaps==4.10.0 \
    psycopg2-binary==2.9.10 \
    boto3==1.37.24

# ── Pré-téléchargement des JARs Maven ────────────────────────────────────────
# Les JARs sont téléchargés pendant le build et mis en cache dans ~/.ivy2.5.2
# → les containers démarrent sans accès réseau Maven au runtime
COPY docker/spark/prefetch_jars.py /tmp/prefetch_jars.py

# redpanda-to-s3 : kafka + hadoop-aws
RUN spark-submit \
        --master local[1] \
        --packages \
            org.apache.hadoop:hadoop-aws:3.4.2,\
            org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
        /tmp/prefetch_jars.py || true

# streaming-silver : hadoop-aws + postgresql
RUN spark-submit \
        --master local[1] \
        --packages \
            org.apache.hadoop:hadoop-aws:3.4.2,\
            org.postgresql:postgresql:42.7.3 \
        /tmp/prefetch_jars.py || true

WORKDIR /app

# Copie des scripts Python du pipeline
COPY code/python/redpanda_to_s3_parquet.py .
COPY code/python/streaming_silver_mobilite_sport.py .
