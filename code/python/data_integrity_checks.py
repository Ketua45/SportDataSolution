"""
Contrôle qualité ad-hoc — SportDataSolution
=============================================
Script à lancer manuellement pour valider les données Bronze et Silver
déjà présentes sur S3, indépendamment du streaming en cours.

Utilise le module data_quality.py (mêmes règles que dans le pipeline).

Usage :
  conda run -n SportDataSolution spark-submit \
    --master local[*] \
    --packages org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.3 \
    code/python/data_integrity_checks.py
"""

import os
import warnings
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv(dotenv_path="/home/romain/formation_data_engineer/Projet_12_SportDataSolution/.env")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, DecimalType,
)

from data_quality import validate_bronze_batch, validate_silver

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("DataQuality_AdHoc")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

S3_BUCKET          = os.getenv("AWS_S3_BUCKET", "sportdatasolution-469345420249-eu-west-3-an")
BRONZE_PATH        = f"s3a://{S3_BUCKET}/pratique_sportives/parquet/"
SILVER_PATH        = f"s3a://{S3_BUCKET}/silver/mobilite_douces_employe_sport/parquet/"

bronze_schema = StructType([
    StructField("id_evenement_sportif",   IntegerType()),
    StructField("employe_id",             IntegerType()),
    StructField("type_pratique_sportive", StringType()),
    StructField("date",                   TimestampType()),
    StructField("temps_ecoule",           IntegerType()),
    StructField("commentaire",            StringType()),
    StructField("is_deleted",             BooleanType()),
    StructField("kafka_ts",               TimestampType()),
    StructField("distance_km",            DecimalType(10, 0)),
])

print("=" * 60)
print("Contrôle qualité — couche Bronze")
print("=" * 60)
df_bronze = spark.read.schema(bronze_schema).parquet(BRONZE_PATH)
print(f"  {df_bronze.count()} enregistrements lus depuis {BRONZE_PATH}")
validate_bronze_batch(df_bronze)

print("=" * 60)
print("Contrôle qualité — couche Silver")
print("=" * 60)
df_silver = spark.read.parquet(SILVER_PATH)
print(f"  {df_silver.count()} enregistrements lus depuis {SILVER_PATH}")
validate_silver(df_silver)

spark.stop()
