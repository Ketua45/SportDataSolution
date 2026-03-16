import os
import base64
import warnings
warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DecimalType,
)
from pyspark.sql.functions import udf
from decimal import Decimal
from delta import configure_spark_with_delta_pip

# ---------------------------------------------------------------------------
# Session Spark
# ---------------------------------------------------------------------------
builder = (
    SparkSession.builder
    .appName("RedPanda_to_S3_Parquet")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.hadoop:hadoop-aws:3.4.2",
        "org.postgresql:postgresql:42.7.3",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
    ]
).getOrCreate()

print("Version de Spark  :", spark.version)
print("Version de Hadoop :", spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

# ---------------------------------------------------------------------------
# Schéma CDC Debezium
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("id_evenement_sportif", IntegerType()),
    StructField("employe_id", IntegerType()),
    StructField("type_pratique_sportive", StringType()),
    StructField("date", StringType()),
    StructField("distance", StructType([
        StructField("scale", IntegerType()),
        StructField("value", StringType()),   # base64
    ])),
    StructField("temps_ecoule", IntegerType()),
    StructField("commentaire", StringType()),
    StructField("__deleted", StringType()),
])

# ---------------------------------------------------------------------------
# UDF : décodage du décimal Debezium (base64 big-endian)
# ---------------------------------------------------------------------------
@udf(returnType=DecimalType(10, 0))
def decode_debezium_decimal(b64_value, scale):
    if b64_value is None:
        return None
    raw = base64.b64decode(b64_value)
    val = int.from_bytes(raw, byteorder="big", signed=True)
    return Decimal(val) / (Decimal(10) ** scale)

# ---------------------------------------------------------------------------
# Lecture streaming depuis Redpanda
# ---------------------------------------------------------------------------
df_stream_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "cdc.public.pratique_sport")
    .option("startingOffsets", "earliest")
    .load()
)

# ---------------------------------------------------------------------------
# Parsing JSON + transformations
# ---------------------------------------------------------------------------
df_stream_final = (
    df_stream_raw
    .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    .select(from_json(col("json_str"), schema).alias("data"), "kafka_ts")
    .select("data.*", "kafka_ts")
    .withColumn("date", (col("date").cast("long") / 1000000).cast("timestamp"))
    .withColumn("distance_base64", col("distance.value"))
    .withColumn("distance_scale", col("distance.scale"))
    .withColumn("is_deleted", col("__deleted") == "true")
    .drop("distance", "__deleted")
    .withColumn("distance_km", decode_debezium_decimal(col("distance_base64"), col("distance_scale")))
    .drop("distance_base64", "distance_scale")
)

# ---------------------------------------------------------------------------
# Ecriture en streaming vers S3 au format Parquet
# ---------------------------------------------------------------------------
PARQUET_PATH    = "s3a://sportdatasolution-469345420249-eu-west-3-an/pratique_sportives/parquet/"
CHECKPOINT_PATH = "s3a://sportdatasolution-469345420249-eu-west-3-an/pratique_sportives/checkpoint/"

query_parquet = (
    df_stream_final.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start(PARQUET_PATH)
)

print("Streaming vers S3 (Parquet) démarré.")
query_parquet.awaitTermination()
