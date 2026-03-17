import os
import threading

import boto3
from dotenv import load_dotenv

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, count, lit, coalesce
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, DecimalType,
)

import googlemaps


# Configuration


load_dotenv(dotenv_path="/home/romain/formation_data_engineer/Projet_12_SportDataSolution/.env")

# Chemins S3
S3_BUCKET              = os.getenv("AWS_S3_BUCKET", "sportdatasolution-469345420249-eu-west-3-an")
PARQUET_SOURCE_PATH    = f"s3a://{S3_BUCKET}/pratique_sportives/parquet/"
SILVER_PARQUET_PATH    = f"s3a://{S3_BUCKET}/silver/mobilite_douces_employe_sport/parquet/"
SILVER_CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/silver/mobilite_douces_employe_sport/checkpoint_parquet/"

# Chemin stable pour PowerBI
SILVER_STABLE_PREFIX   = "silver/mobilite_douces_employe_sport/parquet/"
SILVER_STABLE_KEY      = "silver/mobilite_douces_employe_sport/silver_latest.parquet"

# Référentiel mobilité douce
ADRESSE_ENTREPRISE    = "1362 Av. des Platanes, 34970 Lattes"
SEUIL_DISTANCE_MARCHE = 15   # km
SEUIL_DISTANCE_VELO   = 25   # km


# SparkSession


spark = (
    SparkSession.builder
    .appName("Streaming_Silver_MobiliteSport")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager",
    )
    .getOrCreate()
)

print("Spark :", spark.version)


# Google Maps client


gmaps = googlemaps.Client(key=os.getenv("GMAPS_API_KEY"))


# Schéma Parquet source


parquet_schema = StructType([
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


# Helpers


def read_pg_table(table_name: str):
    """Relit une table PostgreSQL fraîche à chaque appel (capture les nouvelles lignes)."""
    return (
        spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://192.168.1.177:5432/SportDataSolution")
        .option("dbtable", table_name)
        .option("user", os.getenv("PG_USER"))
        .option("password", os.getenv("PG_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .load()
    )


# Cache des distances pour éviter les appels répétés à l'API Google Maps
# Clé : (employe_id, mode) → distance en km
_distance_cache: dict = {}
_cache_lock = threading.Lock()


def get_distance_cached(employe_id: int, adresse_employe: str, mode: str) -> float:
    """Retourne la distance domicile-travail depuis le cache ou via l'API Google Maps."""
    key = (employe_id, mode)
    with _cache_lock:
        if key in _distance_cache:
            return _distance_cache[key]
    result = gmaps.distance_matrix(
        origins=[ADRESSE_ENTREPRISE],
        destinations=[adresse_employe],
        mode=mode,
    )
    km = result["rows"][0]["elements"][0]["distance"]["value"] / 1000
    with _cache_lock:
        _distance_cache[key] = km
    return km


def write_silver_parquet(df_employee_data) -> None:
    """
    Recompte l'intégralité des activités Bronze et écrit le fichier Silver.

    Logique :
    - Relit TOUS les fichiers Parquet Bronze (source complète, pas seulement le batch)
      pour calculer le total exact des activités par employé depuis l'origine.
    - Cette approche est idempotente : redémarrer le streaming ne double pas les compteurs.
    - Fusionne avec les données employés fraîches du batch courant.
    - Réécrit l'intégralité en un seul fichier Parquet (coalesce(1) + overwrite).

    Paramètres
    ----------
    df_employee_data : DataFrame
        Données employés enrichies (adresse, distance, cohérence) issues du batch courant.
    """
    # Recomptage total depuis toute la source Bronze (idempotent)
    df_sport_count_total = (
        spark.read
        .schema(parquet_schema)
        .parquet(PARQUET_SOURCE_PATH)
        .filter(col("is_deleted") == False)
        .groupBy("employe_id")
        .agg(count("employe_id").alias("pratique_sportive_annuelle"))
    )

    new_total = df_sport_count_total.agg(
        F.sum("pratique_sportive_annuelle").alias("total")
    ).collect()[0]["total"] or 0

    # Comparaison avec le Silver existant : écriture uniquement si les données ont changé
    try:
        df_existing = spark.read.parquet(SILVER_PARQUET_PATH)
        existing_total = df_existing.agg(
            F.sum("pratique_sportive_annuelle").alias("total")
        ).collect()[0]["total"] or 0
    except Exception:
        existing_total = -1  # Silver absent → force la première écriture

    if new_total == existing_total:
        print(f"Silver déjà à jour ({new_total} activités) — écriture ignorée.")
        return

    # Fusion données employés fraîches + total recalculé
    df_final = (
        df_employee_data
        .join(df_sport_count_total, on="employe_id", how="left")
        .withColumn(
            "pratique_sportive_annuelle",
            coalesce(col("pratique_sportive_annuelle"), lit(0)),
        )
    )

    # Écriture en un seul fichier Parquet (overwrite)
    (df_final
        .coalesce(1)
        .write
        .format("parquet")
        .mode("overwrite")
        .save(SILVER_PARQUET_PATH))

    print(f"Silver mis à jour — {df_final.count()} employés, {new_total} activités totales")

    # ── Copie vers un chemin stable pour PowerBI ──────────────────────────────
    _publish_stable_parquet()


def _publish_stable_parquet() -> None:
    """Copie le fichier Parquet Silver vers un chemin fixe lisible par PowerBI."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        region_name=os.getenv("AWS_REGION", "eu-west-3"),
    )
    # Liste tous les objets sous le préfixe Silver
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=SILVER_STABLE_PREFIX)
    all_keys = [obj["Key"] for obj in response.get("Contents", [])]
    print(f"Fichiers trouvés dans {SILVER_STABLE_PREFIX} : {all_keys}")

    # Recherche du fichier .parquet Spark (part-XXXXX-*.parquet ou *.snappy.parquet)
    parquet_key = next(
        (k for k in all_keys
         if (k.endswith(".parquet") or k.endswith(".snappy.parquet"))
         and not k.endswith("silver_latest.parquet")),
        None,
    )
    if parquet_key is None:
        print(f"Avertissement : aucun fichier Parquet Spark trouvé dans s3://{S3_BUCKET}/{SILVER_STABLE_PREFIX}")
        return
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": parquet_key},
        Key=SILVER_STABLE_KEY,
    )
    print(f"Fichier stable publié → s3://{S3_BUCKET}/{SILVER_STABLE_KEY}")



# foreachBatch handler


def process_silver_batch(batch_df, batch_id: int) -> None:
    """
    Traite un micro-batch de fichiers Parquet d'activités sportives.

    Les données sont déjà parsées et décodées (écrites par le streaming Redpanda → S3).

    Étapes :
    1. Filtrage des suppressions CDC
    2. Rechargement frais des tables PostgreSQL (nouveaux employés inclus)
    3. Filtre mobilités douces + validation des distances (avec cache Google Maps)
    4. Comptage des activités sportives présentes dans le batch
    5. Écriture en Parquet dans le répertoire Silver
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id} vide — rien à traiter.")
        return

    # --- 1. Filtrage des suppressions CDC ---
    df_batch = batch_df.filter(col("is_deleted") == False)

    # --- 2. Rechargement frais des données PostgreSQL ---
    df_salaries_fresh  = read_pg_table("public.salarie")
    df_moyen_dep_fresh = read_pg_table("public.moyen_deplacement")

    # --- 3a. Filtre mobilités douces ---
    df_mobilite_douce = (
        df_salaries_fresh.alias("s")
        .join(df_moyen_dep_fresh.alias("m"), on="moyen_deplacement_id", how="inner")
        .select(
            "s.employe_id", "s.nom_employe", "s.prenom_employe",
            "m.moyen_deplacement_id", "m.nom_moyen_deplacement", "s.adresse",
        )
        .filter(
            "m.nom_moyen_deplacement = 'Vélo/Trottinette/Autres' "
            "OR m.nom_moyen_deplacement = 'Marche/running'"
        )
    )

    # --- 3b. Validation des distances domicile-travail (avec cache) ---
    rows = df_mobilite_douce.select("employe_id", "adresse", "moyen_deplacement_id").collect()
    results = []
    for row in rows:
        id_employe         = row["employe_id"]
        adresse_employe    = row["adresse"]
        id_moyen_transport = row["moyen_deplacement_id"]

        if id_moyen_transport == 2:       # Marche/running
            km = get_distance_cached(id_employe, adresse_employe, "walking")
            coherence = "OK" if km <= SEUIL_DISTANCE_MARCHE else "ERREUR"
        elif id_moyen_transport == 3:     # Vélo/Trottinette/Autres
            km = get_distance_cached(id_employe, adresse_employe, "bicycling")
            coherence = "OK" if km <= SEUIL_DISTANCE_VELO else "ERREUR"
        else:
            km        = None
            coherence = None

        results.append({
            "employe_id":           id_employe,
            "adresse":              adresse_employe,
            "distance_km":          float(km) if km is not None else None,
            "moyen_deplacement_id": id_moyen_transport,
            "coherence_distance":   coherence,
        })

    df_coherence = spark.createDataFrame(results)

    # Merge salariés avec distances
    df_salarie_mob = (
        df_salaries_fresh.alias("s")
        .join(df_coherence.alias("d"), on="employe_id", how="left")
        .select("s.*", col("d.distance_km"), col("d.coherence_distance"))
    )

    # --- 4. Table Silver : merge final + recomptage total ---
    df_silver = (
        df_salarie_mob
        .withColumn("batch_id",        lit(batch_id))
        .withColumn("processing_time", F.current_timestamp())
    )

    write_silver_parquet(df_silver)
    print(f"Batch {batch_id} traité.")


# Démarrage du streaming


df_stream_sport = (
    spark.readStream
    .format("parquet")
    .schema(parquet_schema)
    .load(PARQUET_SOURCE_PATH)
)

query_silver = (
    df_stream_sport.writeStream
    .foreachBatch(process_silver_batch)
    .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

# Publication initiale
_publish_stable_parquet()

print("Streaming Silver démarré — surveillance de", PARQUET_SOURCE_PATH)
query_silver.awaitTermination()
