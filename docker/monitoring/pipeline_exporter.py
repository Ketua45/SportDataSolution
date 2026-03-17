"""
Pipeline Exporter — métriques Prometheus pour le dashboard SportDataSolution
=============================================================================
Expose sur :9101 les métriques métier du pipeline Silver :
  - pipeline_silver_activites_total   : total des activités actives
  - pipeline_silver_employes_total    : nombre d'employés dans le Silver
  - pipeline_silver_last_update_timestamp : timestamp du dernier update (epoch ms)

Lit le fichier silver_latest.parquet depuis S3 via boto3 + pyarrow.
Actualisation toutes les 60 secondes.
"""

import os
import io
import time

import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv
from prometheus_client import Gauge, start_http_server

load_dotenv(dotenv_path="/home/romain/formation_data_engineer/Projet_12_SportDataSolution/.env")

S3_BUCKET    = os.getenv("AWS_S3_BUCKET", "sportdatasolution-469345420249-eu-west-3-an")
SILVER_KEY   = "silver/mobilite_douces_employe_sport/silver_latest.parquet"
REFRESH_SEC  = 60
EXPORTER_PORT = 9101

activites_gauge    = Gauge("pipeline_silver_activites_total",        "Nombre total d'activités sportives actives dans le Silver")
employes_gauge     = Gauge("pipeline_silver_employes_total",         "Nombre d'employés présents dans le Silver")
last_update_gauge  = Gauge("pipeline_silver_last_update_timestamp",  "Timestamp du dernier fichier Silver (epoch ms)")


def fetch_metrics() -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
        region_name=os.getenv("AWS_REGION", "eu-west-3"),
    )
    try:
        # Téléchargement en mémoire
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SILVER_KEY)
        last_modified = obj["LastModified"].timestamp() * 1000  # epoch ms

        buf = io.BytesIO(obj["Body"].read())
        table = pq.read_table(buf, columns=["employe_id", "pratique_sportive_annuelle"])
        df = table.to_pydict()

        total_activites = sum(v for v in df["pratique_sportive_annuelle"] if v)
        total_employes  = len(df["employe_id"])

        activites_gauge.set(total_activites)
        employes_gauge.set(total_employes)
        last_update_gauge.set(last_modified)

        print(f"[exporter] {total_employes} employés, {total_activites} activités")
    except Exception as e:
        print(f"[exporter] Erreur lecture Silver : {e}")


if __name__ == "__main__":
    start_http_server(EXPORTER_PORT)
    print(f"Pipeline exporter démarré sur :{EXPORTER_PORT}")
    while True:
        fetch_metrics()
        time.sleep(REFRESH_SEC)
