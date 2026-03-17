from confluent_kafka import Consumer, KafkaError
from slack_sdk.webhook import WebhookClient
from datetime import datetime, timezone
import base64, json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()



# --- Configuration ---
REDPANDA_BROKER   = os.getenv("REDPANDA_BROKER", "127.0.0.1:9092")
TOPIC             = "cdc.public.pratique_sport"
GROUP_ID          = "slack-notifier"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

engine = create_engine(os.getenv("DATABASE_URL"))

# --- Helpers ---
def parse_date(microseconds: str) -> str:
    """Convertit un timestamp en microsecondes en date lisible."""
    try:
        ts = int(microseconds) / 1_000_000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d/%m/%Y")
    except Exception:
        return microseconds

def parse_distance(distance_field: dict) -> str:
    """Décode la distance depuis le format Debezium NUMERIC (base64)."""
    try:
        raw   = base64.b64decode(distance_field["value"])
        value = int.from_bytes(raw, byteorder="big")
        scale = distance_field.get("scale", 0)
        result = value / (10 ** scale)
        return f"{result:.2f} km"
    except Exception:
        return "N/A"

def parse_temps(secondes: int) -> str:
    """Convertit des secondes en format mm:ss."""
    m, s = divmod(int(secondes), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}m{s:02d}s"


def recuperation_employe(employe_id: int) -> str:
    """Récupère le nom de l'employé à partir de son ID."""
    try:
        query = text("SELECT prenom_employe, nom_employe FROM salarie WHERE employe_id = :employe_id")
        with engine.connect() as conn:
            result = conn.execute(query, {"employe_id": employe_id}).fetchone()
            if result:
                return f"{result[0]} {result[1]}"
            return f"Employé #{employe_id}"
    except Exception as e:
        print(f"Erreur récupération employé: {e}")
        return f"Employé #{employe_id}"

def format_slack_message(data: dict) -> str:
    """Construit le message Slack personnalisé."""
    est_supprime = data.get("__deleted", "false") == "true"
    # emoji_sport = {
    #     "Natation":  "🏊",
    #     "Course":    "🏃",
    #     "Vélo":      "🚴",
    #     "Football":  "⚽",
    #     "Tennis":    "🎾",
    # }.get(data.get("type_pratique_sportive", ""), "🏅")


    return (
        ""
        f"\n\n*Félicitations {recuperation_employe(data.get('employe_id'))} ! Nouvelle séance de {data.get('type_pratique_sportive').lower()} enregistrée !*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        #f"📋 *Événement*  : `#{data.get('id_evenement_sportif')}`\n"
        #f"👤 *Employé*    : `{data.get('employe_id')}`\n"
        #f"👤 *Employé*    : {recuperation_employe(data.get('employe_id'))}\n"
        f"🏃 *Sport*      : {data.get('type_pratique_sportive')}\n"
        f"📅 *Date*       : {parse_date(data.get('date', '0'))}\n"
        f"📏 *Distance*   : {parse_distance(data.get('distance', {}))}\n"
        f"⏱️ *Temps*      : {parse_temps(data.get('temps_ecoule', 0))}\n"
        f"💬 *Commentaire*: _{data.get('commentaire', '')}_\n\n"
    )

# --- Consumer ---
consumer = Consumer({
    "bootstrap.servers": REDPANDA_BROKER,
    "group.id":          GROUP_ID,
    "auto.offset.reset": "latest",
})
slack = WebhookClient(SLACK_WEBHOOK_URL)
consumer.subscribe([TOPIC])

print("Consumer démarré")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Erreur Kafka : {msg.error()}")
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
        except json.JSONDecodeError:
            print(f"Message non JSON ignoré : {msg.value()}")
            continue

        message_text = format_slack_message(data)
        response = slack.send(text=message_text)

        if response.status_code == 200:
            print(f"Slack notifié | offset={msg.offset()} | employé={data.get('employe_id')}")
        else:
            print(f"Erreur Slack : {response.status_code} - {response.body}")

except KeyboardInterrupt:
    print("\nArrêt du consumer.")
finally:
    consumer.close()