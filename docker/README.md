# Docker — Pipeline de données SportDataSolution

Ce dossier contient l'ensemble des fichiers Docker permettant de faire tourner le pipeline de données en production conteneurisée.

---

## Architecture

```
Redpanda (CDC PostgreSQL)
        │
        ▼
┌───────────────────┐     ┌─────────────────────┐
│   cheering-bot    │     │   redpanda-to-s3     │
│  (Python slim)    │     │   (PySpark 4.1.1)    │
│                   │     │                      │
│ Consomme le topic │     │ Lit le topic Kafka   │
│ Kafka et envoie   │     │ et écrit en Parquet  │
│ une notif Slack   │     │ sur S3 (Bronze)      │
│ par activité      │     │                      │
└───────────────────┘     └──────────┬───────────┘
                                     │ S3 Parquet Bronze
                                     ▼
                          ┌─────────────────────┐
                          │  streaming-silver    │
                          │  (PySpark 4.1.1)     │
                          │                      │
                          │ Surveille S3, joint  │
                          │ avec PostgreSQL et   │
                          │ écrit Parquet Silver │
                          └─────────────────────┘
```

## Services

| Service | Image de base | Script | Rôle |
|---------|--------------|--------|------|
| `cheering-bot` | `python:3.11-slim` | `cheering_crowd_bot.py` | Kafka consumer → notification Slack |
| `redpanda-to-s3` | `python:3.11-slim` + Java 17 | `redpanda_to_s3_parquet.py` | CDC Redpanda → S3 Parquet (Bronze) |
| `streaming-silver` | `python:3.11-slim` + Java 17 | `streaming_silver_mobilite_sport.py` | S3 Parquet + PostgreSQL → S3 Parquet (Silver) |

---

## Prérequis

- [Docker](https://docs.docker.com/engine/install/) ≥ 24
- [Docker Compose](https://docs.docker.com/compose/install/) ≥ 2.20
- Redpanda en cours d'exécution sur la machine hôte (`127.0.0.1:9092`)
- PostgreSQL accessible sur le réseau local (`192.168.1.177:5432`)
- Un bucket S3 AWS configuré et les clés d'accès disponibles
- Un Webhook Slack actif pour le bot de félicitations

---

## Configuration

### 1. Fichier `.env`

Le fichier `.env` doit être placé à la **racine du projet** (un niveau au-dessus de ce dossier). Il est partagé par tous les services via `env_file: ../.env`.

Copier le template et remplir les valeurs :

```bash
cp docker/.env.example .env
```

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY` | Clé d'accès AWS |
| `AWS_SECRET_KEY` | Clé secrète AWS |
| `AWS_REGION` | Région S3 (ex. `eu-west-3`) |
| `AWS_S3_BUCKET` | Nom du bucket S3 |
| `PG_USER` | Utilisateur PostgreSQL |
| `PG_PASSWORD` | Mot de passe PostgreSQL |
| `DATABASE_URL` | URL complète SQLAlchemy (utilisée par le cheering-bot) |
| `GMAPS_API_KEY` | Clé API Google Maps Distance Matrix |
| `SLACK_WEBHOOK_URL` | URL du Webhook entrant Slack |
| `REDPANDA_BROKER` | Adresse du broker Redpanda (défaut : `127.0.0.1:9092`) |

### 2. Réseau

Tous les services utilisent `network_mode: host` afin d'accéder directement à :

- Redpanda sur `127.0.0.1:9092` (même machine)
- PostgreSQL sur `192.168.1.177:5432` (réseau local)

> **Linux uniquement.** Sur macOS ou Windows, remplacer `network_mode: host` par un réseau bridge avec `extra_hosts` :
>
> ```yaml
> networks:
>   - pipeline
> extra_hosts:
>   - "host.docker.internal:host-gateway"
> ```
> Et mettre à jour `REDPANDA_BROKER=host.docker.internal:9092` dans `.env`.

---

## Lancement

Toutes les commandes sont à exécuter depuis la **racine du projet**.

### Construire les images et démarrer tous les services

```bash
docker compose -f docker/docker-compose.yml up --build
```

### Démarrer en arrière-plan (mode détaché)

```bash
docker compose -f docker/docker-compose.yml up --build -d
```

### Démarrer un seul service

```bash
# Uniquement le bot Slack
docker compose -f docker/docker-compose.yml up cheering-bot

# Uniquement le pipeline Redpanda → S3
docker compose -f docker/docker-compose.yml up redpanda-to-s3

# Uniquement le pipeline Silver
docker compose -f docker/docker-compose.yml up streaming-silver
```

### Arrêter tous les services

```bash
docker compose -f docker/docker-compose.yml down
```

---

## Consulter les logs

```bash
# Tous les services en temps réel
docker compose -f docker/docker-compose.yml logs -f

# Un service spécifique
docker compose -f docker/docker-compose.yml logs -f cheering-bot
docker compose -f docker/docker-compose.yml logs -f redpanda-to-s3
docker compose -f docker/docker-compose.yml logs -f streaming-silver
```

---

## Structure des fichiers

```
docker/
├── README.md                        # ce fichier
├── docker-compose.yml               # orchestration des 3 services
├── .env.example                     # template des variables d'environnement
│
├── cheering-bot/
│   └── Dockerfile                   # image Python slim pour le bot Slack
│
└── spark/
    ├── Dockerfile                   # image Python + Java 17 + PySpark 4.1.1
    └── prefetch_jars.py             # pré-téléchargement des JARs Maven au build
```

Les scripts Python du pipeline sont dans `code/python/` (à la racine du projet) et copiés dans les images lors du build :

| Script | Service |
|--------|---------|
| `code/python/cheering_crowd_bot.py` | `cheering-bot` |
| `code/python/redpanda_to_s3_parquet.py` | `redpanda-to-s3` |
| `code/python/streaming_silver_mobilite_sport.py` | `streaming-silver` |

---

## Détails techniques

### Image Spark (`docker/spark/Dockerfile`)

- Base : `eclipse-temurin:17-jre-jammy` (Ubuntu 22.04 + Java 17 préinstallé, `JAVA_HOME` déjà configuré)
- Python 3.11 installé via `apt` (`python3.11`, `python3-pip`)
- PySpark 4.1.1, `googlemaps`, `psycopg2-binary`, `python-dotenv` installés via `pip`
- **JARs Maven pré-téléchargés pendant le build** dans `/root/.ivy2.5.2/` :
  - `org.apache.hadoop:hadoop-aws:3.4.2` (accès S3)
  - `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1` (lecture Redpanda)
  - `org.postgresql:postgresql:42.7.3` (lecture PostgreSQL JDBC)
- Les services passent `--packages` à `spark-submit` au démarrage, les JARs sont déjà dans le cache Ivy → démarrage sans téléchargement réseau

### Image cheering-bot (`docker/cheering-bot/Dockerfile`)

- Base : `python:3.11-slim`
- Dépendances : `confluent-kafka`, `slack-sdk`, `sqlalchemy`, `psycopg2-binary`
- Consomme le topic `cdc.public.pratique_sport` avec `auto.offset.reset=latest`

### Checkpoint S3

Spark Structured Streaming utilise par défaut des renames atomiques pour les fichiers de checkpoint, opération non supportée par S3. Les deux scripts Spark sont configurés avec :

```python
spark.sql.streaming.checkpointFileManagerClass =
    FileSystemBasedCheckpointFileManager
```

Ce gestionnaire écrit directement avec `overwrite=true`, compatible S3.

### Chemins S3

| Répertoire / Fichier | Contenu |
|----------------------|---------|
| `pratique_sportives/parquet/` | Bronze — activités sportives brutes parsées |
| `pratique_sportives/checkpoint/` | Checkpoint du streaming Bronze |
| `silver/mobilite_douces_employe_sport/parquet/` | Silver — données enrichies (fichier Parquet Spark, nom UUID) |
| `silver/mobilite_douces_employe_sport/silver_latest.parquet` | **Chemin fixe PowerBI** — copié par boto3 à chaque batch |
| `silver/mobilite_douces_employe_sport/checkpoint_parquet/` | Checkpoint du streaming Silver |

---

## Dépannage

### `ClassNotFoundException: S3AFileSystem`

Les JARs ne sont pas chargés. Vérifier que `--packages` est bien passé à `spark-submit` dans le `docker-compose.yml`.

### `MALFORMED_LOG_FILE` au démarrage

Le checkpoint S3 provient d'un streaming d'un type différent (ex. Delta Lake). Supprimer le répertoire de checkpoint concerné sur S3 et relancer.

### `Multiple streaming queries are concurrently using checkpoint`

Une instance précédente du streaming est encore active (dans un notebook par exemple), ou le checkpoint est dans un état incohérent. Arrêter toutes les instances actives et, si le problème persiste, supprimer le checkpoint S3.

### Bot Slack ne démarre pas

Vérifier que `SLACK_WEBHOOK_URL` et `DATABASE_URL` sont correctement renseignés dans `.env`, et que Redpanda est bien accessible depuis le container (`REDPANDA_BROKER`).
