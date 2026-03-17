"""
Déclenche le téléchargement des JARs Maven au moment du build Docker.
Ce script est exécuté une seule fois via spark-submit pendant la construction
de l'image, afin que les JARs soient présents dans le cache ~/.ivy2 et
ne nécessitent pas de téléchargement à chaque démarrage du container.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("prefetch_jars").getOrCreate()
spark.stop()
