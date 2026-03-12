import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd 
import random

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

data_employes = pd.read_csv("/home/romain/formation_data_engineer/Projet_12_SportDataSolution/data/tables_sql/employes.csv")
# Fonction de création de données virtuelles


def StravaLikeDataGen(dataset_employes, nombre_activites):

    """""""""""""""""""""
    Fonction qui permet de générer des activités fictives sur le modèle de Strava, pour les salariés de l'entreprise.
     - dataset_employes : dataframe contenant les données des employés (doit contenir une colonne "employe_id" pour faire le lien avec les activités)
     - nombre_activites : nombre d'activités fictives à générer
    """""""""""""""""""""
    import random
    import pandas as pd

    sports = ["Course à pied", "Vélo", "Natation", "Marche", "Trotinette", "Voile"]
    id_employes = dataset_employes["employe_id"]
    distance = range(1,15)
    date = pd.date_range(start='2025-01-01', end='2025-12-31', freq='D')
    temps_exercice = range(600,3600)

    commentaires = [
        "Super séance, je me suis senti(e) en pleine forme !",
        "Pas mal, mais j'ai eu du mal à trouver mon rythme.",
        "A refaire très vite !",
        "J'ai adoré cette activité, c'était vraiment génial !"]
    
    print("Tentative d'insertion de", nombre_activites, "activités...")
    for _ in range(nombre_activites):
        activite = pd.DataFrame({
            "employe_id": [random.choice(id_employes)],
            "type_pratique_sportive": [random.choice(sports)],
            "distance": [random.choice(distance)],
            "date": [random.choice(date)],
            "temps_ecoule": [random.choice(temps_exercice)],
            "commentaire": [random.choice(commentaires)]
        })

        # Récupérer le dernier id présent dans la base
        last_id_query = "SELECT COALESCE(MAX(id_evenement_sportif), 0) FROM pratique_sport"
        last_id = pd.read_sql(last_id_query, con=engine).iloc[0, 0]

        activite.index = [last_id + 1]
        
        activite.to_sql(
            name="pratique_sport",
            con=engine,
            if_exists="append",
            index=True, 
            index_label = "id_evenement_sportif"
        )
    
    print(nombre_activites, "activités insérées avec succès")
    
    pass

StravaLikeDataGen(data_employes, 1500)