import pandas as pd
import json
import pymongo
import os
from pymongo.errors import ServerSelectionTimeoutError, BulkWriteError
import boto3

# 🔹 Paramètres S3
bucket_name = 'ocprojet7'
s3_key = '/infoclimat/2025_03_09_1741535628223_0-1.csv'
file_path = '/tmp/2025_03_09_1741535628223_0-1.csv'

# 🔹 Téléchargement depuis S3
s3 = boto3.client('s3')
s3.download_file(bucket_name, s3_key, file_path)

# 🔹 Vérifier si le fichier existe
if not os.path.exists(file_path):
    print(f"❌ Fichier introuvable : {file_path}")
    exit(1)

# 🔹 Charger le fichier CSV
try:
    df_csv = pd.read_csv(file_path)
    json_list = df_csv["_airbyte_data"].apply(lambda s: json.loads(s)).tolist()
except Exception as e:
    print(f"❌ Erreur lors de la lecture du fichier CSV : {e}")
    exit(1)

# 🔹 Extraction des données JSON
data = json_list[0]  # On suppose ici que le CSV contient un seul objet JSON
df_stations = pd.json_normalize(data["stations"]).iloc[:, :6]  # Garder les 6 premières colonnes
df_hourly = pd.concat([pd.DataFrame(v) for v in data["hourly"].values()], ignore_index=True)

# 🔹 Fusionner avec les données stations
df = df_hourly.merge(df_stations, left_on="id_station", right_on="id", how="left")
df.drop(columns=["id"], inplace=True)



# 🔹 Nettoyage des données
df.dropna(how='all', inplace=True)
df = df.loc[:, df.columns.map(str).map(lambda x: not x.isnumeric())]


# 🔹 Conversion des types 
numeric_columns = [
    "temperature", "pression", "humidite", "point_de_rosee", "visibilite",
    "vent_moyen", "vent_rafales", "vent_direction", "pluie_3h", "pluie_1h",
    "neige_au_sol", "nebulosite", "temps_omm", "latitude", "longitude", "elevation"
]
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df["dh_utc"] = pd.to_datetime(df["dh_utc"], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S')

# 🔹 Remplacer les NaN par `None` pour MongoDB
df = df.where(pd.notnull(df), None)
df = df[df["dh_utc"].notna()]

# 🔹 Sauvegarde en pickle
pickle_path = "/app/df_json.pkl"
df.to_pickle(pickle_path)
print(f"✅ DataFrame sauvegardé en pickle : {pickle_path}")

# 🔹 Connexion MongoDB avec Replica Set
MONGO_URI = "mongodb://mongo1.mongo.local:27017,mongo2.mongo.local:27017,mongo-arbiter.mongo.local:27018/?replicaSet=rs0"
DATABASE_NAME = "projet7"
COLLECTION_NAME = "weather"

# 🔹 Vérifier que MongoDB est accessible
try:
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()  # Test de connexion
    print("✅ Connexion à MongoDB réussie.")
except ServerSelectionTimeoutError:
    print("❌ Impossible de se connecter à MongoDB. Vérifie que le service est bien démarré.")
    exit(1)

# 🔹 Insertion dans MongoDB avec gestion des erreurs
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

try:
    data_to_insert = df.to_dict(orient="records")
    collection.insert_many(data_to_insert)
    print(f"✅ {len(data_to_insert)} documents insérés dans MongoDB.")
except BulkWriteError as bwe:
    print(f"❌ Erreur lors de l'insertion dans MongoDB : {bwe.details}")
except Exception as e:
    print(f"❌ Erreur MongoDB inconnue : {e}")
