import pandas as pd
import pymongo
import os
from pymongo.errors import ServerSelectionTimeoutError, BulkWriteError
import boto3

# 🔹 Paramètres S3
bucket_name = 'ocprojet7'
s3_key = '/lamadeleine/Weather+Underground+-+La+Madeleine,+FR.xlsx'
file_path = '/tmp/Weather+Underground+-+La+Madeleine,+FR.xlsx'

# 🔹 Téléchargement depuis S3
s3 = boto3.client('s3')
s3.download_file(bucket_name, s3_key, file_path)
# 🔹 Vérifier si le fichier existe
if not os.path.exists(file_path):
    print(f"❌ Fichier introuvable : {file_path}")
    exit(1)

# 🔹 Charger le fichier Excel
try:
    xls = pd.ExcelFile(file_path)
except Exception as e:
    print(f"❌ Erreur lors de la lecture du fichier Excel : {e}")
    exit(1)

# 🔹 Liste pour stocker les DataFrames transformés
dfs = []

# 🔹 Dictionnaire pour convertir les directions de vent en degrés
wind_directions = {
    "North": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
    "East": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
    "South": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
    "West": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5
}

# 🔹 Parcourir chaque feuille du fichier Excel
for sheet_name in xls.sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)

    # 🔹 Nettoyage
    df.dropna(how="all", inplace=True)  # Supprime les lignes vides
    df.replace("\xa0", "", regex=True, inplace=True)  # Supprime espaces insécables

    # 🔹 Conversion de la direction du vent
    df["Wind"] = df["Wind"].map(wind_directions)

    # 🔹 Transformation du temps et de la date
    date = pd.to_datetime(sheet_name, format="%d%m%y", errors='coerce').strftime("%Y-%m-%d")
    df["Time"] = pd.to_datetime(df["Time"].str.strip(), format="%H:%M:%S", errors='coerce').dt.time
    df["datetime_iso"] = pd.to_datetime(date + " " + df["Time"].astype(str), errors='coerce')
    df.drop(columns=["Time"], inplace=True)

    # 🔹 Ajout des informations station météo
    df["id_station"] = "ILAMAD25"
    df["station_name"] = "La Madeleine"
    df["latitude"] = 50.659
    df["longitude"] = 3.07
    df["elevation"] = 23
    df["name"] = "La Madeleine"
    df["state"] = "-/-"
    df["hardware"] = "other"
    df["software"] = "EasyWeatherPro_V5.1.6"

    dfs.append(df)

# 🔹 Fusion des feuilles Excel
df = pd.concat(dfs, ignore_index=True)

# 🔹 Renommer les colonnes pour MongoDB
df.columns = [
    "temperature", "point_de_rosee", "humidite", "vent_direction",
    "vent_moyen", "vent_rafales", "pression", "pluie_1h", "pluie_3h",
    "uv", "rayonnement_solaire", "dh_utc", "id_station", "station_name",
    "latitude", "longitude", "elevation", "name", "state", "hardware", "software"
]

# 🔹 Conversion des unités (température, vent, pression, pluie)
df["temperature"] = ((df["temperature"].str.replace("°F", "", regex=True).astype(float) - 32) * 5/9).round(1)
df["point_de_rosee"] = ((df["point_de_rosee"].str.replace("°F", "", regex=True).astype(float) - 32) * 5/9).round(1)
df["humidite"] = df["humidite"].str.replace("%", "", regex=True).astype(float)
df["vent_moyen"] = (df["vent_moyen"].str.replace("mph", "", regex=True).astype(float) * 1.60934).round(1)
df["vent_rafales"] = (df["vent_rafales"].str.replace("mph", "", regex=True).astype(float) * 1.60934).round(1)
df["pression"] = (df["pression"].str.replace("in", "", regex=True).astype(float) * 33.8639).round(1)
df["pluie_1h"] = (df["pluie_1h"].str.replace("in", "", regex=True).astype(float) * 25.4).round(1)
df["pluie_3h"] = (df["pluie_3h"].str.replace("in", "", regex=True).astype(float) * 25.4).round(1)
df["uv"] = df["uv"].astype(int)
df["rayonnement_solaire"] = df["rayonnement_solaire"].str.replace("w/m²", "", regex=True).astype(float)

df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

df.dropna(how='all', inplace=True)

# 🔹 Sauvegarde en pickle
pickle_path = "/app/df_xls2.pkl"
df.to_pickle(pickle_path)
print(f"✅ DataFrame sauvegardé en pickle : {pickle_path}")

# 🔹 Connexion MongoDB avec Replica Set
MONGO_URI = "mongodb://172.31.43.57:27017,172.31.30.71:27017/?replicaSet=rs0"
DATABASE_NAME = "projet7"
COLLECTION_NAME = "weather"

# 🔹 Vérification de la connexion MongoDB
try:
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()
    print("✅ Connexion à MongoDB réussie.")
except ServerSelectionTimeoutError:
    print("❌ Impossible de se connecter à MongoDB. Vérifie que le service est bien démarré.")
    exit(1)

# 🔹 Insertion des données avec gestion des erreurs
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

try:
    data_to_insert = df.to_dict(orient="records")
    collection.insert_many(data_to_insert)
    print(f"✅ {len(data_to_insert)} documents insérés dans MongoDB.")
except BulkWriteError as bwe:
    print(f"❌ Erreur d'insertion dans MongoDB : {bwe.details}")
except Exception as e:
    print(f"❌ Erreur MongoDB inconnue : {e}")
