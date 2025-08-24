import pymongo
import subprocess
import os

# Configuration de la connexion MongoDB
MONGO_URI = "mongodb://mongo1.mongo.local:27017,mongo2.mongo.local:27017,mongo-arbiter.mongo.local:27017/?replicaSet=rs0"

DATABASE_NAME = "projet7"
COLLECTION_NAME = "weather"

# Connexion à la base de données
client = pymongo.MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Vérifier si la collection est vide
if collection.count_documents({}) == 0:
    print("La collection est vide. Lancement des scripts de migration...")
    subprocess.run(["python", "scriptjson.py"])
    subprocess.run(["python", "scriptxls1.py"])
    subprocess.run(["python", "scriptxls2.py"])
    subprocess.run(["pytest", "test_migration.py"])
else:
    print("La collection contient déjà des données. Aucune migration nécessaire.")

# Suppression des fichiers pickle
pickle_files = ["/app/df_json.pkl", "/app/df_xls1.pkl", "/app/df_xls2.pkl"]

for file in pickle_files:
    if os.path.exists(file):
        os.remove(file)
        print(f"{file} supprimé.")
    else:
        print(f"{file} n'existe pas.")
