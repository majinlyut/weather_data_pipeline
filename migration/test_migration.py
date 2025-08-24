import pytest
import pandas as pd
import pymongo

# Configuration MongoDB et chemins vers les fichiers pickle
MONGO_URI = "mongodb://mongo1:27017"
DATABASE_NAME = "projet7"
COLLECTION_NAME = "weather"
PICKLE_JSON = "/app/df_json.pkl"
PICKLE_XLS1 = "/app/df_xls1.pkl"
PICKLE_XLS2 = "/app/df_xls2.pkl"

# Fixtures pour charger les DataFrames depuis les fichiers pickle
@pytest.fixture(scope="module")
def df_json():
    return pd.read_pickle(PICKLE_JSON)

@pytest.fixture(scope="module")
def df_xls1():
    return pd.read_pickle(PICKLE_XLS1)

@pytest.fixture(scope="module")
def df_xls2():
    return pd.read_pickle(PICKLE_XLS2)

# Fixture pour récupérer la collection MongoDB (unique pour toutes les sources)
@pytest.fixture(scope="module")
def mongo_collection():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    return db[COLLECTION_NAME]

def test_migration_json_columns_and_rows(df_json, mongo_collection):
    """
    Vérifie que pour la source JSON, les documents insérés dans la collection
    contiennent exactement les colonnes attendues et que le nombre de documents
    correspond au nombre de lignes du DataFrame JSON.
    """
    expected_columns = [
        'temperature', 'pression', 'humidite', 'point_de_rosee', 'visibilite', 
        'vent_moyen', 'vent_rafales', 'vent_direction', 'pluie_3h', 'pluie_1h', 
        'neige_au_sol', 'nebulosite', 'temps_omm', 'latitude', 'longitude', 'elevation',
        'id_station', 'name', 'type', 'dh_utc'
    ]
    
    # Filtrer par les id_station présents dans le DataFrame JSON
    stations = df_json['id_station'].unique()
    mongo_documents = list(mongo_collection.find({"id_station": {"$in": list(stations)}}))
    assert mongo_documents, "Aucun document trouvé dans MongoDB pour la source JSON."
    
    # Vérifier que les colonnes du premier document correspondent exactement aux colonnes attendues
    mongo_columns = [col for col in mongo_documents[0].keys() if col != '_id']
    assert sorted(mongo_columns) == sorted(expected_columns), (
        f"Pour JSON, les colonnes MongoDB ne correspondent pas aux attendues : {mongo_columns} vs {expected_columns}"
    )
    
    # Vérifier que le nombre de documents correspond au nombre de lignes du DataFrame JSON
    expected_row_count = len(df_json)
    actual_row_count = len(mongo_documents)
    assert expected_row_count == actual_row_count, (
        f"Pour JSON, le nombre de lignes ne correspond pas : Pickle ({expected_row_count}) vs MongoDB ({actual_row_count})"
    )

def test_migration_xls1_columns_and_rows(df_xls1, mongo_collection):
    """
    Vérifie que pour la source XLS1, les documents insérés dans la collection
    contiennent exactement les colonnes attendues et que le nombre de documents
    correspond au nombre de lignes du DataFrame XLS1.
    """
    expected_columns = [
        "temperature", "point_de_rosee", "humidite", "vent_direction", "vent_moyen", 
        "vent_rafales", "pression", "pluie_1h", "pluie_3h", "uv", "rayonnement_solaire", 
        "dh_utc", "id_station", "station_name", "latitude", "longitude", "elevation",
        "name", "state", "hardware", "software"
    ]
    
    stations = df_xls1['id_station'].unique()
    mongo_documents = list(mongo_collection.find({"id_station": {"$in": list(stations)}}))
    assert mongo_documents, "Aucun document trouvé dans MongoDB pour la source XLS1."
    
    mongo_columns = [col for col in mongo_documents[0].keys() if col != '_id']
    assert sorted(mongo_columns) == sorted(expected_columns), (
        f"Pour XLS1, les colonnes MongoDB ne correspondent pas aux attendues : {mongo_columns} vs {expected_columns}"
    )
    
    expected_row_count = len(df_xls1)
    actual_row_count = len(mongo_documents)
    assert expected_row_count == actual_row_count, (
        f"Pour XLS1, le nombre de lignes ne correspond pas : Pickle ({expected_row_count}) vs MongoDB ({actual_row_count})"
    )

def test_migration_xls2_columns_and_rows(df_xls2, mongo_collection):
    """
    Vérifie que pour la source XLS2, les documents insérés dans la collection
    contiennent exactement les colonnes attendues et que le nombre de documents
    correspond au nombre de lignes du DataFrame XLS2.
    """
    expected_columns = [
        "temperature", "point_de_rosee", "humidite", "vent_direction", "vent_moyen", 
        "vent_rafales", "pression", "pluie_1h", "pluie_3h", "uv", "rayonnement_solaire", 
        "dh_utc", "id_station", "station_name", "latitude", "longitude", "elevation",
        "name", "state", "hardware", "software"
    ]
    
    stations = df_xls2['id_station'].unique()
    mongo_documents = list(mongo_collection.find({"id_station": {"$in": list(stations)}}))
    assert mongo_documents, "Aucun document trouvé dans MongoDB pour la source XLS2."
    
    mongo_columns = [col for col in mongo_documents[0].keys() if col != '_id']
    assert sorted(mongo_columns) == sorted(expected_columns), (
        f"Pour XLS2, les colonnes MongoDB ne correspondent pas aux attendues : {mongo_columns} vs {expected_columns}"
    )
    
    expected_row_count = len(df_xls2)
    actual_row_count = len(mongo_documents)
    assert expected_row_count == actual_row_count, (
        f"Pour XLS2, le nombre de lignes ne correspond pas : Pickle ({expected_row_count}) vs MongoDB ({actual_row_count})"
    )
