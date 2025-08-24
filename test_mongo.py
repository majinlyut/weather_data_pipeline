import subprocess
import time

# 🔹 Demander l'IP publique
ip = input(" Entrez l'adresse IP publique de l'instance EC2 : ")

#  Paramètres
user = "ec2-user"
key_path = "keyprojet7.pem"

#  Requête MongoDB en ISODate bien échappée
mongo_query = (
    'db.weather.aggregate(['
    '{'
        '$match: {'
            'dh_utc: { $gte: ISODate("2024-01-01T00:00:00Z") }, '
            'pluie_1h: { $exists: true, $ne: null }, '
            'temperature: { $lte: 35 }'
        '}'
    '}, '
    '{'
        '$group: {'
            '_id: "$station_name", '
            'total_mesures: { $sum: 1 }, '
            'moyenne_pluie: { $avg: "$pluie_1h" }, '
            'min_temperature: { $min: "$temperature" }, '
            'max_temperature: { $max: "$temperature" }'
        '}'
    '}, '
    '{'
        '$sort: { moyenne_pluie: -1 }'
    '}'
    '])'
)


#  Commande SSH avec requête
remote_command = f"""
echo " Recherche du conteneur avec l'image mongo:latest..."
container_id=$(docker ps --filter "ancestor=mongo:latest" -q)
echo " Conteneur détecté : $container_id"
if [ -z "$container_id" ]; then
  echo " Aucun conteneur trouvé avec l'image mongo:latest"
else
  echo " Requête exécutée : {mongo_query}"
  docker exec -i $container_id mongosh projet7 --quiet --eval '{mongo_query}'
fi
"""

#  Chrono
start = time.time()

ssh_command = [
    "ssh",
    "-i", key_path,
    "-o", "StrictHostKeyChecking=no",
    f"{user}@{ip}",
    remote_command
]

try:
    result = subprocess.run(ssh_command, capture_output=True, text=True, timeout=60, encoding="utf-8")
    end = time.time()

    print("\n----  OUTPUT ----")
    print(result.stdout)

    if result.stderr.strip():
        print("\n----  ERREUR ----")
        print(result.stderr)

    print(f"\n⏱ Temps total d'accès à MongoDB : {end - start:.2f} secondes")

except subprocess.TimeoutExpired:
    print(" Timeout : la commande a mis trop de temps à répondre.")
except Exception as e:
    print(f" Erreur inattendue : {e}")
