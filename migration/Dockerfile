FROM python:3.11.9

WORKDIR /app

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le script de migration
COPY migration.py .
COPY scriptxls1.py .
COPY scriptjson.py .
COPY scriptxls2.py .
COPY test_migration.py .

# Copier le dossier "data" dans  ici dans /app/data
COPY data /app/data

