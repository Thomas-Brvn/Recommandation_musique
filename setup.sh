#!/bin/bash

# Script d'installation et configuration du projet

echo "=== Configuration du projet Recommandation Musique ==="

# 1. Installer les dépendances avec Airflow
echo "Installation des dépendances (avec Airflow)..."
uv sync --extra airflow

# 2. Définir le répertoire Airflow
export AIRFLOW_HOME=$(pwd)
echo "AIRFLOW_HOME défini à: $AIRFLOW_HOME"

# 3. Initialiser la base de données Airflow
echo "Initialisation de la base de données Airflow..."
uv run airflow db migrate

# 4. Créer un utilisateur admin
echo "Création de l'utilisateur admin..."
uv run airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo ""
echo "=== Configuration terminée! ==="
echo ""
echo "Pour démarrer Airflow:"
echo "  1. Terminal 1: export AIRFLOW_HOME=$(pwd) && uv run airflow webserver --port 8080"
echo "  2. Terminal 2: export AIRFLOW_HOME=$(pwd) && uv run airflow scheduler"
echo ""
echo "Accédez ensuite à: http://localhost:8080"
echo "Login: admin / Password: admin"
