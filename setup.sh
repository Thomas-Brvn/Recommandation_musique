#!/bin/bash

# Script d'installation et configuration du projet

echo "=== Configuration du projet Recommandation Musique ==="

# 1. Activer l'environnement virtuel
echo "Activation de l'environnement virtuel..."
source venv/bin/activate

# 2. Définir le répertoire Airflow
export AIRFLOW_HOME=$(pwd)
echo "AIRFLOW_HOME défini à: $AIRFLOW_HOME"

# 3. Initialiser la base de données Airflow
echo "Initialisation de la base de données Airflow..."
airflow db migrate

# 4. Créer un utilisateur admin
echo "Création de l'utilisateur admin..."
airflow users create \
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
echo "  1. Terminal 1: source venv/bin/activate && export AIRFLOW_HOME=$(pwd) && airflow webserver --port 8080"
echo "  2. Terminal 2: source venv/bin/activate && export AIRFLOW_HOME=$(pwd) && airflow scheduler"
echo ""
echo "Accédez ensuite à: http://localhost:8080"
echo "Login: admin / Password: admin"