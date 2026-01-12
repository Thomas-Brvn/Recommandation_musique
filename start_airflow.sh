#!/bin/bash

# Script pour démarrer Airflow facilement

export AIRFLOW_HOME=$(pwd)
source venv/bin/activate

echo "=== Démarrage d'Airflow ==="
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo ""
echo "Ouvrez un DEUXIÈME terminal et lancez:"
echo "  cd $AIRFLOW_HOME"
echo "  source venv/bin/activate"
echo "  export AIRFLOW_HOME=$AIRFLOW_HOME"
echo "  airflow scheduler"
echo ""
echo "Démarrage du webserver..."
airflow webserver --port 8080