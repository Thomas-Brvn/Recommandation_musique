#!/bin/bash

# Script pour démarrer Airflow facilement

export AIRFLOW_HOME=$(pwd)

echo "=== Démarrage d'Airflow ==="
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo ""
echo "Ouvrez un DEUXIÈME terminal et lancez:"
echo "  cd $AIRFLOW_HOME"
echo "  export AIRFLOW_HOME=$AIRFLOW_HOME"
echo "  uv run airflow scheduler"
echo ""
echo "Démarrage du webserver..."
uv run airflow webserver --port 8080
