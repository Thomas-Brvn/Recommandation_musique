#!/bin/bash

# Script de démarrage rapide pour le projet

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Recommandation Musique - Démarrage Rapide               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Que souhaitez-vous faire?"
echo ""
echo "  1. Configurer GCS (première fois)"
echo "  2. Télécharger via GCE -> GCS (Recommandé, pas de download local)"
echo "  3. Télécharger localement: MusicBrainz (~7 GB)"
echo "  4. Télécharger localement: ListenBrainz (~100 GB)"
echo "  5. Uploader les données locales vers GCS"
echo "  6. Monitorer l'instance GCE"
echo "  7. Démarrer Airflow (webserver + scheduler)"
echo "  8. Afficher les informations du projet"
echo ""
read -p "Votre choix (1-8): " choice

case $choice in
  1)
    echo ""
    echo "Configuration de GCS..."
    uv run python scripts/setup_gcs.py
    ;;
  2)
    echo ""
    echo "Téléchargement via GCE -> GCS..."
    echo "Cette méthode télécharge directement vers GCS (pas sur votre machine)"
    uv run python scripts/download_to_gcs_via_gce.py
    ;;
  3)
    echo ""
    echo "Téléchargement local: MusicBrainz..."
    uv run python scripts/download_musicbrainz.py
    ;;
  4)
    echo ""
    echo "Téléchargement local: ListenBrainz..."
    echo "ATTENTION: Ce téléchargement fait ~100 GB!"
    read -p "Continuer? (o/N): " confirm
    if [ "$confirm" = "o" ] || [ "$confirm" = "O" ]; then
      uv run python scripts/download_listenbrainz.py
    else
      echo "Annulé"
    fi
    ;;
  5)
    echo ""
    echo "Upload des données locales vers GCS..."
    uv run python scripts/upload_to_gcs.py
    ;;
  6)
    echo ""
    echo "Monitoring de l'instance GCE..."
    uv run python scripts/monitor_gce_download.py
    ;;
  7)
    echo ""
    echo "Démarrage d'Airflow..."
    echo ""
    echo "IMPORTANT: Ouvrez un DEUXIÈME terminal et lancez:"
    echo "  cd $(pwd)"
    echo "  export AIRFLOW_HOME=$(pwd)"
    echo "  uv run airflow scheduler"
    echo ""
    read -p "Appuyez sur Entrée quand le scheduler est prêt..."

    export AIRFLOW_HOME=$(pwd)
    uv run airflow webserver --port 8080
    ;;
  8)
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  Informations du projet                                  ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    echo "Structure:"
    echo "  scripts/          - Scripts Python pour GCP"
    echo "  dags/             - DAGs Airflow"
    echo "  data/             - Données téléchargées (local)"
    echo "  config/           - Configuration GCP"
    echo ""
    echo "Liens utiles:"
    echo "  Airflow UI: http://localhost:8080"
    echo "  GCS Console: https://console.cloud.google.com/storage/browser"
    echo ""
    echo "Workflow recommandé (avec GCE):"
    echo "  1. Configurer GCS (option 1)"
    echo "  2. Télécharger via GCE (option 2) - MusicBrainz + ListenBrainz"
    echo "  3. Monitorer la GCE (option 6)"
    echo "  4. L'instance GCE s'arrête automatiquement"
    echo ""
    ;;
  *)
    echo "Choix invalide"
    exit 1
    ;;
esac

echo ""
echo "Terminé!"
