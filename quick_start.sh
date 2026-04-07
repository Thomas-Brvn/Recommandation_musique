#!/bin/bash

# Script de démarrage rapide pour le projet

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Recommandation Musique - Démarrage Rapide               ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Que souhaitez-vous faire?"
echo ""
echo "  1. Configurer AWS S3 (première fois)"
echo "  2. Télécharger via EC2 -> S3 (Recommandé, pas de download local)"
echo "  3. Télécharger localement: MusicBrainz (~7 GB)"
echo "  4. Télécharger localement: ListenBrainz (~100 GB)"
echo "  5. Uploader les données locales vers S3"
echo "  6. Monitorer l'instance EC2"
echo "  7. Démarrer Airflow (webserver + scheduler)"
echo "  8. Afficher les informations du projet"
echo ""
read -p "Votre choix (1-8): " choice

case $choice in
  1)
    echo ""
    echo "Configuration d'AWS S3..."
    uv run python scripts/setup_aws_s3.py
    ;;
  2)
    echo ""
    echo "Téléchargement via EC2 -> S3..."
    echo "Cette méthode télécharge directement vers S3 (pas sur votre machine)"
    uv run python scripts/download_to_s3_via_ec2.py
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
    echo "Upload des données locales vers S3..."
    uv run python scripts/upload_to_s3.py
    ;;
  6)
    echo ""
    echo "Monitoring de l'instance EC2..."
    uv run python scripts/monitor_ec2_download.py
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
    echo "  scripts/          - Scripts Python pour AWS"
    echo "  dags/             - DAGs Airflow"
    echo "  data/             - Données téléchargées (local)"
    echo "  config/           - Configuration AWS"
    echo ""
    echo "Guides disponibles:"
    echo "  README.md         - Guide général"
    echo "  docs/GUIDE_AWS.md - Guide AWS S3"
    echo "  docs/GUIDE_EC2.md - Guide EC2 (téléchargement direct)"
    echo ""
    echo "Liens utiles:"
    echo "  Airflow UI: http://localhost:8080"
    echo "  AWS Console: https://console.aws.amazon.com/s3"
    echo ""
    echo "Workflow recommandé (avec EC2):"
    echo "  1. Configurer AWS S3 (option 1)"
    echo "  2. Télécharger via EC2 (option 2) - MusicBrainz + ListenBrainz"
    echo "  3. Monitorer l'EC2 (option 6)"
    echo "  4. Terminer l'instance EC2 quand c'est fini"
    echo ""
    ;;
  *)
    echo "Choix invalide"
    exit 1
    ;;
esac

echo ""
echo "Terminé!"
