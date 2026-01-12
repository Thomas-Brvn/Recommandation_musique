#!/bin/bash

# Script de démarrage rapide pour le projet

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  🎵 Recommandation Musique - Démarrage Rapide          ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Que souhaitez-vous faire?"
echo ""
echo "  1. 📦 Configurer AWS S3 (première fois)"
echo "  2. 🚀 Télécharger via EC2 → S3 (Recommandé, pas de download local)"
echo "  3. ⬇️  Télécharger localement: MusicBrainz (~7 GB)"
echo "  4. ⬇️  Télécharger localement: ListenBrainz (~100 GB)"
echo "  5. ☁️  Uploader les données locales vers S3"
echo "  6. 📊 Monitorer l'instance EC2"
echo "  7. 🚀 Démarrer Airflow (webserver + scheduler)"
echo "  8. ℹ️  Afficher les informations du projet"
echo ""
read -p "Votre choix (1-8): " choice

case $choice in
  1)
    echo ""
    echo "🔧 Configuration d'AWS S3..."
    python3 scripts/setup_aws_s3.py
    ;;
  2)
    echo ""
    echo "🚀 Téléchargement via EC2 → S3..."
    echo "💡 Cette méthode télécharge directement vers S3 (pas sur votre machine)"
    python3 scripts/download_to_s3_via_ec2.py
    ;;
  3)
    echo ""
    echo "📥 Téléchargement local: MusicBrainz..."
    python3 scripts/download_musicbrainz.py
    ;;
  4)
    echo ""
    echo "📥 Téléchargement local: ListenBrainz..."
    echo "⚠️  ATTENTION: Ce téléchargement fait ~100 GB!"
    read -p "Continuer? (o/N): " confirm
    if [ "$confirm" = "o" ] || [ "$confirm" = "O" ]; then
      python3 scripts/download_listenbrainz.py
    else
      echo "Annulé"
    fi
    ;;
  5)
    echo ""
    echo "☁️  Upload des données locales vers S3..."
    python3 scripts/upload_to_s3.py
    ;;
  6)
    echo ""
    echo "📊 Monitoring de l'instance EC2..."
    python3 scripts/monitor_ec2_download.py
    ;;
  7)
    echo ""
    echo "🚀 Démarrage d'Airflow..."
    echo ""
    echo "IMPORTANT: Ouvrez un DEUXIÈME terminal et lancez:"
    echo "  cd $(pwd)"
    echo "  source venv/bin/activate"
    echo "  export AIRFLOW_HOME=$(pwd)"
    echo "  airflow scheduler"
    echo ""
    read -p "Appuyez sur Entrée quand le scheduler est prêt..."

    source venv/bin/activate
    export AIRFLOW_HOME=$(pwd)
    airflow webserver --port 8080
    ;;
  8)
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  📊 Informations du projet                              ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    echo "📂 Structure:"
    echo "  • scripts/          - Scripts Python pour AWS"
    echo "  • dags/            - DAGs Airflow"
    echo "  • data/            - Données téléchargées (local)"
    echo "  • config/          - Configuration AWS"
    echo ""
    echo "📚 Guides disponibles:"
    echo "  • README.md        - Guide général"
    echo "  • GUIDE_AWS.md     - Guide AWS S3"
    echo "  • GUIDE_EC2.md     - Guide EC2 (téléchargement direct)"
    echo ""
    echo "🔗 Liens utiles:"
    echo "  • Airflow UI: http://localhost:8080"
    echo "  • AWS Console: https://console.aws.amazon.com/s3"
    echo ""
    echo "💡 Workflow recommandé (avec EC2):"
    echo "  1. Configurer AWS S3 (option 1)"
    echo "  2. Télécharger via EC2 (option 2) - MusicBrainz + ListenBrainz"
    echo "  3. Monitorer l'EC2 (option 6)"
    echo "  4. Terminer l'instance EC2 quand c'est fini"
    echo ""
    echo "💡 Workflow alternatif (local):"
    echo "  1. Configurer AWS S3 (option 1)"
    echo "  2. Télécharger localement (options 3 ou 4)"
    echo "  3. Uploader vers S3 (option 5)"
    echo ""
    ;;
  *)
    echo "❌ Choix invalide"
    exit 1
    ;;
esac

echo ""
echo "✅ Terminé!"