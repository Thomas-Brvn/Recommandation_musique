"""
Pipeline de mise à jour de la base festivals.

Étapes :
  1. Scrape offi.fr  → data/festivals_YYYY.json
  2. Indexe          → ChromaDB (HTTP ou Persistent selon env)

Usage :
  uv run python scripts/update_festival_db.py               # scrape + index
  uv run python scripts/update_festival_db.py --scrape-only  # scrape seul
  uv run python scripts/update_festival_db.py --index-only   # index depuis JSON existant
  uv run python scripts/update_festival_db.py --year 2027
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Racine du projet (scripts/ est un niveau en dessous)
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.app.load_festival.get_festival import scrape_festivals, export_to_json
from src.app.load_festival.festival_to_vectorstore import (
    load_festivals_from_file,
    create_vector_store,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

FESTIVAL_URL = (
    "https://www.offi.fr/tendances/concerts/"
    "les-grands-festivals-musicaux-de-lete-{year}-942.html"
)


def run_scrape(year: int, output_dir: Path) -> Path:
    url = FESTIVAL_URL.format(year=year)
    log.info("Scraping : %s", url)
    start = time.monotonic()

    festivals = scrape_festivals(url)
    log.info("%d festivals trouvés (%.1fs)", len(festivals), time.monotonic() - start)

    if not festivals:
        raise RuntimeError("Aucun festival récupéré — le scraper a peut-être besoin d'une mise à jour.")

    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"festivals_{year}.json"
    filepath = export_to_json(festivals, filename)

    # Copie canonique sans l'année pour la compatibilité avec les autres scripts
    canonical = output_dir / "festivals_2026.json"
    canonical.write_text(
        json.dumps(
            [
                {
                    "nom": f.nom,
                    "dates": f.dates,
                    "lieu": f.lieu,
                    "artistes": f.artistes,
                    "billetterie": f.billetterie_url,
                }
                for f in festivals
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info("JSON sauvegardé : %s (+ %s)", filepath, canonical)
    return Path(filepath)


def run_index(json_path: Path) -> None:
    os.environ.setdefault("FESTIVALS_FILE", str(json_path))
    log.info("Indexation ChromaDB depuis %s", json_path)
    start = time.monotonic()

    festivals = load_festivals_from_file()
    create_vector_store(festivals)

    log.info("Indexation terminée (%.1fs)", time.monotonic() - start)


def main() -> None:
    parser = argparse.ArgumentParser(description="Met à jour la base de données festivals.")
    parser.add_argument("--scrape-only", action="store_true", help="Scrape uniquement, sans indexer")
    parser.add_argument("--index-only", action="store_true", help="Indexe sans scraper")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Année des festivals")
    parser.add_argument("--data-dir", default=str(ROOT / "data"), help="Répertoire de données")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    json_path = data_dir / f"festivals_{args.year}.json"

    try:
        if not args.index_only:
            json_path = run_scrape(args.year, data_dir)

        if not args.scrape_only:
            if not json_path.exists():
                raise FileNotFoundError(f"Fichier JSON introuvable : {json_path}")
            run_index(json_path)

        log.info("Pipeline terminé avec succès.")

    except Exception as exc:
        log.error("Erreur pipeline : %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
