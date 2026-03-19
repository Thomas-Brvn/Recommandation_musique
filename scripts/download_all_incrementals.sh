#!/bin/bash
# Script pour telecharger tous les dumps incrementaux ListenBrainz

OUTPUT_DIR="/tmp/listenbrainz_incremental/all"
mkdir -p "$OUTPUT_DIR"

BASE_URL="https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental"

# Liste des dumps disponibles (16 dec 2025 - 14 jan 2026)
DUMPS=(
    "listenbrainz-dump-2365-20251216-000003-incremental"
    "listenbrainz-dump-2366-20251217-000003-incremental"
    "listenbrainz-dump-2367-20251218-000003-incremental"
    "listenbrainz-dump-2368-20251219-000003-incremental"
    "listenbrainz-dump-2369-20251220-000003-incremental"
    "listenbrainz-dump-2370-20251221-000003-incremental"
    "listenbrainz-dump-2371-20251222-000003-incremental"
    "listenbrainz-dump-2372-20251223-000003-incremental"
    "listenbrainz-dump-2373-20251224-000003-incremental"
    "listenbrainz-dump-2374-20251225-000003-incremental"
    "listenbrainz-dump-2375-20251226-000003-incremental"
    "listenbrainz-dump-2376-20251227-000003-incremental"
    "listenbrainz-dump-2377-20251228-000003-incremental"
    "listenbrainz-dump-2378-20251229-000003-incremental"
    "listenbrainz-dump-2379-20251230-000003-incremental"
    "listenbrainz-dump-2380-20251231-000003-incremental"
    "listenbrainz-dump-2381-20260101-000003-incremental"
    "listenbrainz-dump-2383-20260102-000003-incremental"
    "listenbrainz-dump-2384-20260103-000003-incremental"
    "listenbrainz-dump-2385-20260104-000003-incremental"
    "listenbrainz-dump-2386-20260105-000003-incremental"
    "listenbrainz-dump-2387-20260106-000003-incremental"
    "listenbrainz-dump-2388-20260107-000003-incremental"
    "listenbrainz-dump-2389-20260108-000003-incremental"
    "listenbrainz-dump-2390-20260109-000003-incremental"
    "listenbrainz-dump-2391-20260110-000003-incremental"
    "listenbrainz-dump-2392-20260111-000003-incremental"
    "listenbrainz-dump-2393-20260112-000003-incremental"
    "listenbrainz-dump-2394-20260113-000003-incremental"
    "listenbrainz-dump-2395-20260114-000003-incremental"
)

echo "=========================================="
echo "Telechargement de ${#DUMPS[@]} dumps incrementaux"
echo "=========================================="

TOTAL=${#DUMPS[@]}
COUNT=0

for DUMP in "${DUMPS[@]}"; do
    COUNT=$((COUNT + 1))
    FILENAME="listenbrainz-listens-dump-${DUMP#listenbrainz-dump-}.tar.zst"
    URL="$BASE_URL/$DUMP/$FILENAME"
    OUTPUT_FILE="$OUTPUT_DIR/$FILENAME"

    if [ -f "$OUTPUT_FILE" ]; then
        echo "[$COUNT/$TOTAL] $FILENAME existe deja, skip"
        continue
    fi

    echo "[$COUNT/$TOTAL] Telechargement de $FILENAME..."
    curl -# -o "$OUTPUT_FILE" "$URL"

    if [ $? -ne 0 ]; then
        echo "ERREUR: Echec du telechargement de $FILENAME"
        rm -f "$OUTPUT_FILE"
    fi
done

echo "=========================================="
echo "Telechargement termine"
echo "=========================================="
ls -lh "$OUTPUT_DIR"
du -sh "$OUTPUT_DIR"