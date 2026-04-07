#!/usr/bin/env python3
"""
Déduplication des titres de tracks par blocking + score de similarité.

Logique:
1. Normalise chaque titre (minuscule, sans accents, sans feat/remix/live...)
2. Groupe les candidats par mots-clés communs (blocking) pour éviter O(n²)
3. Calcule un score de similarité uniquement dans chaque bloc
4. Fusionne les titres similaires (seuil configurable, défaut 88)
5. Produit un mapping old_key → canonical_key à appliquer dans aggregate_data.py

Exemple:
  "gims - ciel"         ┐
  "ciel maitre gims"    ├─ → "gims - ciel"  (canonical = plus fréquent)
  "Ciel---GIMS"         ┘

Usage:
  python scripts/deduplicate_tracks.py
  python scripts/deduplicate_tracks.py --threshold 85 --max-block-size 300
"""
import json
import re
import unicodedata
import argparse
from collections import defaultdict
from pathlib import Path

from rapidfuzz import fuzz
from tqdm import tqdm

PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
MAPPINGS_FILE  = PROCESSED_DIR / "mappings.json"
OUTPUT_FILE    = PROCESSED_DIR / "track_dedup_map.json"

DEFAULT_THRESHOLD    = 88   # Score minimum pour fusionner (0-100)
DEFAULT_MAX_BLOCK    = 500  # Taille max d'un bloc (au-delà on skip)

STOP_WORDS = {
    'the', 'and', 'for', 'you', 'are', 'this', 'that', 'with', 'have',
    'from', 'not', 'but', 'all', 'can', 'was', 'one', 'get', 'its',
    'who', 'she', 'him', 'his', 'my', 'is', 'it', 'in', 'of', 'to', 'a', 'i',
    'les', 'des', 'une', 'est', 'par', 'sur', 'dans', 'que', 'qui', 'pas',
    'son', 'mon', 'ton', 'nos', 'ses', 'ces', 'aux', 'avec',
}


# ──────────────────────────────────────────────
# Normalisation
# ──────────────────────────────────────────────

def normalize(s: str) -> str:
    """
    Normalise un titre pour la comparaison.

    - Supprime les accents
    - Minuscule
    - Supprime les numéros de piste (01., 05. ...)
    - Supprime feat/ft et ce qui suit
    - Supprime les parenthèses contenant des variantes connues
      (remix, live, acoustic, radio edit, remaster, official, video)
    - Remplace les séparateurs (-, —, |, ·, _) par des espaces
    - Garde uniquement les caractères alphanumériques
    """
    if not isinstance(s, str):
        return ''

    # Normaliser les accents
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')

    s = s.lower()

    # Supprimer les numéros de piste en début de string
    s = re.sub(r'^\s*\d{1,3}[\.\-\s]+', '', s)

    # Supprimer feat/ft et tout ce qui suit
    s = re.sub(r'\b(feat\.?|ft\.?|featuring)\b.*', '', s)

    # Supprimer parenthèses/crochets contenant des variantes qualitatives
    variant_pattern = re.compile(
        r'[\(\[（][^)\]）]*\b('
        r'remix|rmx|live|acoustic|radio.?edit|radio.?version|'
        r'remaster|remastered|official|video|clip|lyric|version|'
        r'edit|extended|instrumental|cover|tribute|karaoke'
        r')\b[^)\]）]*[\)\]）]',
        re.IGNORECASE
    )
    s = variant_pattern.sub('', s)

    # Remplacer séparateurs par espace
    s = re.sub(r'[-—|·_/\\]+', ' ', s)

    # Garder uniquement alphanumériques + espace
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)

    # Normaliser les espaces
    return re.sub(r'\s+', ' ', s).strip()


def blocking_key(norm: str) -> str | None:
    """
    Calcule la clé de blocage d'un titre normalisé.

    Utilise les 2 mots les plus longs (>= 5 chars, hors stop words)
    pour regrouper les candidats potentiels. L'idée est que deux titres
    doublons partagent probablement au moins 2 mots significatifs.
    """
    words = [w for w in norm.split() if len(w) >= 5 and w not in STOP_WORDS]

    if len(words) >= 2:
        # Trier pour que "ciel gims" et "gims ciel" donnent la même clé
        return '_'.join(sorted(words)[:2])
    elif len(words) == 1:
        return words[0]
    return None


# ──────────────────────────────────────────────
# Union-Find
# ──────────────────────────────────────────────

class UnionFind:
    def __init__(self):
        self._parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]  # path compression
            x = self._parent[x]
        return x

    def union(self, a: str, b: str):
        pa, pb = self.find(a), self.find(b)
        if pa != pb:
            self._parent[pb] = pa

    def clusters(self) -> dict[str, list[str]]:
        result = defaultdict(list)
        for x in self._parent:
            result[self.find(x)].append(x)
        return dict(result)


# ──────────────────────────────────────────────
# Pipeline principal
# ──────────────────────────────────────────────

def deduplicate_tracks(
    mappings_file: Path = MAPPINGS_FILE,
    output_file:   Path = OUTPUT_FILE,
    threshold:     int  = DEFAULT_THRESHOLD,
    max_block_size: int = DEFAULT_MAX_BLOCK,
) -> dict[str, str]:

    print("=" * 60)
    print("DÉDUPLICATION DES TRACKS")
    print("=" * 60)
    print(f"Seuil de similarité : {threshold}")
    print(f"Taille max de bloc  : {max_block_size}")

    # ── 1. Charger les tracks ──────────────────
    print(f"\nChargement de {mappings_file}...")
    with open(mappings_file, encoding='utf-8') as f:
        mappings = json.load(f)

    # Trier par ID pour que le canonical soit la version "la plus ancienne"
    track_to_id: dict[str, int] = mappings['track_to_id']
    track_keys = sorted(track_to_id, key=lambda k: track_to_id[k])
    print(f"Tracks à traiter : {len(track_keys):,}")

    uf = UnionFind()

    # ── 2. Normaliser ─────────────────────────
    print("\nNormalisation des titres...")
    norm_of: dict[str, str] = {}
    for k in tqdm(track_keys, desc="Normalisation"):
        norm_of[k] = normalize(k)

    # ── 3. Fusions exactes (même string normalisé) ─
    print("\nFusions exactes (même normalisation)...")
    norm_to_originals: dict[str, list[str]] = defaultdict(list)
    for orig in track_keys:
        norm_to_originals[norm_of[orig]].append(orig)

    exact_count = 0
    for originals in norm_to_originals.values():
        if len(originals) >= 2:
            canonical = originals[0]  # le plus ancien (trié par ID)
            for other in originals[1:]:
                uf.union(canonical, other)
                exact_count += 1

    print(f"  Fusions exactes : {exact_count:,}")

    # ── 4. Blocking ───────────────────────────
    print("\nConstruction des blocs...")
    blocks: dict[str, list[str]] = defaultdict(list)

    # On travaille sur les normes uniques pour éviter les doublons dans les blocs
    unique_norms = list(norm_to_originals.keys())
    for norm in tqdm(unique_norms, desc="Blocking"):
        bk = blocking_key(norm)
        if bk:
            blocks[bk].append(norm)

    candidate_blocks = {k: v for k, v in blocks.items() if 2 <= len(v) <= max_block_size}
    skipped_blocks   = sum(1 for v in blocks.values() if len(v) > max_block_size)

    total_comparisons = sum(len(v) * (len(v) - 1) // 2 for v in candidate_blocks.values())
    print(f"  Blocs candidats     : {len(candidate_blocks):,}")
    print(f"  Blocs trop grands   : {skipped_blocks} (ignorés)")
    print(f"  Comparaisons prévues: {total_comparisons:,}")

    # ── 5. Similarité fuzzy dans chaque bloc ──
    print(f"\nComparaisons fuzzy...")
    seen_pairs: set[tuple[str, str]] = set()
    fuzzy_count = 0

    for norms in tqdm(candidate_blocks.values(), desc="Similarité"):
        for i in range(len(norms)):
            for j in range(i + 1, len(norms)):
                a, b = norms[i], norms[j]
                pair = (a, b) if a < b else (b, a)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                # token_sort_ratio gère les réordonnements de mots
                score = fuzz.token_sort_ratio(a, b)
                if score >= threshold:
                    orig_a = norm_to_originals[a][0]
                    orig_b = norm_to_originals[b][0]
                    uf.union(orig_a, orig_b)
                    fuzzy_count += 1

    print(f"  Paires fusionnées (fuzzy) : {fuzzy_count:,}")

    # ── 6. Construire le mapping final ────────
    print("\nConstruction du mapping final...")
    clusters = uf.clusters()

    # mapping : chaque track_key → sa canonical key
    # canonical = l'élément dont le parent Union-Find est lui-même = le plus ancien
    dedup_map: dict[str, str] = {}
    merged_tracks = 0

    for canonical, members in clusters.items():
        if len(members) >= 2:
            merged_tracks += len(members) - 1
        for member in members:
            if member != canonical:
                dedup_map[member] = canonical
            # (les canonicals ne sont pas inclus dans le mapping = pas de redirect)

    # ── 7. Statistiques ───────────────────────
    print(f"\n{'=' * 60}")
    print("RÉSULTATS")
    print(f"{'=' * 60}")
    print(f"Tracks originales        : {len(track_keys):,}")
    print(f"Tracks fusionnées        : {merged_tracks:,}")
    print(f"Tracks uniques après dedup: {len(track_keys) - merged_tracks:,}")
    print(f"Réduction                : {merged_tracks / len(track_keys) * 100:.1f}%")

    # Exemples de clusters fusionnés
    print("\nExemples de clusters fusionnés:")
    shown = 0
    for canonical, members in sorted(clusters.items(), key=lambda x: -len(x[1])):
        if len(members) >= 3 and shown < 5:
            print(f"  Canonical: {repr(canonical)}")
            for m in members[:4]:
                print(f"    ← {repr(m)}")
            shown += 1

    # ── 8. Sauvegarder ────────────────────────
    output_file.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nSauvegarde vers {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(dedup_map, f, ensure_ascii=False)

    size_mb = output_file.stat().st_size / 1024 / 1024
    print(f"Fichier : {size_mb:.1f} MB  ({len(dedup_map):,} redirections)")

    return dedup_map


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Déduplication des tracks par similarité")
    parser.add_argument("--input",          type=Path, default=MAPPINGS_FILE)
    parser.add_argument("--output",         type=Path, default=OUTPUT_FILE)
    parser.add_argument("--threshold",      type=int,  default=DEFAULT_THRESHOLD,
                        help="Score de similarité minimum (0-100, défaut 88)")
    parser.add_argument("--max-block-size", type=int,  default=DEFAULT_MAX_BLOCK,
                        help="Taille max d'un bloc avant ignoré (défaut 500)")
    args = parser.parse_args()

    deduplicate_tracks(
        mappings_file=args.input,
        output_file=args.output,
        threshold=args.threshold,
        max_block_size=args.max_block_size,
    )
