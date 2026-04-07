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

def _clean_base(s: str) -> str:
    """Nettoyage de base commun à l'artiste et au titre."""
    # Normaliser les accents
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = s.lower()
    # Supprimer uniquement la ponctuation, garder les caractères unicode
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


_VARIANT_RE = re.compile(
    r'[\(\[（][^)\]）]*\b('
    r'remix|rmx|live|acoustic|radio.?edit|radio.?version|'
    r'remaster|remastered|official|video|clip|lyric|version|'
    r'edit|extended|instrumental|cover|tribute|karaoke'
    r')\b[^)\]）]*[\)\]）]',
    re.IGNORECASE
)


def normalize_title(s: str) -> str:
    """
    Normalise la partie TITRE d'un morceau.
    Supprime feat/ft, les variantes entre parenthèses et les numéros de piste.
    """
    if not isinstance(s, str):
        return ''
    # Supprimer les numéros de piste en début
    s = re.sub(r'^\s*\d{1,3}[\.\-\s]+', '', s)
    # Supprimer feat/ft et tout ce qui suit (seulement dans le titre)
    s = re.sub(r'\b(feat\.?|ft\.?|featuring)\b.*', '', s)
    # Supprimer variantes qualitatives entre parenthèses
    s = _VARIANT_RE.sub('', s)
    return _clean_base(s)


def normalize_artist(s: str) -> str:
    """
    Normalise la partie ARTISTE. Ne supprime pas feat. pour éviter de
    perdre le nom de l'artiste principal dans 'Artiste Feat. X - Titre'.
    Supprime juste le feat. et les guests (on garde l'artiste principal).
    """
    if not isinstance(s, str):
        return ''
    # Garder seulement l'artiste principal (avant feat/ft/&/,)
    s = re.sub(r'\s*(feat\.?|ft\.?|featuring)\b.*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\s*[&,]\s*.*', '', s)  # "Artist & Guest" → "Artist"
    return _clean_base(s)


def normalize(s: str) -> str:
    """
    Normalise une string complète 'Artiste - Titre'.
    Sépare artiste et titre avant d'appliquer les règles spécifiques.
    """
    if not isinstance(s, str):
        return ''
    parts = s.split(' - ', 1)
    if len(parts) == 2:
        artist = normalize_artist(parts[0])
        title  = normalize_title(parts[1])
        return f"{artist} - {title}" if artist and title else (artist or title)
    return normalize_title(s)


def blocking_key(norm: str) -> str | None:
    """
    Calcule la clé de blocage d'un titre normalisé.

    Le séparateur ' - ' divise artiste et titre. On prend le mot le plus long
    côté artiste ET le mot le plus long côté titre, pour éviter de regrouper
    deux chansons différentes du même artiste.
    """
    # Essayer de séparer artiste et titre sur ' - '
    parts = norm.split(' - ', 1)
    if len(parts) == 2:
        artist_part, track_part = parts
    else:
        # Pas de séparateur → on prend les deux moitiés
        mid = len(norm) // 2
        artist_part, track_part = norm[:mid], norm[mid:]

    def best_word(s: str) -> str | None:
        words = [w for w in s.split() if len(w) >= 4 and w not in STOP_WORDS]
        return max(words, key=len) if words else None

    artist_word = best_word(artist_part)
    track_word  = best_word(track_part)

    if artist_word and track_word:
        return f"{artist_word}_{track_word}"
    elif track_word:
        return track_word
    elif artist_word:
        return artist_word
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
    # On sépare artiste et titre AVANT normalisation pour garder la frontière.
    # La clé complète sert à détecter les doublons exacts.
    # La partie titre sert au score de similarité fuzzy.
    print("\nNormalisation des titres...")
    norm_of:       dict[str, str] = {}  # clé complète normalisée
    title_norm_of: dict[str, str] = {}  # titre seul normalisé

    for k in tqdm(track_keys, desc="Normalisation"):
        parts = k.split(' - ', 1)
        title_norm_of[k] = normalize_title(parts[1] if len(parts) == 2 else k)
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

    # Précalculer les titres normalisés par norm complet (pour lookup rapide)
    norm_to_title: dict[str, str] = {}
    for orig, norm in norm_of.items():
        if norm not in norm_to_title:
            norm_to_title[norm] = title_norm_of[orig]

    # ── 5. Similarité fuzzy dans chaque bloc ──
    # Le score est calculé sur le TITRE seul (pas artiste + titre).
    # Cela évite de fusionner deux chansons différentes du même artiste
    # parce que le nom d'artiste (souvent long) dominerait le score.
    _ROMAN = {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4, 'v': 5, 'vi': 6,
              'vii': 7, 'viii': 8, 'ix': 9, 'x': 10, 'xi': 11, 'xii': 12}
    _ROMAN_RE = re.compile(r'\b(i{1,3}|iv|vi{0,3}|ix|xi{0,2}|xii)\b')

    def extract_numbers(s: str) -> set[str]:
        """Extrait les nombres arabes ET romains, normalisés en arabes."""
        arabic  = set(re.findall(r'\d+', s))
        romans  = {str(_ROMAN[m]) for m in _ROMAN_RE.findall(s.lower())}
        return arabic | romans

    def should_merge(norm_a: str, norm_b: str, title_a: str, title_b: str) -> bool:
        """
        Décide si deux tracks normalisés doivent être fusionnés.

        Règles:
        1. Score de similarité sur les titres seuls >= threshold
        2. Si le titre est court (<= 2 mots significatifs), exiger aussi
           que la string complète (artiste + titre) soit similaire
           → évite "Alt-J - Intro" ≈ "Autre artiste - Intro"
        3. Si les deux titres contiennent des chiffres, les chiffres
           doivent être identiques
           → évite "Oxygene Part 1" ≈ "Oxygene Part 3"
        """
        # Règle 3 : chiffres (arabes ou romains) asymétriques ou différents → pas de fusion
        # "Part I" ≠ "Part II", et "Oxygene Part 1" ≠ "Oxygene" (sans numéro)
        nums_a = extract_numbers(title_a)
        nums_b = extract_numbers(title_b)
        if nums_a != nums_b:
            return False

        title_score = fuzz.token_sort_ratio(title_a, title_b)
        if title_score < threshold:
            return False

        # Règle 2 : titre court → vérifier aussi la string complète
        sig_words_a = [w for w in title_a.split() if len(w) >= 4]
        sig_words_b = [w for w in title_b.split() if len(w) >= 4]
        if len(sig_words_a) <= 1 or len(sig_words_b) <= 1:
            full_score = fuzz.token_sort_ratio(norm_a, norm_b)
            return full_score >= threshold

        return True

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

                title_a = norm_to_title.get(a, a)
                title_b = norm_to_title.get(b, b)
                if should_merge(a, b, title_a, title_b):
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
