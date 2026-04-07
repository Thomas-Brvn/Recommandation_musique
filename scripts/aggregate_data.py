#!/usr/bin/env python3
"""
Script pour agréger les données d'écoutes en un dataset final.
Crée le fichier listens.parquet avec les colonnes:
user_id, track_id, artist_id, timestamp, play_count
"""
import os
from pathlib import Path
from typing import Tuple

import re
import unicodedata

import pandas as pd
import numpy as np
from tqdm import tqdm


def normalize_track_string(artist: str, track: str) -> str:
    """
    Normalise un titre pour regrouper les variations d'un même morceau.
    Ex: "Ciel---GIMS", "ciel  maitre gims", "GIMS - Ciel" → même clé.
    """
    def clean(s: str) -> str:
        if not isinstance(s, str):
            return ''
        # Normaliser les accents (é → e)
        s = unicodedata.normalize('NFD', s)
        s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
        # Minuscule
        s = s.lower()
        # Remplacer tout ce qui n'est pas alphanumérique par un espace
        s = re.sub(r'[^a-z0-9]+', ' ', s)
        return s.strip()

    artist_clean = clean(artist)
    track_clean = clean(track)
    # Trier les tokens pour gérer "gims ciel" vs "ciel gims"
    tokens = sorted(artist_clean.split() + track_clean.split())
    return ' '.join(tokens)

# Configuration
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
INPUT_FILE = PROCESSED_DIR / "listens_raw.parquet"
OUTPUT_FILE = PROCESSED_DIR / "listens.parquet"


def create_id_mapping(series: pd.Series, prefix: str = "") -> Tuple[dict, dict]:
    """
    Crée un mapping bidirectionnel entre les valeurs et des IDs numériques.

    Returns:
        (value_to_id, id_to_value)
    """
    unique_values = series.dropna().unique()
    value_to_id = {v: i for i, v in enumerate(unique_values)}
    id_to_value = {i: v for v, i in value_to_id.items()}
    return value_to_id, id_to_value


def aggregate_listens(
    input_file: Path = INPUT_FILE,
    output_file: Path = OUTPUT_FILE,
    min_user_listens: int = 5,
    min_track_listens: int = 3
) -> Tuple[pd.DataFrame, dict, dict, dict]:
    """
    Agrège les écoutes et crée le dataset final.

    Args:
        input_file: Fichier parquet des écoutes brutes
        output_file: Fichier parquet de sortie
        min_user_listens: Minimum d'écoutes par utilisateur
        min_track_listens: Minimum d'écoutes par track

    Returns:
        (DataFrame agrégé, user_mapping, track_mapping, artist_mapping)
    """
    print("=" * 60)
    print("Agrégation des données d'écoutes")
    print("=" * 60)

    # Charger les données
    print(f"\nChargement de {input_file}...")
    df = pd.read_parquet(input_file)
    print(f"Écoutes chargées: {len(df):,}")

    # Nettoyer les données
    print("\nNettoyage des données...")
    initial_count = len(df)

    # Supprimer les lignes sans user ou track
    df = df.dropna(subset=['user_name', 'track_name'])

    # Créer une clé unique pour les tracks:
    # - Priorité 1: recording_mbid (identifiant stable MusicBrainz)
    # - Priorité 2: string normalisé (regroupe les variations de titre)
    has_mbid = df['recording_mbid'].notna() & (df['recording_mbid'] != '')
    df['track_key'] = df['recording_mbid'].where(has_mbid)
    fallback_mask = ~has_mbid
    df.loc[fallback_mask, 'track_key'] = df[fallback_mask].apply(
        lambda r: normalize_track_string(r['artist_name'], r['track_name']), axis=1
    )
    mbid_pct = has_mbid.mean() * 100
    print(f"  {mbid_pct:.1f}% des écoutes ont un recording_mbid")

    print(f"Après nettoyage: {len(df):,} ({len(df)/initial_count*100:.1f}%)")

    # Filtrer les utilisateurs avec peu d'écoutes
    print(f"\nFiltrage des utilisateurs (min {min_user_listens} écoutes)...")
    user_counts = df['user_name'].value_counts()
    valid_users = user_counts[user_counts >= min_user_listens].index
    df = df[df['user_name'].isin(valid_users)]
    print(f"Utilisateurs conservés: {len(valid_users):,}")

    # Filtrer les tracks avec peu d'écoutes
    print(f"Filtrage des tracks (min {min_track_listens} écoutes)...")
    track_counts = df['track_key'].value_counts()
    valid_tracks = track_counts[track_counts >= min_track_listens].index
    df = df[df['track_key'].isin(valid_tracks)]
    print(f"Tracks conservés: {len(valid_tracks):,}")

    # Créer les mappings
    print("\nCréation des mappings ID...")
    user_to_id, id_to_user = create_id_mapping(df['user_name'])
    track_to_id, id_to_track = create_id_mapping(df['track_key'])
    artist_to_id, id_to_artist = create_id_mapping(df['artist_name'])

    # Appliquer les mappings
    df['user_id'] = df['user_name'].map(user_to_id)
    df['track_id'] = df['track_key'].map(track_to_id)
    df['artist_id'] = df['artist_name'].map(artist_to_id)

    # Agréger: compter les écoutes par (user, track)
    print("\nAgrégation des play counts...")
    agg_df = df.groupby(['user_id', 'track_id', 'artist_id']).agg({
        'listened_at': ['min', 'max', 'count']
    }).reset_index()

    # Aplatir les colonnes multi-index
    agg_df.columns = ['user_id', 'track_id', 'artist_id',
                      'first_listen', 'last_listen', 'play_count']

    # Statistiques finales
    print(f"\n{'=' * 60}")
    print("STATISTIQUES FINALES")
    print(f"{'=' * 60}")
    print(f"Interactions (user, track): {len(agg_df):,}")
    print(f"Utilisateurs uniques: {agg_df['user_id'].nunique():,}")
    print(f"Tracks uniques: {agg_df['track_id'].nunique():,}")
    print(f"Artistes uniques: {agg_df['artist_id'].nunique():,}")
    print(f"Play count moyen: {agg_df['play_count'].mean():.2f}")
    print(f"Play count médian: {agg_df['play_count'].median():.0f}")
    print(f"Play count max: {agg_df['play_count'].max():,}")

    # Sparsité de la matrice
    n_users = agg_df['user_id'].nunique()
    n_tracks = agg_df['track_id'].nunique()
    sparsity = 1 - (len(agg_df) / (n_users * n_tracks))
    print(f"Sparsité de la matrice: {sparsity*100:.4f}%")

    # Sauvegarder le dataset
    print(f"\nSauvegarde vers {output_file}...")
    agg_df.to_parquet(output_file, index=False)

    # Sauvegarder les mappings
    mappings = {
        'user': {'to_id': user_to_id, 'to_name': id_to_user},
        'track': {'to_id': track_to_id, 'to_name': id_to_track},
        'artist': {'to_id': artist_to_id, 'to_name': id_to_artist}
    }

    import json
    mappings_file = PROCESSED_DIR / "mappings.json"

    # Convertir les clés int en str pour JSON
    mappings_json = {
        'user_to_id': user_to_id,
        'id_to_user': {str(k): v for k, v in id_to_user.items()},
        'track_to_id': track_to_id,
        'id_to_track': {str(k): v for k, v in id_to_track.items()},
        'artist_to_id': artist_to_id,
        'id_to_artist': {str(k): v for k, v in id_to_artist.items()}
    }

    with open(mappings_file, 'w', encoding='utf-8') as f:
        json.dump(mappings_json, f, ensure_ascii=False)

    print(f"Mappings sauvegardés: {mappings_file}")

    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"Taille du fichier: {file_size_mb:.1f} MB")

    return agg_df, mappings


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Agréger les données d'écoutes")
    parser.add_argument("--input", type=Path, default=INPUT_FILE,
                       help="Fichier parquet d'entrée")
    parser.add_argument("--output", type=Path, default=OUTPUT_FILE,
                       help="Fichier parquet de sortie")
    parser.add_argument("--min-user-listens", type=int, default=5,
                       help="Minimum d'écoutes par utilisateur")
    parser.add_argument("--min-track-listens", type=int, default=3,
                       help="Minimum d'écoutes par track")

    args = parser.parse_args()

    aggregate_listens(
        input_file=args.input,
        output_file=args.output,
        min_user_listens=args.min_user_listens,
        min_track_listens=args.min_track_listens
    )


if __name__ == "__main__":
    main()
