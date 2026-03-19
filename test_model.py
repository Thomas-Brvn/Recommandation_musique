#!/usr/bin/env python3
"""
Script simple pour tester le modèle de recommandation.
"""
import json
import pickle
import numpy as np
from pathlib import Path

# Chemins
BASE_DIR = Path(__file__).parent
MODELS_DIR = BASE_DIR / "models"
DATA_DIR = BASE_DIR / "data" / "processed"

def load_model():
    """Charge le modèle et les mappings."""
    print("Chargement du modèle...")

    with open(MODELS_DIR / "als_model.pkl", "rb") as f:
        state = pickle.load(f)

    # Le modèle implicit est dans la clé 'model'
    model = state['model']

    with open(DATA_DIR / "mappings.json", "r") as f:
        mappings = json.load(f)

    # Créer les mappings inverses
    id_to_track = {int(v): k for k, v in mappings["track_to_id"].items()}
    id_to_user = {int(v): k for k, v in mappings["user_to_id"].items()}

    print(f"  → {len(mappings['track_to_id']):,} pistes")
    print(f"  → {len(mappings['user_to_id']):,} utilisateurs")

    return model, mappings, id_to_track, id_to_user


def normalize(s: str) -> str:
    """Normalise une chaîne pour la déduplication."""
    import re
    s = re.sub(r'[\(\[\{][^\)\]\}]*[\)\]\}]', '', s)  # retire (xxx), [xxx], {xxx}
    return re.sub(r'[^a-z0-9]', '', s.lower())


def search_tracks(query: str, mappings: dict, limit: int = 10):
    """Recherche des pistes par nom, sans doublons."""
    query_lower = query.lower()
    results = []
    seen = set()

    for track_key, track_id in mappings["track_to_id"].items():
        if query_lower in track_key.lower():
            key = normalize(track_key)
            if key not in seen:
                seen.add(key)
                results.append((track_key, track_id))
                if len(results) >= limit:
                    break

    return results


def get_similar_tracks(model, track_id: int, id_to_track: dict, n: int = 10):
    """Trouve les pistes similaires à une piste donnée."""
    # Note: Les facteurs sont dans user_factors car le modèle a été entraîné
    # sur la matrice transposée
    track_factors = model.user_factors

    if track_id >= len(track_factors):
        return []

    # Vecteur de la piste
    track_vector = track_factors[track_id]

    # Calculer les similarités (produit scalaire normalisé = cosine)
    norms = np.linalg.norm(track_factors, axis=1)
    norms[norms == 0] = 1  # Éviter division par zéro

    similarities = track_factors @ track_vector
    similarities = similarities / (norms * np.linalg.norm(track_vector))

    # Top N (exclure la piste elle-même)
    top_indices = np.argsort(similarities)[::-1][:n+1]

    results = []
    for idx in top_indices:
        if idx != track_id and idx in id_to_track:
            results.append({
                "track": id_to_track[idx],
                "similarity": float(similarities[idx])
            })
            if len(results) >= n:
                break

    return results


def main():
    # Charger
    model, mappings, id_to_track, id_to_user = load_model()

    print("\n" + "=" * 60)
    print("TEST DU MODÈLE DE RECOMMANDATION")
    print("=" * 60)

    # Menu interactif
    while True:
        print("\nOptions:")
        print("  1. Rechercher une piste")
        print("  2. Trouver des pistes similaires")
        print("  3. Statistiques du modèle")
        print("  q. Quitter")

        choice = input("\nChoix: ").strip().lower()

        if choice == "q":
            break

        elif choice == "1":
            query = input("Recherche (artiste ou titre): ").strip()
            if query:
                results = search_tracks(query, mappings, limit=50)
                if results:
                    print(f"\n{len(results)} résultats trouvés:")
                    for i, (track, tid) in enumerate(results, 1):
                        print(f"  {i}. [{tid}] {track}")
                else:
                    print("Aucun résultat.")

        elif choice == "2":
            track_id_str = input("ID de la piste (utilise option 1 pour trouver): ").strip()
            try:
                track_id = int(track_id_str)
                if track_id in id_to_track:
                    print(f"\nPiste: {id_to_track[track_id]}")
                    print("\nPistes similaires:")
                    similar = get_similar_tracks(model, track_id, id_to_track)
                    for i, item in enumerate(similar, 1):
                        print(f"  {i}. {item['track']} (score: {item['similarity']:.3f})")
                else:
                    print("ID non trouvé.")
            except ValueError:
                print("ID invalide.")

        elif choice == "3":
            print(f"\nStatistiques:")
            print(f"  - Pistes: {len(mappings['track_to_id']):,}")
            print(f"  - Utilisateurs: {len(mappings['user_to_id']):,}")
            print(f"  - Facteurs: {model.user_factors.shape[1]}")
            print(f"  - Taille user_factors: {model.user_factors.shape}")
            print(f"  - Taille item_factors: {model.item_factors.shape}")


if __name__ == "__main__":
    main()
