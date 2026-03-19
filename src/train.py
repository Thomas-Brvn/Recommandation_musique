#!/usr/bin/env python3
"""
Script d'entraînement du modèle ALS.
"""
import argparse
import time
from pathlib import Path

import numpy as np
from scipy import sparse

from models.als_model import ALSRecommender

# Configuration par défaut
DATA_DIR = Path(__file__).parent.parent / "data" / "processed"
MODELS_DIR = Path(__file__).parent.parent / "models"

DEFAULT_MATRIX = DATA_DIR / "user_item_matrix.npz"
DEFAULT_USER_MAPPING = DATA_DIR / "user_mapping.json"
DEFAULT_ITEM_MAPPING = DATA_DIR / "item_mapping.json"
DEFAULT_MODEL_OUTPUT = MODELS_DIR / "als_model.pkl"


def train_model(
    matrix_path: Path = DEFAULT_MATRIX,
    user_mapping_path: Path = DEFAULT_USER_MAPPING,
    item_mapping_path: Path = DEFAULT_ITEM_MAPPING,
    output_path: Path = DEFAULT_MODEL_OUTPUT,
    factors: int = 128,
    regularization: float = 0.01,
    iterations: int = 15,
    use_gpu: bool = False
) -> ALSRecommender:
    """
    Entraîne le modèle ALS sur les données préparées.

    Args:
        matrix_path: Chemin vers la matrice sparse .npz
        user_mapping_path: Chemin vers le mapping utilisateurs
        item_mapping_path: Chemin vers le mapping items
        output_path: Chemin de sortie pour le modèle
        factors: Nombre de facteurs latents
        regularization: Terme de régularisation
        iterations: Nombre d'itérations
        use_gpu: Utiliser CUDA

    Returns:
        Modèle entraîné
    """
    print("=" * 60)
    print("ENTRAÎNEMENT DU MODÈLE ALS")
    print("=" * 60)

    # Charger la matrice
    print(f"\nChargement de la matrice: {matrix_path}")
    user_item_matrix = sparse.load_npz(matrix_path)
    print(f"Dimensions: {user_item_matrix.shape[0]:,} users × {user_item_matrix.shape[1]:,} items")
    print(f"Interactions: {user_item_matrix.nnz:,}")

    # Créer le modèle
    print(f"\nConfiguration du modèle:")
    print(f"  - Facteurs latents: {factors}")
    print(f"  - Régularisation: {regularization}")
    print(f"  - Itérations: {iterations}")
    print(f"  - GPU: {'Oui' if use_gpu else 'Non'}")

    recommender = ALSRecommender(
        factors=factors,
        regularization=regularization,
        iterations=iterations,
        use_gpu=use_gpu
    )

    # Charger les mappings
    if user_mapping_path.exists() and item_mapping_path.exists():
        print("\nChargement des mappings...")
        recommender.load_mappings(user_mapping_path, item_mapping_path)
        print(f"  - {len(recommender.user_mapping):,} utilisateurs")
        print(f"  - {len(recommender.item_mapping):,} tracks")

    # Entraîner
    print("\n" + "=" * 60)
    start_time = time.time()
    recommender.fit(user_item_matrix, show_progress=True)
    training_time = time.time() - start_time
    print(f"\nTemps d'entraînement: {training_time:.1f}s")

    # Sauvegarder
    print("\n" + "=" * 60)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    recommender.save(output_path)

    # Test rapide
    print("\n" + "=" * 60)
    print("TEST RAPIDE")
    print("=" * 60)

    # Sélectionner un utilisateur actif (avec beaucoup d'écoutes)
    user_activity = np.diff(user_item_matrix.indptr)
    active_users = np.argsort(user_activity)[-10:]  # Top 10 users les plus actifs

    test_user = active_users[0]
    print(f"\nRecommandations pour l'utilisateur {test_user} ({user_activity[test_user]} écoutes):")

    recommendations = recommender.recommend_with_names(test_user, n=5)
    for i, rec in enumerate(recommendations, 1):
        print(f"  {i}. {rec['track']} (score: {rec['score']:.3f})")

    # Similaires
    print(f"\nTracks similaires au premier résultat:")
    if recommendations:
        similar = recommender.similar_items(recommendations[0]['item_id'], n=3)
        for item_id, score in similar:
            track_name = recommender.get_track_name(item_id)
            print(f"  - {track_name} (score: {score:.3f})")

    print("\n" + "=" * 60)
    print("ENTRAÎNEMENT TERMINÉ")
    print("=" * 60)
    print(f"Modèle sauvegardé: {output_path}")

    return recommender


def main():
    parser = argparse.ArgumentParser(description="Entraîner le modèle ALS")
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX,
                       help="Matrice user-item (.npz)")
    parser.add_argument("--user-mapping", type=Path, default=DEFAULT_USER_MAPPING,
                       help="Mapping utilisateurs (.json)")
    parser.add_argument("--item-mapping", type=Path, default=DEFAULT_ITEM_MAPPING,
                       help="Mapping items (.json)")
    parser.add_argument("--output", type=Path, default=DEFAULT_MODEL_OUTPUT,
                       help="Fichier de sortie du modèle")
    parser.add_argument("--factors", type=int, default=128,
                       help="Nombre de facteurs latents")
    parser.add_argument("--regularization", type=float, default=0.01,
                       help="Terme de régularisation")
    parser.add_argument("--iterations", type=int, default=15,
                       help="Nombre d'itérations")
    parser.add_argument("--gpu", action="store_true",
                       help="Utiliser CUDA")

    args = parser.parse_args()

    train_model(
        matrix_path=args.matrix,
        user_mapping_path=args.user_mapping,
        item_mapping_path=args.item_mapping,
        output_path=args.output,
        factors=args.factors,
        regularization=args.regularization,
        iterations=args.iterations,
        use_gpu=args.gpu
    )


if __name__ == "__main__":
    main()
