#!/usr/bin/env python3
"""
Script pour construire la matrice sparse user-item (CSR format).
Applique la normalisation log(1 + play_count) pour les implicit feedbacks.
"""
import json
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
from scipy import sparse
from tqdm import tqdm

# Configuration
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
INPUT_FILE = PROCESSED_DIR / "listens.parquet"
OUTPUT_MATRIX = PROCESSED_DIR / "user_item_matrix.npz"
OUTPUT_USER_MAPPING = PROCESSED_DIR / "user_mapping.json"
OUTPUT_ITEM_MAPPING = PROCESSED_DIR / "item_mapping.json"


def load_mappings(mappings_file: Path) -> Tuple[dict, dict]:
    """Charge les mappings depuis le fichier JSON."""
    with open(mappings_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Convertir les clés str en int pour id_to_*
    id_to_user = {int(k): v for k, v in data['id_to_user'].items()}
    id_to_track = {int(k): v for k, v in data['id_to_track'].items()}

    return id_to_user, id_to_track


def build_sparse_matrix(
    input_file: Path = INPUT_FILE,
    output_matrix: Path = OUTPUT_MATRIX,
    confidence_scaling: float = 40.0,
    use_log_transform: bool = True
) -> Tuple[sparse.csr_matrix, dict, dict]:
    """
    Construit la matrice sparse user-item au format CSR.

    La matrice utilise des "confidence weights" pour les implicit feedbacks:
    - confidence = 1 + alpha * log(1 + play_count)

    Args:
        input_file: Fichier parquet des écoutes agrégées
        output_matrix: Chemin de sortie pour la matrice .npz
        confidence_scaling: Facteur alpha pour le scaling des confidences
        use_log_transform: Si True, utilise log(1 + count) pour la transformation

    Returns:
        (matrice CSR, user_mapping, item_mapping)
    """
    print("=" * 60)
    print("Construction de la matrice sparse user-item")
    print("=" * 60)

    # Charger les données
    print(f"\nChargement de {input_file}...")
    df = pd.read_parquet(input_file)
    print(f"Interactions chargées: {len(df):,}")

    # Dimensions de la matrice
    n_users = df['user_id'].max() + 1
    n_items = df['track_id'].max() + 1
    print(f"Dimensions: {n_users:,} users × {n_items:,} items")

    # Calculer les valeurs de confidence
    print("\nCalcul des confidence weights...")
    if use_log_transform:
        # Transformation log pour les implicit feedbacks
        # Référence: "Collaborative Filtering for Implicit Feedback Datasets" (Hu et al., 2008)
        values = 1 + confidence_scaling * np.log1p(df['play_count'].values)
    else:
        values = df['play_count'].values.astype(np.float32)

    # Construire la matrice sparse
    print("Construction de la matrice CSR...")
    user_item_matrix = sparse.csr_matrix(
        (values, (df['user_id'].values, df['track_id'].values)),
        shape=(n_users, n_items),
        dtype=np.float32
    )

    # Statistiques
    nnz = user_item_matrix.nnz
    sparsity = 1 - (nnz / (n_users * n_items))
    memory_mb = (user_item_matrix.data.nbytes +
                 user_item_matrix.indices.nbytes +
                 user_item_matrix.indptr.nbytes) / (1024 * 1024)

    print(f"\n{'=' * 60}")
    print("STATISTIQUES DE LA MATRICE")
    print(f"{'=' * 60}")
    print(f"Forme: {user_item_matrix.shape}")
    print(f"Éléments non-nuls: {nnz:,}")
    print(f"Sparsité: {sparsity*100:.4f}%")
    print(f"Mémoire (sparse): {memory_mb:.1f} MB")
    print(f"Mémoire (dense, théorique): {n_users * n_items * 4 / (1024**3):.1f} GB")
    print(f"Ratio compression: {(n_users * n_items * 4) / (memory_mb * 1024 * 1024):.0f}x")

    # Distribution des valeurs
    print(f"\nDistribution des confidence weights:")
    print(f"  Min: {user_item_matrix.data.min():.2f}")
    print(f"  Max: {user_item_matrix.data.max():.2f}")
    print(f"  Moyenne: {user_item_matrix.data.mean():.2f}")
    print(f"  Médiane: {np.median(user_item_matrix.data):.2f}")

    # Sauvegarder la matrice
    print(f"\nSauvegarde vers {output_matrix}...")
    sparse.save_npz(output_matrix, user_item_matrix)

    # Charger et sauvegarder les mappings séparément
    mappings_file = PROCESSED_DIR / "mappings.json"
    if mappings_file.exists():
        id_to_user, id_to_track = load_mappings(mappings_file)

        # Sauvegarder les mappings individuels pour l'API
        with open(OUTPUT_USER_MAPPING, 'w', encoding='utf-8') as f:
            json.dump(id_to_user, f, ensure_ascii=False)

        with open(OUTPUT_ITEM_MAPPING, 'w', encoding='utf-8') as f:
            json.dump(id_to_track, f, ensure_ascii=False)

        print(f"Mappings sauvegardés:")
        print(f"  - {OUTPUT_USER_MAPPING}")
        print(f"  - {OUTPUT_ITEM_MAPPING}")

    file_size_mb = output_matrix.stat().st_size / (1024 * 1024)
    print(f"\nTaille du fichier matrice: {file_size_mb:.1f} MB")

    return user_item_matrix


def create_train_test_split(
    matrix: sparse.csr_matrix,
    test_ratio: float = 0.2,
    random_state: int = 42
) -> Tuple[sparse.csr_matrix, sparse.csr_matrix]:
    """
    Crée un split train/test pour l'évaluation.
    Pour chaque utilisateur, met de côté un % de ses interactions pour le test.

    Args:
        matrix: Matrice user-item complète
        test_ratio: Ratio d'interactions pour le test
        random_state: Seed pour la reproductibilité

    Returns:
        (train_matrix, test_matrix)
    """
    print(f"\nCréation du split train/test ({(1-test_ratio)*100:.0f}%/{test_ratio*100:.0f}%)...")

    np.random.seed(random_state)

    train = matrix.copy().tolil()
    test = sparse.lil_matrix(matrix.shape, dtype=matrix.dtype)

    for user_id in tqdm(range(matrix.shape[0]), desc="Split"):
        # Indices des items consommés par cet utilisateur
        items = matrix[user_id].indices

        if len(items) > 1:
            # Nombre d'items pour le test
            n_test = max(1, int(len(items) * test_ratio))

            # Sélectionner aléatoirement les items de test
            test_items = np.random.choice(items, size=n_test, replace=False)

            # Déplacer vers le test set
            for item in test_items:
                test[user_id, item] = train[user_id, item]
                train[user_id, item] = 0

    train = train.tocsr()
    test = test.tocsr()

    # Éliminer les zéros explicites
    train.eliminate_zeros()
    test.eliminate_zeros()

    print(f"Train: {train.nnz:,} interactions")
    print(f"Test: {test.nnz:,} interactions")

    return train, test


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Construire la matrice user-item")
    parser.add_argument("--input", type=Path, default=INPUT_FILE,
                       help="Fichier parquet d'entrée")
    parser.add_argument("--output", type=Path, default=OUTPUT_MATRIX,
                       help="Fichier .npz de sortie")
    parser.add_argument("--alpha", type=float, default=40.0,
                       help="Facteur de scaling des confidences")
    parser.add_argument("--no-log", action="store_true",
                       help="Désactiver la transformation log")
    parser.add_argument("--split", action="store_true",
                       help="Créer aussi un split train/test")
    parser.add_argument("--test-ratio", type=float, default=0.2,
                       help="Ratio pour le test set")

    args = parser.parse_args()

    matrix = build_sparse_matrix(
        input_file=args.input,
        output_matrix=args.output,
        confidence_scaling=args.alpha,
        use_log_transform=not args.no_log
    )

    if args.split:
        train, test = create_train_test_split(matrix, test_ratio=args.test_ratio)

        train_file = args.output.parent / "train_matrix.npz"
        test_file = args.output.parent / "test_matrix.npz"

        sparse.save_npz(train_file, train)
        sparse.save_npz(test_file, test)

        print(f"\nMatrices train/test sauvegardées:")
        print(f"  - {train_file}")
        print(f"  - {test_file}")


if __name__ == "__main__":
    main()
