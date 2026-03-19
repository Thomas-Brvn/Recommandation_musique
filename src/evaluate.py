#!/usr/bin/env python3
"""
Script d'évaluation du modèle de recommandation.
Calcule les métriques: Precision@K, Recall@K, NDCG@K, Coverage, Novelty.
"""
import argparse
import json
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
from scipy import sparse
from tqdm import tqdm

from models.als_model import ALSRecommender

# Configuration
DATA_DIR = Path(__file__).parent.parent / "data" / "processed"
MODELS_DIR = Path(__file__).parent.parent / "models"


def precision_at_k(recommended: List[int], relevant: set, k: int) -> float:
    """
    Calcule Precision@K: proportion de recommandations pertinentes.

    Args:
        recommended: Liste ordonnée des items recommandés
        relevant: Set des items pertinents (ground truth)
        k: Nombre de recommandations à considérer
    """
    recommended_k = recommended[:k]
    n_relevant = len(set(recommended_k) & relevant)
    return n_relevant / k if k > 0 else 0.0


def recall_at_k(recommended: List[int], relevant: set, k: int) -> float:
    """
    Calcule Recall@K: proportion d'items pertinents recommandés.
    """
    if not relevant:
        return 0.0
    recommended_k = recommended[:k]
    n_relevant = len(set(recommended_k) & relevant)
    return n_relevant / len(relevant)


def ndcg_at_k(recommended: List[int], relevant: set, k: int) -> float:
    """
    Calcule NDCG@K (Normalized Discounted Cumulative Gain).
    Mesure la qualité du ranking.
    """
    recommended_k = recommended[:k]

    # DCG
    dcg = 0.0
    for i, item in enumerate(recommended_k):
        if item in relevant:
            dcg += 1.0 / np.log2(i + 2)  # position starts at 1

    # Ideal DCG (tous les items pertinents en haut)
    ideal_k = min(len(relevant), k)
    idcg = sum(1.0 / np.log2(i + 2) for i in range(ideal_k))

    return dcg / idcg if idcg > 0 else 0.0


def average_precision(recommended: List[int], relevant: set) -> float:
    """
    Calcule Average Precision pour un utilisateur.
    """
    if not relevant:
        return 0.0

    score = 0.0
    n_relevant = 0

    for i, item in enumerate(recommended):
        if item in relevant:
            n_relevant += 1
            score += n_relevant / (i + 1)

    return score / len(relevant) if relevant else 0.0


def evaluate_model(
    model: ALSRecommender,
    train_matrix: sparse.csr_matrix,
    test_matrix: sparse.csr_matrix,
    k_values: List[int] = [5, 10, 20],
    n_users_sample: int = None
) -> Dict:
    """
    Évalue le modèle sur le test set.

    Args:
        model: Modèle entraîné
        train_matrix: Matrice d'entraînement
        test_matrix: Matrice de test (ground truth)
        k_values: Valeurs de K pour les métriques
        n_users_sample: Nombre d'utilisateurs à évaluer (None = tous)

    Returns:
        Dict avec toutes les métriques
    """
    print("=" * 60)
    print("ÉVALUATION DU MODÈLE")
    print("=" * 60)

    # Trouver les utilisateurs avec des items de test
    test_users = np.where(test_matrix.getnnz(axis=1) > 0)[0]
    print(f"\nUtilisateurs avec données de test: {len(test_users):,}")

    if n_users_sample and n_users_sample < len(test_users):
        np.random.seed(42)
        test_users = np.random.choice(test_users, size=n_users_sample, replace=False)
        print(f"Échantillon évalué: {len(test_users):,}")

    # Initialiser les accumulateurs
    metrics = {f"precision@{k}": [] for k in k_values}
    metrics.update({f"recall@{k}": [] for k in k_values})
    metrics.update({f"ndcg@{k}": [] for k in k_values})
    metrics["map"] = []

    # Items recommandés (pour coverage)
    all_recommended_items = set()
    max_k = max(k_values)

    # Évaluer chaque utilisateur
    for user_id in tqdm(test_users, desc="Évaluation"):
        # Ground truth: items dans le test set
        relevant_items = set(test_matrix[user_id].indices)

        if not relevant_items:
            continue

        # Générer les recommandations
        try:
            recommendations = model.recommend(user_id, n=max_k, filter_already_liked=True)
            recommended_items = [item_id for item_id, _ in recommendations]
        except Exception:
            continue

        all_recommended_items.update(recommended_items)

        # Calculer les métriques pour chaque K
        for k in k_values:
            metrics[f"precision@{k}"].append(precision_at_k(recommended_items, relevant_items, k))
            metrics[f"recall@{k}"].append(recall_at_k(recommended_items, relevant_items, k))
            metrics[f"ndcg@{k}"].append(ndcg_at_k(recommended_items, relevant_items, k))

        metrics["map"].append(average_precision(recommended_items, relevant_items))

    # Calculer les moyennes
    results = {}
    for metric_name, values in metrics.items():
        if values:
            results[metric_name] = {
                "mean": float(np.mean(values)),
                "std": float(np.std(values)),
                "n_users": len(values)
            }

    # Coverage: proportion d'items recommandés au moins une fois
    n_total_items = train_matrix.shape[1]
    coverage = len(all_recommended_items) / n_total_items
    results["coverage"] = {
        "value": coverage,
        "n_items_recommended": len(all_recommended_items),
        "n_items_total": n_total_items
    }

    # Novelty: inverse de la popularité moyenne des items recommandés
    item_popularity = np.array(train_matrix.sum(axis=0)).flatten()
    item_popularity = item_popularity / item_popularity.sum()  # Normaliser

    recommended_popularity = [item_popularity[i] for i in all_recommended_items if i < len(item_popularity)]
    if recommended_popularity:
        avg_popularity = np.mean(recommended_popularity)
        novelty = -np.log2(avg_popularity) if avg_popularity > 0 else 0
        results["novelty"] = {
            "value": float(novelty),
            "avg_popularity": float(avg_popularity)
        }

    return results


def print_results(results: Dict):
    """Affiche les résultats de manière formatée."""
    print("\n" + "=" * 60)
    print("RÉSULTATS")
    print("=" * 60)

    # Métriques de ranking
    print("\n📊 Métriques de Ranking:")
    print("-" * 40)
    for metric in ['precision', 'recall', 'ndcg']:
        for k in [5, 10, 20]:
            key = f"{metric}@{k}"
            if key in results:
                mean = results[key]['mean']
                std = results[key]['std']
                print(f"  {key:15} {mean:.4f} (±{std:.4f})")

    # MAP
    if 'map' in results:
        print(f"\n  {'MAP':15} {results['map']['mean']:.4f} (±{results['map']['std']:.4f})")

    # Coverage
    if 'coverage' in results:
        cov = results['coverage']
        print(f"\n📈 Coverage:")
        print(f"  {cov['value']*100:.2f}% ({cov['n_items_recommended']:,}/{cov['n_items_total']:,} items)")

    # Novelty
    if 'novelty' in results:
        nov = results['novelty']
        print(f"\n🆕 Novelty:")
        print(f"  {nov['value']:.2f} (popularité moyenne: {nov['avg_popularity']:.6f})")


def main():
    parser = argparse.ArgumentParser(description="Évaluer le modèle de recommandation")
    parser.add_argument("--model", type=Path, default=MODELS_DIR / "als_model.pkl",
                       help="Chemin vers le modèle")
    parser.add_argument("--train", type=Path, default=DATA_DIR / "train_matrix.npz",
                       help="Matrice d'entraînement")
    parser.add_argument("--test", type=Path, default=DATA_DIR / "test_matrix.npz",
                       help="Matrice de test")
    parser.add_argument("--full-matrix", type=Path, default=DATA_DIR / "user_item_matrix.npz",
                       help="Matrice complète (pour charger le modèle)")
    parser.add_argument("--k", type=int, nargs='+', default=[5, 10, 20],
                       help="Valeurs de K")
    parser.add_argument("--sample", type=int,
                       help="Nombre d'utilisateurs à évaluer")
    parser.add_argument("--output", type=Path,
                       help="Fichier JSON pour sauvegarder les résultats")

    args = parser.parse_args()

    # Charger le modèle
    print(f"Chargement du modèle: {args.model}")
    full_matrix = sparse.load_npz(args.full_matrix)
    model = ALSRecommender.load(args.model, user_item_matrix=full_matrix)

    # Charger les matrices train/test
    print(f"Chargement des matrices train/test...")
    train_matrix = sparse.load_npz(args.train)
    test_matrix = sparse.load_npz(args.test)

    print(f"Train: {train_matrix.nnz:,} interactions")
    print(f"Test: {test_matrix.nnz:,} interactions")

    # Évaluer
    results = evaluate_model(
        model=model,
        train_matrix=train_matrix,
        test_matrix=test_matrix,
        k_values=args.k,
        n_users_sample=args.sample
    )

    # Afficher
    print_results(results)

    # Sauvegarder si demandé
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"\nRésultats sauvegardés: {args.output}")


if __name__ == "__main__":
    main()
