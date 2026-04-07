"""
Modèle ALS (Alternating Least Squares) pour les recommandations musicales.
Utilise la bibliothèque `implicit` pour la factorisation matricielle.
"""
import json
import pickle
from pathlib import Path
from typing import List, Tuple, Optional

import numpy as np
from scipy import sparse
from implicit.als import AlternatingLeastSquares
from implicit.evaluation import precision_at_k, mean_average_precision_at_k


class ALSRecommender:
    """
    Système de recommandation basé sur la factorisation matricielle ALS.

    Attributs:
        model: Modèle ALS de la bibliothèque implicit
        user_item_matrix: Matrice sparse des interactions user-item
        user_mapping: Dict mapping user_id -> user_name
        item_mapping: Dict mapping item_id -> track_name
    """

    def __init__(
        self,
        factors: int = 128,
        regularization: float = 0.01,
        iterations: int = 15,
        use_gpu: bool = False,
        random_state: int = 42
    ):
        """
        Initialise le modèle ALS.

        Args:
            factors: Nombre de dimensions latentes
            regularization: Terme de régularisation L2
            iterations: Nombre d'itérations d'entraînement
            use_gpu: Utiliser CUDA si disponible
            random_state: Seed pour la reproductibilité
        """
        self.factors = factors
        self.regularization = regularization
        self.iterations = iterations
        self.use_gpu = use_gpu
        self.random_state = random_state

        self.model = AlternatingLeastSquares(
            factors=factors,
            regularization=regularization,
            iterations=iterations,
            use_gpu=use_gpu,
            random_state=random_state
        )

        self.user_item_matrix: Optional[sparse.csr_matrix] = None
        self.item_user_matrix: Optional[sparse.csr_matrix] = None
        self.user_mapping: dict = {}
        self.item_mapping: dict = {}
        self.is_fitted: bool = False

    def fit(self, user_item_matrix: sparse.csr_matrix, show_progress: bool = True) -> 'ALSRecommender':
        """
        Entraîne le modèle sur la matrice user-item.

        Args:
            user_item_matrix: Matrice sparse (n_users, n_items) avec les confidence weights
            show_progress: Afficher la barre de progression

        Returns:
            self
        """
        print(f"Entraînement ALS (factors={self.factors}, reg={self.regularization}, iter={self.iterations})")
        print(f"Matrice: {user_item_matrix.shape[0]:,} users × {user_item_matrix.shape[1]:,} items")

        self.user_item_matrix = user_item_matrix
        # implicit attend une matrice item-user pour fit()
        self.item_user_matrix = user_item_matrix.T.tocsr()

        self.model.fit(self.item_user_matrix, show_progress=show_progress)
        self.is_fitted = True

        print("Entraînement terminé!")
        return self

    def recommend(
        self,
        user_id: int,
        n: int = 10,
        filter_already_liked: bool = True
    ) -> List[Tuple[int, float]]:
        """
        Génère des recommandations pour un utilisateur.

        Args:
            user_id: ID de l'utilisateur
            n: Nombre de recommandations
            filter_already_liked: Exclure les items déjà consommés

        Returns:
            Liste de (item_id, score)
        """
        if not self.is_fitted:
            raise ValueError("Le modèle n'est pas entraîné. Appelez fit() d'abord.")

        if user_id < 0 or user_id >= self.user_item_matrix.shape[0]:
            raise ValueError(f"user_id {user_id} hors limites [0, {self.user_item_matrix.shape[0]})")

        # Récupérer les interactions de l'utilisateur
        user_items = self.user_item_matrix[user_id]

        # Générer les recommandations
        item_ids, scores = self.model.recommend(
            userid=user_id,
            user_items=user_items,
            N=n,
            filter_already_liked_items=filter_already_liked
        )

        return list(zip(item_ids.tolist(), scores.tolist()))

    def recommend_batch(
        self,
        user_ids: List[int],
        n: int = 10,
        filter_already_liked: bool = True
    ) -> dict:
        """
        Génère des recommandations pour plusieurs utilisateurs.

        Args:
            user_ids: Liste des IDs utilisateurs
            n: Nombre de recommandations par utilisateur
            filter_already_liked: Exclure les items déjà consommés

        Returns:
            Dict {user_id: [(item_id, score), ...]}
        """
        if not self.is_fitted:
            raise ValueError("Le modèle n'est pas entraîné. Appelez fit() d'abord.")

        results = {}
        user_items = self.user_item_matrix[user_ids]

        item_ids, scores = self.model.recommend(
            userid=np.array(user_ids),
            user_items=user_items,
            N=n,
            filter_already_liked_items=filter_already_liked
        )

        for i, user_id in enumerate(user_ids):
            results[user_id] = list(zip(item_ids[i].tolist(), scores[i].tolist()))

        return results

    def similar_items(self, item_id: int, n: int = 10) -> List[Tuple[int, float]]:
        """
        Trouve les items similaires à un item donné.

        Args:
            item_id: ID de l'item
            n: Nombre d'items similaires

        Returns:
            Liste de (item_id, score)
        """
        if not self.is_fitted:
            raise ValueError("Le modèle n'est pas entraîné. Appelez fit() d'abord.")

        item_ids, scores = self.model.similar_items(item_id, N=n + 1)

        # Exclure l'item lui-même (premier résultat)
        results = list(zip(item_ids[1:].tolist(), scores[1:].tolist()))
        return results[:n]

    def similar_users(self, user_id: int, n: int = 10) -> List[Tuple[int, float]]:
        """
        Trouve les utilisateurs similaires à un utilisateur donné.

        Args:
            user_id: ID de l'utilisateur
            n: Nombre d'utilisateurs similaires

        Returns:
            Liste de (user_id, score)
        """
        if not self.is_fitted:
            raise ValueError("Le modèle n'est pas entraîné. Appelez fit() d'abord.")

        user_ids, scores = self.model.similar_users(user_id, N=n + 1)

        # Exclure l'utilisateur lui-même
        results = list(zip(user_ids[1:].tolist(), scores[1:].tolist()))
        return results[:n]

    def get_user_factors(self, user_id: int) -> np.ndarray:
        """Retourne le vecteur latent d'un utilisateur."""
        return self.model.user_factors[user_id]

    def get_item_factors(self, item_id: int) -> np.ndarray:
        """Retourne le vecteur latent d'un item."""
        return self.model.item_factors[item_id]

    def load_mappings(self, user_mapping_path: Path, item_mapping_path: Path):
        """Charge les mappings ID -> nom."""
        with open(user_mapping_path, 'r', encoding='utf-8') as f:
            self.user_mapping = {int(k): v for k, v in json.load(f).items()}

        with open(item_mapping_path, 'r', encoding='utf-8') as f:
            self.item_mapping = {int(k): v for k, v in json.load(f).items()}

    def get_track_name(self, item_id: int) -> str:
        """Retourne le nom d'un track à partir de son ID."""
        return self.item_mapping.get(item_id, f"Unknown (ID: {item_id})")

    def get_user_name(self, user_id: int) -> str:
        """Retourne le nom d'un utilisateur à partir de son ID."""
        return self.user_mapping.get(user_id, f"Unknown (ID: {user_id})")

    def recommend_with_names(
        self,
        user_id: int,
        n: int = 10,
        filter_already_liked: bool = True
    ) -> List[dict]:
        """
        Génère des recommandations avec les noms des tracks.

        Returns:
            Liste de {"track": str, "score": float, "item_id": int}
        """
        recommendations = self.recommend(user_id, n, filter_already_liked)

        return [
            {
                "track": self.get_track_name(item_id),
                "score": round(score, 4),
                "item_id": item_id
            }
            for item_id, score in recommendations
        ]

    def save(self, path: Path):
        """Sauvegarde le modèle complet."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        state = {
            'model': self.model,
            'factors': self.factors,
            'regularization': self.regularization,
            'iterations': self.iterations,
            'user_mapping': self.user_mapping,
            'item_mapping': self.item_mapping,
            'is_fitted': self.is_fitted
        }

        with open(path, 'wb') as f:
            pickle.dump(state, f)

        print(f"Modèle sauvegardé: {path}")

    @classmethod
    def _from_state(cls, state: dict, user_item_matrix: Optional[sparse.csr_matrix] = None) -> 'ALSRecommender':
        recommender = cls(
            factors=state['factors'],
            regularization=state['regularization'],
            iterations=state['iterations']
        )

        recommender.model = state['model']
        recommender.user_mapping = state['user_mapping']
        recommender.item_mapping = state['item_mapping']
        recommender.is_fitted = state['is_fitted']

        if user_item_matrix is not None:
            recommender.user_item_matrix = user_item_matrix
            recommender.item_user_matrix = user_item_matrix.T.tocsr()

        return recommender

    @classmethod
    def load_from_bytes(cls, data: bytes, user_item_matrix: Optional[sparse.csr_matrix] = None) -> 'ALSRecommender':
        """Charge un modèle depuis des bytes (ex: stream S3)."""
        import io
        state = pickle.load(io.BytesIO(data))
        return cls._from_state(state, user_item_matrix)

    @classmethod
    def load(cls, path: Path, user_item_matrix: Optional[sparse.csr_matrix] = None) -> 'ALSRecommender':
        """
        Charge un modèle sauvegardé.

        Args:
            path: Chemin vers le fichier .pkl
            user_item_matrix: Matrice user-item (nécessaire pour les recommandations)

        Returns:
            Instance ALSRecommender
        """
        with open(path, 'rb') as f:
            state = pickle.load(f)

        recommender = cls._from_state(state, user_item_matrix)
        print(f"Modèle chargé: {path}")
        return recommender

    def __repr__(self) -> str:
        status = "fitted" if self.is_fitted else "not fitted"
        return f"ALSRecommender(factors={self.factors}, reg={self.regularization}, {status})"
