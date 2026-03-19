"""
Service de recommandation pour l'API.
Gère le chargement du modèle et les requêtes de recommandation.
"""
import json
from pathlib import Path
from typing import List, Optional

from scipy import sparse

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from models.als_model import ALSRecommender


class RecommendationService:
    """
    Service singleton pour gérer les recommandations.
    Charge le modèle et la matrice une seule fois au démarrage.
    """

    _instance: Optional['RecommendationService'] = None

    def __init__(self):
        self.model: Optional[ALSRecommender] = None
        self.user_item_matrix: Optional[sparse.csr_matrix] = None
        self.user_name_to_id: dict = {}
        self.is_loaded: bool = False

    @classmethod
    def get_instance(cls) -> 'RecommendationService':
        """Retourne l'instance singleton."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def load(
        self,
        model_path: Path,
        matrix_path: Path,
        mappings_path: Optional[Path] = None
    ):
        """
        Charge le modèle et les données nécessaires.

        Args:
            model_path: Chemin vers le modèle .pkl
            matrix_path: Chemin vers la matrice .npz
            mappings_path: Chemin vers le fichier mappings.json
        """
        print(f"Chargement du service de recommandation...")

        # Charger la matrice
        print(f"  - Matrice: {matrix_path}")
        self.user_item_matrix = sparse.load_npz(matrix_path)

        # Charger le modèle
        print(f"  - Modèle: {model_path}")
        self.model = ALSRecommender.load(model_path, self.user_item_matrix)

        # Charger le mapping inverse (nom -> id)
        if mappings_path and mappings_path.exists():
            print(f"  - Mappings: {mappings_path}")
            with open(mappings_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.user_name_to_id = data.get('user_to_id', {})

        self.is_loaded = True
        print(f"Service chargé: {self.user_item_matrix.shape[0]:,} users, {self.user_item_matrix.shape[1]:,} items")

    def _ensure_loaded(self):
        """Vérifie que le service est chargé."""
        if not self.is_loaded:
            raise RuntimeError("Le service n'est pas chargé. Appelez load() d'abord.")

    def get_user_id(self, user_identifier: str | int) -> int:
        """
        Résout un identifiant utilisateur (nom ou ID) en ID numérique.

        Args:
            user_identifier: Nom d'utilisateur ou ID numérique

        Returns:
            ID numérique de l'utilisateur

        Raises:
            ValueError: Si l'utilisateur n'existe pas
        """
        self._ensure_loaded()

        # Si c'est déjà un entier
        if isinstance(user_identifier, int):
            if 0 <= user_identifier < self.user_item_matrix.shape[0]:
                return user_identifier
            raise ValueError(f"User ID {user_identifier} hors limites")

        # Essayer de convertir en int
        try:
            user_id = int(user_identifier)
            if 0 <= user_id < self.user_item_matrix.shape[0]:
                return user_id
        except ValueError:
            pass

        # Chercher par nom
        if user_identifier in self.user_name_to_id:
            return self.user_name_to_id[user_identifier]

        raise ValueError(f"Utilisateur '{user_identifier}' non trouvé")

    def recommend(
        self,
        user_identifier: str | int,
        n: int = 10,
        filter_already_liked: bool = True
    ) -> List[dict]:
        """
        Génère des recommandations pour un utilisateur.

        Args:
            user_identifier: Nom ou ID de l'utilisateur
            n: Nombre de recommandations
            filter_already_liked: Exclure les items déjà écoutés

        Returns:
            Liste de {"track": str, "artist": str, "score": float, "item_id": int}
        """
        self._ensure_loaded()

        user_id = self.get_user_id(user_identifier)
        recommendations = self.model.recommend(user_id, n, filter_already_liked)

        results = []
        for item_id, score in recommendations:
            track_info = self.model.get_track_name(item_id)

            # Parser "Artist - Track" si possible
            if ' - ' in track_info:
                artist, track = track_info.split(' - ', 1)
            else:
                artist = "Unknown"
                track = track_info

            results.append({
                "track": track,
                "artist": artist,
                "score": round(float(score), 4),
                "item_id": item_id
            })

        return results

    def similar_tracks(self, item_id: int, n: int = 10) -> List[dict]:
        """
        Trouve les tracks similaires à un track donné.

        Args:
            item_id: ID du track
            n: Nombre de tracks similaires

        Returns:
            Liste de {"track": str, "artist": str, "score": float, "item_id": int}
        """
        self._ensure_loaded()

        similar = self.model.similar_items(item_id, n)

        results = []
        for sim_item_id, score in similar:
            track_info = self.model.get_track_name(sim_item_id)

            if ' - ' in track_info:
                artist, track = track_info.split(' - ', 1)
            else:
                artist = "Unknown"
                track = track_info

            results.append({
                "track": track,
                "artist": artist,
                "score": round(float(score), 4),
                "item_id": sim_item_id
            })

        return results

    def get_user_history(self, user_identifier: str | int, n: int = 20) -> List[dict]:
        """
        Retourne l'historique d'écoute d'un utilisateur.

        Args:
            user_identifier: Nom ou ID de l'utilisateur
            n: Nombre maximum d'items

        Returns:
            Liste de {"track": str, "artist": str, "play_count": float}
        """
        self._ensure_loaded()

        user_id = self.get_user_id(user_identifier)
        user_row = self.user_item_matrix[user_id]

        # Obtenir les items et leurs scores (play counts transformés)
        items = user_row.indices
        values = user_row.data

        # Trier par valeur décroissante
        sorted_indices = values.argsort()[::-1][:n]

        results = []
        for idx in sorted_indices:
            item_id = items[idx]
            value = values[idx]

            track_info = self.model.get_track_name(item_id)

            if ' - ' in track_info:
                artist, track = track_info.split(' - ', 1)
            else:
                artist = "Unknown"
                track = track_info

            results.append({
                "track": track,
                "artist": artist,
                "confidence_score": round(float(value), 2),
                "item_id": item_id
            })

        return results

    def get_stats(self) -> dict:
        """Retourne des statistiques sur le service."""
        self._ensure_loaded()

        return {
            "n_users": self.user_item_matrix.shape[0],
            "n_items": self.user_item_matrix.shape[1],
            "n_interactions": self.user_item_matrix.nnz,
            "sparsity": 1 - (self.user_item_matrix.nnz /
                           (self.user_item_matrix.shape[0] * self.user_item_matrix.shape[1])),
            "model_factors": self.model.factors,
            "model_regularization": self.model.regularization
        }
