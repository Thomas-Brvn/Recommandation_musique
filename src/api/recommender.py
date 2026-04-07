"""
Service de recommandation pour l'API.
Entièrement async : boto3 et calculs ALS exécutés dans un thread via asyncio.to_thread.
"""
import asyncio
import io
import json
import os
from pathlib import Path
from typing import List, Optional

import boto3
from scipy import sparse

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from models.als_model import ALSRecommender


class RecommendationService:
    """Service singleton pour gérer les recommandations."""

    _instance: Optional["RecommendationService"] = None

    def __init__(self):
        self.model: Optional[ALSRecommender] = None
        self.user_item_matrix: Optional[sparse.csr_matrix] = None
        self.user_name_to_id: dict = {}
        self.is_loaded: bool = False

    @classmethod
    def get_instance(cls) -> "RecommendationService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def load_from_s3(
        self,
        bucket: str,
        model_key: str = "models/als_model.pkl",
        matrix_key: str = "processed/user_item_matrix.npz",
        mappings_key: str = "processed/mappings.json",
        region: str = "eu-north-1",
    ):
        """Charge le modèle et les données directement depuis S3 (non-bloquant)."""
        print("Chargement depuis S3...")
        matrix_bytes, model_bytes, mappings_bytes = await asyncio.gather(
            asyncio.to_thread(self._s3_read, bucket, matrix_key, region),
            asyncio.to_thread(self._s3_read, bucket, model_key, region),
            asyncio.to_thread(self._s3_read, bucket, mappings_key, region),
        )
        print(f"  - Matrice: s3://{bucket}/{matrix_key}")
        print(f"  - Modèle: s3://{bucket}/{model_key}")
        print(f"  - Mappings: s3://{bucket}/{mappings_key}")

        # Désérialisation dans un thread (CPU-bound)
        self.user_item_matrix, self.model, self.user_name_to_id = await asyncio.gather(
            asyncio.to_thread(lambda: sparse.load_npz(io.BytesIO(matrix_bytes))),
            asyncio.to_thread(lambda: ALSRecommender.load_from_bytes(model_bytes, None)),
            asyncio.to_thread(lambda: json.loads(mappings_bytes).get("user_to_id", {})),
        )
        # Attacher la matrice au modèle
        self.model.user_item_matrix = self.user_item_matrix
        self.model.item_user_matrix = self.user_item_matrix.T.tocsr()

        self.is_loaded = True
        print(f"Service chargé: {self.user_item_matrix.shape[0]:,} users, {self.user_item_matrix.shape[1]:,} items")

    async def load(
        self,
        model_path: Path,
        matrix_path: Path,
        mappings_path: Optional[Path] = None,
    ):
        """Charge le modèle et les données depuis le disque (non-bloquant)."""
        print("Chargement du service de recommandation...")
        print(f"  - Matrice: {matrix_path}")
        print(f"  - Modèle: {model_path}")

        self.user_item_matrix = await asyncio.to_thread(sparse.load_npz, str(matrix_path))
        self.model = await asyncio.to_thread(ALSRecommender.load, model_path, self.user_item_matrix)

        if mappings_path and mappings_path.exists():
            print(f"  - Mappings: {mappings_path}")
            def _read_mappings():
                with open(mappings_path, "r", encoding="utf-8") as f:
                    return json.load(f).get("user_to_id", {})
            self.user_name_to_id = await asyncio.to_thread(_read_mappings)

        self.is_loaded = True
        print(f"Service chargé: {self.user_item_matrix.shape[0]:,} users, {self.user_item_matrix.shape[1]:,} items")

    @staticmethod
    def _s3_read(bucket: str, key: str, region: str) -> bytes:
        s3 = boto3.client("s3", region_name=region)
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def _ensure_loaded(self):
        if not self.is_loaded:
            raise RuntimeError("Le service n'est pas chargé.")

    def get_user_id(self, user_identifier: str | int) -> int:
        self._ensure_loaded()
        if isinstance(user_identifier, int):
            if 0 <= user_identifier < self.user_item_matrix.shape[0]:
                return user_identifier
            raise ValueError(f"User ID {user_identifier} hors limites")
        try:
            uid = int(user_identifier)
            if 0 <= uid < self.user_item_matrix.shape[0]:
                return uid
        except ValueError:
            pass
        if user_identifier in self.user_name_to_id:
            return self.user_name_to_id[user_identifier]
        raise ValueError(f"Utilisateur '{user_identifier}' non trouvé")

    async def recommend(self, user_identifier: str | int, n: int = 10, filter_already_liked: bool = True) -> List[dict]:
        self._ensure_loaded()
        user_id = self.get_user_id(user_identifier)
        recommendations = await asyncio.to_thread(self.model.recommend, user_id, n, filter_already_liked)
        return self._format_tracks(recommendations)

    async def similar_tracks(self, item_id: int, n: int = 10) -> List[dict]:
        self._ensure_loaded()
        similar = await asyncio.to_thread(self.model.similar_items, item_id, n)
        return self._format_tracks(similar)

    async def get_user_history(self, user_identifier: str | int, n: int = 20) -> List[dict]:
        self._ensure_loaded()
        user_id = self.get_user_id(user_identifier)

        def _extract():
            row = self.user_item_matrix[user_id]
            items = row.indices
            values = row.data
            sorted_idx = values.argsort()[::-1][:n]
            return [(items[i], values[i]) for i in sorted_idx]

        pairs = await asyncio.to_thread(_extract)
        results = []
        for item_id, value in pairs:
            track_info = self.model.get_track_name(item_id)
            if " - " in track_info:
                artist, track = track_info.split(" - ", 1)
            else:
                artist, track = "Unknown", track_info
            results.append({
                "track": track,
                "artist": artist,
                "confidence_score": round(float(value), 2),
                "item_id": item_id,
            })
        return results

    def _format_tracks(self, pairs: List[tuple]) -> List[dict]:
        results = []
        for item_id, score in pairs:
            track_info = self.model.get_track_name(item_id)
            if " - " in track_info:
                artist, track = track_info.split(" - ", 1)
            else:
                artist, track = "Unknown", track_info
            results.append({
                "track": track,
                "artist": artist,
                "score": round(float(score), 4),
                "item_id": item_id,
            })
        return results

    async def get_stats(self) -> dict:
        self._ensure_loaded()
        return {
            "n_users": self.user_item_matrix.shape[0],
            "n_items": self.user_item_matrix.shape[1],
            "n_interactions": self.user_item_matrix.nnz,
            "sparsity": 1 - (self.user_item_matrix.nnz / (self.user_item_matrix.shape[0] * self.user_item_matrix.shape[1])),
            "model_factors": self.model.factors,
            "model_regularization": self.model.regularization,
        }
