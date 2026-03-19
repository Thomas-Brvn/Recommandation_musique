"""
API FastAPI pour le système de recommandation musicale.
"""
import os
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .recommender import RecommendationService

# Configuration
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR = BASE_DIR / "models"

# Chemins par défaut (peuvent être overridés par variables d'environnement)
MODEL_PATH = Path(os.getenv("MODEL_PATH", MODELS_DIR / "als_model.pkl"))
MATRIX_PATH = Path(os.getenv("MATRIX_PATH", DATA_DIR / "user_item_matrix.npz"))
MAPPINGS_PATH = Path(os.getenv("MAPPINGS_PATH", DATA_DIR / "mappings.json"))

# Créer l'application
app = FastAPI(
    title="Music Recommendation API",
    description="API de recommandation musicale basée sur le Collaborative Filtering (ALS)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Modèles Pydantic
class TrackRecommendation(BaseModel):
    """Une recommandation de track."""
    track: str = Field(..., description="Nom du morceau")
    artist: str = Field(..., description="Nom de l'artiste")
    score: float = Field(..., description="Score de recommandation")
    item_id: int = Field(..., description="ID interne du track")


class RecommendationResponse(BaseModel):
    """Réponse de recommandation."""
    user_id: str = Field(..., description="Identifiant de l'utilisateur")
    recommendations: List[TrackRecommendation] = Field(..., description="Liste des recommandations")


class SimilarTracksResponse(BaseModel):
    """Réponse pour les tracks similaires."""
    track_id: int = Field(..., description="ID du track source")
    similar_tracks: List[TrackRecommendation] = Field(..., description="Tracks similaires")


class HealthResponse(BaseModel):
    """Réponse de health check."""
    status: str
    model_loaded: bool
    n_users: Optional[int] = None
    n_items: Optional[int] = None


class StatsResponse(BaseModel):
    """Statistiques du service."""
    n_users: int
    n_items: int
    n_interactions: int
    sparsity: float
    model_factors: int
    model_regularization: float


class HistoryItem(BaseModel):
    """Un item de l'historique."""
    track: str
    artist: str
    confidence_score: float
    item_id: int


class HistoryResponse(BaseModel):
    """Historique d'écoute."""
    user_id: str
    history: List[HistoryItem]


# Service singleton
service = RecommendationService.get_instance()


@app.on_event("startup")
async def startup_event():
    """Charge le modèle au démarrage."""
    try:
        if MODEL_PATH.exists() and MATRIX_PATH.exists():
            service.load(
                model_path=MODEL_PATH,
                matrix_path=MATRIX_PATH,
                mappings_path=MAPPINGS_PATH if MAPPINGS_PATH.exists() else None
            )
            print("✅ Modèle chargé avec succès")
        else:
            print("⚠️ Fichiers du modèle non trouvés. L'API démarre sans modèle.")
            print(f"   Model: {MODEL_PATH} (exists: {MODEL_PATH.exists()})")
            print(f"   Matrix: {MATRIX_PATH} (exists: {MATRIX_PATH.exists()})")
    except Exception as e:
        print(f"❌ Erreur au chargement du modèle: {e}")


@app.get("/", tags=["Info"])
async def root():
    """Page d'accueil de l'API."""
    return {
        "name": "Music Recommendation API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", response_model=HealthResponse, tags=["Info"])
async def health_check():
    """Vérifie l'état du service."""
    if service.is_loaded:
        stats = service.get_stats()
        return HealthResponse(
            status="healthy",
            model_loaded=True,
            n_users=stats["n_users"],
            n_items=stats["n_items"]
        )
    return HealthResponse(
        status="degraded",
        model_loaded=False
    )


@app.get("/stats", response_model=StatsResponse, tags=["Info"])
async def get_stats():
    """Retourne les statistiques du modèle."""
    if not service.is_loaded:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    stats = service.get_stats()
    return StatsResponse(**stats)


@app.get("/recommend/{user_id}", response_model=RecommendationResponse, tags=["Recommendations"])
async def recommend(
    user_id: str,
    n: int = Query(default=10, ge=1, le=100, description="Nombre de recommandations"),
    filter_liked: bool = Query(default=True, description="Exclure les tracks déjà écoutés")
):
    """
    Génère des recommandations personnalisées pour un utilisateur.

    - **user_id**: ID numérique ou nom d'utilisateur
    - **n**: Nombre de recommandations (1-100)
    - **filter_liked**: Exclure les tracks déjà dans l'historique
    """
    if not service.is_loaded:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    try:
        recommendations = service.recommend(user_id, n, filter_liked)
        return RecommendationResponse(
            user_id=user_id,
            recommendations=[TrackRecommendation(**r) for r in recommendations]
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")


@app.get("/similar/{track_id}", response_model=SimilarTracksResponse, tags=["Recommendations"])
async def similar_tracks(
    track_id: int,
    n: int = Query(default=10, ge=1, le=50, description="Nombre de tracks similaires")
):
    """
    Trouve les tracks similaires à un track donné.

    - **track_id**: ID du track
    - **n**: Nombre de tracks similaires (1-50)
    """
    if not service.is_loaded:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    try:
        similar = service.similar_tracks(track_id, n)
        return SimilarTracksResponse(
            track_id=track_id,
            similar_tracks=[TrackRecommendation(**t) for t in similar]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")


@app.get("/history/{user_id}", response_model=HistoryResponse, tags=["Users"])
async def user_history(
    user_id: str,
    n: int = Query(default=20, ge=1, le=100, description="Nombre d'items")
):
    """
    Retourne l'historique d'écoute d'un utilisateur.

    - **user_id**: ID numérique ou nom d'utilisateur
    - **n**: Nombre d'items à retourner (1-100)
    """
    if not service.is_loaded:
        raise HTTPException(status_code=503, detail="Modèle non chargé")

    try:
        history = service.get_user_history(user_id, n)
        return HistoryResponse(
            user_id=user_id,
            history=[HistoryItem(**h) for h in history]
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")


@app.post("/reload", tags=["Admin"])
async def reload_model():
    """
    Recharge le modèle depuis le disque.
    Utile après un réentraînement.
    """
    try:
        service.load(
            model_path=MODEL_PATH,
            matrix_path=MATRIX_PATH,
            mappings_path=MAPPINGS_PATH if MAPPINGS_PATH.exists() else None
        )
        return {"status": "success", "message": "Modèle rechargé"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur au rechargement: {str(e)}")
