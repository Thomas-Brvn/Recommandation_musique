"""
API FastAPI pour le système de recommandation musicale.
"""
import asyncio
import importlib.util
import sys
import uuid
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .catalog import CatalogService
from .cover_service import get_cover_url
from .library import LibraryService
from .recommender import RecommendationService

# ---------------------------------------------------------------------------
# Import de l'agent festival (src/app/agent/)
# ---------------------------------------------------------------------------
_AGENT_DIR = str(Path(__file__).parent.parent / "app" / "agent")
if _AGENT_DIR not in sys.path:
    sys.path.insert(0, _AGENT_DIR)

_festival_ask = None
try:
    _spec = importlib.util.spec_from_file_location(
        "festival_agent",
        Path(_AGENT_DIR) / "agent.py",
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    _festival_ask = _mod.ask
    print("✅ Agent festival chargé")
except Exception as _e:
    print(f"⚠️  Agent festival non disponible: {_e}")

_festival_sessions: dict[str, list] = {}

# Configuration S3
S3_BUCKET = os.getenv("S3_BUCKET_MODEL", "brainz-data")
S3_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
S3_MODEL_KEY = os.getenv("S3_MODEL_KEY", "models/als_model.pkl")
S3_MATRIX_KEY = os.getenv("S3_MATRIX_KEY", "processed/user_item_matrix.npz")
S3_MAPPINGS_KEY = os.getenv("S3_MAPPINGS_KEY", "processed/mappings.json")
S3_CATALOG_KEY = os.getenv("S3_CATALOG_KEY", "processed/track_dedup_map.json")

STATIC_DIR = Path(__file__).parent.parent / "static"

# Chemins locaux (fallback si S3 non configuré)
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"
MODELS_DIR = BASE_DIR / "models"
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


# Services singletons
service = RecommendationService.get_instance()
catalog  = CatalogService.get_instance()
library  = LibraryService.get_instance()


async def _load_model():
    """Charge le modèle depuis S3 (prioritaire) ou depuis le disque local."""
    if S3_BUCKET:
        await service.load_from_s3(
            bucket=S3_BUCKET,
            model_key=S3_MODEL_KEY,
            matrix_key=S3_MATRIX_KEY,
            mappings_key=S3_MAPPINGS_KEY,
            region=S3_REGION,
        )
    elif MODEL_PATH.exists() and MATRIX_PATH.exists():
        await service.load(
            model_path=MODEL_PATH,
            matrix_path=MATRIX_PATH,
            mappings_path=MAPPINGS_PATH if MAPPINGS_PATH.exists() else None,
        )
    else:
        raise FileNotFoundError(
            "Aucune source de modèle disponible. "
            "Définissez S3_BUCKET_MODEL ou placez les fichiers localement."
        )


async def _load_catalog():
    """Charge le catalogue de tracks depuis S3."""
    if S3_BUCKET:
        await catalog.load_from_s3(bucket=S3_BUCKET, key=S3_CATALOG_KEY, region=S3_REGION)
    else:
        raise FileNotFoundError("S3_BUCKET_MODEL requis pour charger le catalogue.")


@app.on_event("startup")
async def startup_event():
    """Charge le catalogue et la bibliothèque au démarrage."""
    try:
        await _load_catalog()
        print("✅ Catalogue chargé avec succès")
    except Exception as e:
        print(f"❌ Erreur au chargement du catalogue: {e}")
    try:
        await library.load()
        print("✅ Bibliothèque chargée avec succès")
    except Exception as e:
        print(f"❌ Erreur au chargement de la bibliothèque: {e}")


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
        stats = await service.get_stats()
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

    stats = await service.get_stats()
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
        recommendations = await service.recommend(user_id, n, filter_liked)
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
        similar = await service.similar_tracks(track_id, n)
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
        history = await service.get_user_history(user_id, n)
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
    Recharge le modèle (depuis S3 ou disque).
    Utile après un réentraînement, sans redémarrer l'API.
    """
    try:
        await _load_model()
        return {"status": "success", "message": "Modèle rechargé"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur au rechargement: {str(e)}")


# ---------------------------------------------------------------------------
# Catalogue de tracks
# ---------------------------------------------------------------------------

class TrackItem(BaseModel):
    id: int
    canonical_name: str
    artist: str
    title: str


class CatalogPage(BaseModel):
    total: int
    page: int
    size: int
    tracks: List[TrackItem]


@app.get("/catalog/tracks", response_model=CatalogPage, tags=["Catalog"])
async def list_tracks(
    page: int = Query(default=0, ge=0),
    size: int = Query(default=48, ge=1, le=200),
):
    """Retourne une page du catalogue de tracks."""
    if not catalog.is_loaded:
        raise HTTPException(status_code=503, detail="Catalogue non chargé")
    return CatalogPage(
        total=catalog.total(),
        page=page,
        size=size,
        tracks=[TrackItem(**t) for t in catalog.get_page(page, size)],
    )


@app.get("/catalog/search", response_model=List[TrackItem], tags=["Catalog"])
async def search_tracks(
    q: str = Query(..., min_length=1, description="Texte à rechercher"),
    limit: int = Query(default=24, ge=1, le=100),
):
    """Recherche des tracks par artiste ou titre."""
    if not catalog.is_loaded:
        raise HTTPException(status_code=503, detail="Catalogue non chargé")
    results = catalog.search(q, limit)
    return [TrackItem(**t) for t in results]


@app.get("/catalog/cover", tags=["Catalog"])
async def get_album_cover(
    artist: str = Query(..., description="Nom de l'artiste"),
    title: str = Query(..., description="Titre du morceau"),
):
    """
    Retourne l'URL de la cover d'album via l'iTunes Search API.
    Résultat mis en cache en mémoire.
    """
    url = await get_cover_url(artist, title)
    return {"url": url}


# ---------------------------------------------------------------------------
# Bibliothèque : Likes & Playlists
# ---------------------------------------------------------------------------

class TrackPayload(BaseModel):
    item_id: int
    artist: str
    title: str
    canonical_name: str = ""


class PlaylistCreate(BaseModel):
    name: str


class PlaylistRename(BaseModel):
    name: str


# ── Likes ────────────────────────────────────────────────────────────────

@app.post("/library/{user_id}/likes", status_code=204, tags=["Library"])
async def like_track(user_id: str, track: TrackPayload):
    await library.like(user_id, track.model_dump())


@app.delete("/library/{user_id}/likes/{item_id}", status_code=204, tags=["Library"])
async def unlike_track(user_id: str, item_id: int):
    await library.unlike(user_id, item_id)


@app.get("/library/{user_id}/likes", tags=["Library"])
async def get_likes(user_id: str):
    return await library.get_likes(user_id)


@app.get("/library/{user_id}/likes/{item_id}", tags=["Library"])
async def check_liked(user_id: str, item_id: int):
    return {"liked": await library.is_liked(user_id, item_id)}


# ── Playlists ─────────────────────────────────────────────────────────────

@app.post("/library/{user_id}/playlists", tags=["Library"])
async def create_playlist(user_id: str, body: PlaylistCreate):
    return await library.create_playlist(user_id, body.name)


@app.get("/library/{user_id}/playlists", tags=["Library"])
async def get_playlists(user_id: str):
    return await library.get_playlists(user_id)


@app.get("/library/{user_id}/playlists/{playlist_id}", tags=["Library"])
async def get_playlist(user_id: str, playlist_id: str):
    pl = await library.get_playlist(user_id, playlist_id)
    if not pl:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    return pl


@app.patch("/library/{user_id}/playlists/{playlist_id}", tags=["Library"])
async def rename_playlist(user_id: str, playlist_id: str, body: PlaylistRename):
    ok = await library.rename_playlist(user_id, playlist_id, body.name)
    if not ok:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    return {"renamed": True}


@app.delete("/library/{user_id}/playlists/{playlist_id}", status_code=204, tags=["Library"])
async def delete_playlist(user_id: str, playlist_id: str):
    await library.delete_playlist(user_id, playlist_id)


@app.post("/library/{user_id}/playlists/{playlist_id}/tracks", status_code=204, tags=["Library"])
async def add_to_playlist(user_id: str, playlist_id: str, track: TrackPayload):
    ok = await library.add_to_playlist(user_id, playlist_id, track.model_dump())
    if not ok:
        raise HTTPException(status_code=404, detail="Playlist introuvable")


@app.delete("/library/{user_id}/playlists/{playlist_id}/tracks/{item_id}", status_code=204, tags=["Library"])
async def remove_from_playlist(user_id: str, playlist_id: str, item_id: int):
    await library.remove_from_playlist(user_id, playlist_id, item_id)


# ---------------------------------------------------------------------------
# Agent Festival RAG
# ---------------------------------------------------------------------------

class FestivalChatRequest(BaseModel):
    question: str
    session_id: Optional[str] = None


class FestivalChatResponse(BaseModel):
    answer: str
    session_id: str


@app.post("/festival/chat", response_model=FestivalChatResponse, tags=["Festival"])
async def festival_chat(request: FestivalChatRequest):
    """Pose une question à l'agent RAG festivals 2026."""
    if _festival_ask is None:
        raise HTTPException(status_code=503, detail="Agent festival non disponible (vérifiez GOOGLE_API_KEY, PINECONE_API_KEY, OPENAI_API_KEY)")

    session_id = request.session_id or str(uuid.uuid4())
    history = _festival_sessions.get(session_id, [])

    try:
        raw = await asyncio.to_thread(_festival_ask, request.question, history)
        # Gemini peut retourner une liste de blocs [{type, text}] ou une string
        if isinstance(raw, list):
            # Ne garder que les blocs de type "text" (ignorer "thinking", "executable_code", etc.)
            answer = "".join(
                block.get("text", "")
                for block in raw
                if isinstance(block, dict) and block.get("type") == "text"
            ).strip()
            if not answer:
                # Fallback: concaténer tout contenu textuel disponible
                answer = "".join(
                    block.get("text", "") for block in raw if isinstance(block, dict)
                ).strip()
        else:
            answer = str(raw)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    history.append(("human", request.question))
    history.append(("ai", answer))
    _festival_sessions[session_id] = history

    return FestivalChatResponse(answer=answer, session_id=session_id)


@app.delete("/festival/sessions/{session_id}", tags=["Festival"])
async def delete_festival_session(session_id: str):
    _festival_sessions.pop(session_id, None)
    return {"deleted": session_id}


# ---------------------------------------------------------------------------
# Frontend statique
# ---------------------------------------------------------------------------

if STATIC_DIR.exists():
    app.mount("/app", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

    @app.get("/player", include_in_schema=False)
    async def serve_frontend():
        return FileResponse(str(STATIC_DIR / "index.html"))
