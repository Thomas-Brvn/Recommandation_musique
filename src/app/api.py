"""
FastAPI - Agent RAG Festivals 2026
"""

import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

from agent.agent import ask  # noqa: E402

log = logging.getLogger("festival_api")


def _auto_index() -> None:
    """Indexe la collection ChromaDB si elle est vide ou absente."""
    try:
        from load_festival.festival_to_vectorstore import (
            _get_chroma_client,
            create_vector_store,
            load_festivals_from_file,
        )

        client = _get_chroma_client()
        try:
            col = client.get_collection("festival")
            if col.count() > 0:
                log.info("ChromaDB déjà indexé (%d documents).", col.count())
                return
        except Exception:
            pass  # collection absente → on indexe

        festivals_file = os.getenv("FESTIVALS_FILE", "data/festivals_2026.json")
        if not Path(festivals_file).exists():
            log.warning("Fichier festivals introuvable (%s) — indexation ignorée.", festivals_file)
            return

        log.info("Collection vide ou absente — indexation automatique depuis %s…", festivals_file)
        festivals = load_festivals_from_file()
        create_vector_store(festivals)
        log.info("Indexation automatique terminée.")
    except Exception as exc:
        log.error("Erreur lors de l'auto-indexation : %s", exc)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await asyncio.to_thread(_auto_index)
    yield


# ============================================================================
# APP
# ============================================================================

app = FastAPI(
    title="Festival RAG API",
    description="Agent IA pour découvrir les festivals de musique en France 2026",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Stockage en mémoire des historiques de session
_sessions: dict[str, list] = {}

# ============================================================================
# SCHEMAS
# ============================================================================

class ChatRequest(BaseModel):
    question: str
    session_id: str | None = None  # si None, nouvelle session créée

class ChatResponse(BaseModel):
    answer: str
    session_id: str

class Message(BaseModel):
    role: str  # "human" ou "ai"
    content: str

# ============================================================================
# ROUTES
# ============================================================================

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    session_id = request.session_id or str(uuid.uuid4())

    history = _sessions.get(session_id, [])

    try:
        answer = await ask(question=request.question, chat_history=history)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    history.append(("human", request.question))
    history.append(("ai", answer))
    _sessions[session_id] = history

    return ChatResponse(answer=answer, session_id=session_id)


@app.get("/sessions/{session_id}/history", response_model=list[Message])
async def get_history(session_id: str):
    history = _sessions.get(session_id)
    if history is None:
        raise HTTPException(status_code=404, detail="Session introuvable")
    return [Message(role=role, content=content) for role, content in history]


@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    _sessions.pop(session_id, None)
    return {"deleted": session_id}
