"""
FastAPI - Agent RAG Festivals 2026
"""

import uuid
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

from agent.agent import ask

# ============================================================================
# APP
# ============================================================================

app = FastAPI(
    title="Festival RAG API",
    description="Agent IA pour découvrir les festivals de musique en France 2026",
    version="1.0.0",
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
def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest):
    session_id = request.session_id or str(uuid.uuid4())

    history = _sessions.get(session_id, [])

    try:
        answer = ask(question=request.question, chat_history=history)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Mettre à jour l'historique (format LangChain : tuples human/ai)
    history.append(("human", request.question))
    history.append(("ai", answer))
    _sessions[session_id] = history

    return ChatResponse(answer=answer, session_id=session_id)


@app.get("/sessions/{session_id}/history", response_model=list[Message])
def get_history(session_id: str):
    history = _sessions.get(session_id)
    if history is None:
        raise HTTPException(status_code=404, detail="Session introuvable")
    return [Message(role=role, content=content) for role, content in history]


@app.delete("/sessions/{session_id}")
def delete_session(session_id: str):
    _sessions.pop(session_id, None)
    return {"deleted": session_id}
