"""
Tools pour l'agent RAG Festivals 2026
Outil de recherche dans le vector store ChromaDB local
"""

import os
import chromadb
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from langchain.tools import tool

load_dotenv()

COLLECTION_NAME = "festival"
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"

_model: SentenceTransformer | None = None
_client = None


def _get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def _get_client():
    global _client
    if _client is None:
        if os.getenv("CHROMA_HTTP", "false").lower() == "true":
            _client = chromadb.HttpClient(
                host=os.getenv("CHROMA_HOST", "localhost"),
                port=int(os.getenv("CHROMA_PORT", "8000")),
            )
        else:
            _client = chromadb.PersistentClient(
                path=os.getenv("CHROMA_PATH", "./data/chroma")
            )
    return _client


@tool
def search_festival_store(query: str) -> str:
    """
    Recherche des festivals dans le vector store ChromaDB.

    Args:
        query: La requête de recherche (artiste, genre, lieu, etc.)

    Returns:
        Les informations des festivals trouvés formatées en string
    """
    try:
        collection = _get_client().get_collection(COLLECTION_NAME)
    except Exception:
        return (
            "La collection ChromaDB n'existe pas encore. "
            "Lancez festival_to_vectorstore.py pour indexer les festivals."
        )

    query_embedding = _get_model().encode([query]).tolist()[0]
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=5,
        include=["metadatas", "distances"],  # type: ignore[arg-type]
    )

    metadatas = results["metadatas"] or []
    distances = results["distances"] or []
    if not metadatas or not metadatas[0]:
        return "Aucun festival trouvé pour cette recherche."

    output = []
    for i, (m, dist) in enumerate(zip(metadatas[0], distances[0]), 1):
        score = 1 - dist
        output.append(f"""
Festival {i}:
- Nom: {m.get('nom', 'N/A')}
- Dates: {m.get('dates', 'N/A')}
- Lieu: {m.get('lieu', 'N/A')}
- Artistes: {m.get('text', 'N/A')}
- Billetterie: {m.get('billetterie', 'Non disponible')}
- Score de pertinence: {score:.4f}
""")

    return "\n".join(output)


if __name__ == "__main__":
    print("Test de l'outil festival_store\n")
    print("=" * 60)
    result = search_festival_store.invoke({"query": "GIMS"})
    print(result)
