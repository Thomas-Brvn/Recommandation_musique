"""
Vector Store Festivals 2026 - ChromaDB local
Charge les données depuis le fichier local et stocke les vecteurs dans ChromaDB
"""

import json
import os
import chromadb
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

load_dotenv()

FESTIVALS_FILE = os.getenv("FESTIVALS_FILE", "data/festivals_2026.json")
CHROMA_HOST = os.getenv("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.getenv("CHROMA_PORT", "8000"))
COLLECTION_NAME = "festival"
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"


def load_festivals_from_file() -> list[dict]:
    """Charge les festivals depuis le fichier local."""
    with open(FESTIVALS_FILE, 'r', encoding='utf-8') as f:
        festivals = json.load(f)
    print(f"{len(festivals)} festivals chargés depuis {FESTIVALS_FILE}")
    return festivals


def _get_chroma_client():
    if os.getenv("CHROMA_HTTP", "false").lower() == "true":
        return chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
    return chromadb.PersistentClient(path=os.getenv("CHROMA_PATH", "./data/chroma"))


def create_vector_store(festivals: list[dict]):
    """Crée le vector store dans ChromaDB avec des embeddings locaux."""
    client = _get_chroma_client()
    model = SentenceTransformer(EMBEDDING_MODEL)

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"}
    )

    texts = []
    metadatas = []
    ids = []

    for i, festival in enumerate(festivals):
        artistes = festival.get("artistes", [])
        if not artistes:
            continue

        artistes_text = ", ".join(artistes) if isinstance(artistes, list) else str(artistes)
        metadata = {
            "nom": festival.get("nom", ""),
            "dates": festival.get("dates", ""),
            "lieu": festival.get("lieu", ""),
            "billetterie": festival.get("billetterie") or "",
            "text": artistes_text,
        }

        texts.append(artistes_text)
        metadatas.append(metadata)
        ids.append(f"festival_{i}")

    print(f"Génération des embeddings pour {len(texts)} festivals...")
    embeddings = model.encode(texts, show_progress_bar=True).tolist()

    collection.upsert(
        ids=ids,
        embeddings=embeddings,
        metadatas=metadatas,
        documents=texts,
    )

    print(f"{len(texts)} festivals indexés dans ChromaDB")
    return collection


def search(query: str, k: int = 5):
    """Recherche par similarité."""
    client = _get_chroma_client()
    model = SentenceTransformer(EMBEDDING_MODEL)
    collection = client.get_collection(COLLECTION_NAME)

    query_embedding = model.encode([query]).tolist()[0]
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=k,
        include=["metadatas", "distances"],  # type: ignore[arg-type]
    )

    metadatas = results["metadatas"] or []
    distances = results["distances"] or []

    print(f"\nRecherche: '{query}'")
    print("=" * 60)
    for m, dist in zip(metadatas[0] if metadatas else [], distances[0] if distances else []):
        score = 1 - dist
        print(f"  {m.get('nom', 'N/A')} (score: {score:.4f})")
        print(f"   {m.get('dates', 'N/A')} | {m.get('lieu', 'N/A')}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    festivals = load_festivals_from_file()
    create_vector_store(festivals)
    print("Vector Store OK")
