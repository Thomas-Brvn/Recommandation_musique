"""
Vector Store Festivals 2026 - Pinecone + LangChain
Charge les données depuis S3 et stocke les vecteurs sur Pinecone

Ton index Pinecone:
- Dimensions: 512
- Model: text-embedding-3-small (intégré)
- Host: https://festival-gi2cz9u.svc.aped-4627-b74a.pinecone.io
"""

import json
import os
import boto3
from dotenv import load_dotenv
from pinecone import Pinecone
from openai import OpenAI

# Charger les variables d'environnement
load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

BUCKET_NAME = "projet-etude-m2"
S3_KEY = "data_musique/festival/festivals_2026.json"
AWS_REGION = "eu-west-3"
INDEX_NAME = "festival"
EMBEDDING_MODEL = "text-embedding-3-small"


# ============================================================================
# FONCTIONS
# ============================================================================

def load_festivals_from_s3() -> list[dict]:
    """Charge les festivals depuis S3."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=AWS_REGION
    )
    
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_KEY)
    festivals = json.loads(response['Body'].read().decode('utf-8'))
    
    print(f" {len(festivals)} festivals chargés depuis S3")
    return festivals


def create_vector_store(festivals: list[dict]):
    """
    Crée le vector store sur Pinecone.
    Génère les embeddings avec OpenAI text-embedding-3-small.
    """

    pc = Pinecone(api_key=os.getenv('PINECONE_API_KEY'))
    index = pc.Index(INDEX_NAME)
    openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    # Préparer les textes et métadonnées
    texts = []
    metadatas = []
    ids = []

    for i, festival in enumerate(festivals):
        artistes = festival.get("artistes", [])
        if not artistes:
            continue

        # Métadonnées
        artistes_text = ", ".join(artistes) if isinstance(artistes, list) else str(artistes)
        metadata = {
            "nom": festival.get("nom", ""),
            "dates": festival.get("dates", ""),
            "lieu": festival.get("lieu", ""),
            "billetterie": festival.get("billetterie") or "",
            "text": artistes_text
        }

        texts.append(artistes_text)
        metadatas.append(metadata)
        ids.append(f"festival_{i}")

    # Générer les embeddings avec OpenAI (512 dimensions pour correspondre à l'index)
    print(f"Génération des embeddings pour {len(texts)} festivals...")
    response = openai_client.embeddings.create(
        input=texts,
        model=EMBEDDING_MODEL,
        dimensions=512
    )
    embeddings = [item.embedding for item in response.data]

    # Préparer les vecteurs pour Pinecone
    vectors = []
    for id_, embedding, metadata in zip(ids, embeddings, metadatas):
        vectors.append({
            "id": id_,
            "values": embedding,
            "metadata": metadata
        })

    # Upsert dans Pinecone
    index.upsert(vectors=vectors, namespace="__default__")

    print(f"{len(vectors)} festivals indexés sur Pinecone")
    return index


def search(query: str, k: int = 5):
    """Recherche par similarité."""

    pc = Pinecone(api_key=os.getenv('PINECONE_API_KEY'))
    index = pc.Index(INDEX_NAME)
    openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    # Générer l'embedding de la query (512 dimensions)
    response = openai_client.embeddings.create(
        input=[query],
        model=EMBEDDING_MODEL,
        dimensions=512
    )
    query_embedding = response.data[0].embedding

    # Recherche dans Pinecone
    results = index.query(
        namespace="__default__",
        vector=query_embedding,
        top_k=k,
        include_metadata=True
    )

    print(f"\n🔍 Recherche: '{query}'")
    print("=" * 60)
    for i, match in enumerate(results["matches"], 1):
        m = match.get("metadata", {})
        nom = m.get('nom', 'N/A')
        dates = m.get('dates', 'N/A')
        lieu = m.get('lieu', 'N/A')
        score = match.get('score', 0)
        print(f"{i}. 🎪 {nom} (score: {score:.4f})")
        print(f"   📅 {dates} | 📍 {lieu}")
    print("=" * 60)

    return results


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    festivals = load_festivals_from_s3()
    create_vector_store(festivals)
    print("Vector Store OK")