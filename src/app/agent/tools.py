"""
Tools pour l'agent RAG Festivals 2026
Outil de recherche dans le vector store Pinecone
"""

import os
from dotenv import load_dotenv
from pinecone import Pinecone
from openai import OpenAI
from langchain.tools import tool

# Charger les variables d'environnement
load_dotenv()

INDEX_NAME = "festival"
EMBEDDING_MODEL = "text-embedding-3-small"

@tool
def search_festival_store(query: str) -> str:
    """
    Recherche des festivals dans le vector store Pinecone.
    
    Args:
        query: La requête de recherche (artiste, genre, lieu, etc.)
    
    Returns:
        Les informations des festivals trouvés formatées en string
    """
    # Initialiser Pinecone et OpenAI
    pc = Pinecone(api_key=os.getenv('PINECONE_API_KEY'))
    index = pc.Index(INDEX_NAME)
    openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    # Générer l'embedding de la query avec OpenAI (512 dimensions)
    response = openai_client.embeddings.create(
        input=query,  # Pass string directly, not as list
        model=EMBEDDING_MODEL,
        dimensions=512
    )
    query_embedding = response.data[0].embedding

    # Recherche dans Pinecone avec l'embedding généré
    results = index.query(
        namespace="__default__",
        vector=query_embedding,
        top_k=5,
        include_metadata=True
    )
    
    # Vérifier si des résultats ont été trouvés
    if not results.get("matches"):
        return "Aucun festival trouvé pour cette recherche."
    
    # Formater les résultats
    output = []
    for i, match in enumerate(results["matches"], 1):
        m = match.get("metadata", match)
        
        festival_info = f"""
Festival {i}:
- Nom: {m.get('nom', 'N/A')}
- Dates: {m.get('dates', 'N/A')}
- Lieu: {m.get('lieu', 'N/A')}
- Artistes: {m.get('text', 'N/A')}
- Billetterie: {m.get('billetterie', 'Non disponible')}
- Score de pertinence: {match.get('score', 0):.4f}
"""
        output.append(festival_info)
    
    return "\n".join(output)

# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    print("🔍 Test de l'outil festival_store\n")
    print("=" * 60)
    print("Test 1: Recherche 'GIMS'")
    print("=" * 60)
    result = search_festival_store.invoke({"query": "GIMS"})
    print(result)
    