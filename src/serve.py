#!/usr/bin/env python3
"""
Script de démarrage du serveur API.
"""
import argparse
import os

import uvicorn


def main():
    parser = argparse.ArgumentParser(description="Démarrer le serveur de recommandation")
    parser.add_argument("--host", default="0.0.0.0", help="Adresse d'écoute")
    parser.add_argument("--port", type=int, default=8000, help="Port d'écoute")
    parser.add_argument("--reload", action="store_true", help="Activer le hot reload (dev)")
    parser.add_argument("--workers", type=int, default=1, help="Nombre de workers")

    args = parser.parse_args()

    print("=" * 60)
    print("DÉMARRAGE DU SERVEUR DE RECOMMANDATION")
    print("=" * 60)
    print(f"Host: {args.host}")
    print(f"Port: {args.port}")
    print(f"Workers: {args.workers}")
    print(f"Reload: {args.reload}")
    print("=" * 60)

    uvicorn.run(
        "api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers if not args.reload else 1
    )


if __name__ == "__main__":
    main()
