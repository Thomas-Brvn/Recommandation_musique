"""
Scraper pour les festivals musicaux de l'été 2026
Avec export JSON vers Amazon S3
"""

import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Optional
import json
import re
import os
import boto3
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

@dataclass
class Festival:
    nom: str
    dates: str
    lieu: str
    artistes: list[str]
    billetterie_url: Optional[str] = None


def scrape_festivals(url: str) -> list[Festival]:
    """
    Scrape les informations des festivals depuis la page web.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    festivals = []
    
    # Trouver tous les h3 (noms des festivals)
    h3_tags = soup.find_all('h3')
    
    for h3 in h3_tags:
        nom_complet = h3.get_text(strip=True)
        
        # Extraire le nom et les dates
        match = re.match(r'^(.+?)\s*:\s*(.+)$', nom_complet)
        if match:
            nom = match.group(1).strip()
            dates = match.group(2).strip()
        else:
            continue
        
        # Trouver le contenu suivant le h3
        sibling = h3.find_next_sibling()
        
        lieu = ""
        artistes = []
        billetterie = None
        
        while sibling and sibling.name != 'h3' and sibling.name != 'h2':
            text = sibling.get_text(strip=True)
            
            # Chercher le lieu (ligne commençant par les dates)
            if text.startswith('Du ') or text.startswith('Le '):
                lieu_match = re.search(r'(?:à|au|dans)\s+(.+?)(?:\s*La billetterie|$)', text)
                if lieu_match:
                    lieu = lieu_match.group(1).strip()
            
            # Chercher les liens de billetterie
            links = sibling.find_all('a', href=True)
            for link in links:
                href = link['href']
                if 'billetterie' in link.get_text().lower() or any(
                    domain in href for domain in [
                        'welovegreen', 'solidays', 'rockenseine', 'hellfest',
                        'eurockeennes', 'francofolies', 'vieillescharrues',
                        'garorock', 'musilac', 'mainsquare', 'cabaretvert'
                    ]
                ):
                    billetterie = href if href.startswith('http') else None
            
            # Chercher les artistes (liens vers /artiste/)
            artiste_links = sibling.find_all('a', href=re.compile(r'/artiste/'))
            for link in artiste_links:
                artiste = link.get_text(strip=True)
                if artiste and artiste not in artistes:
                    artistes.append(artiste)
            
            sibling = sibling.find_next_sibling()
        
        if nom and dates:
            festivals.append(Festival(
                nom=nom,
                dates=dates,
                lieu=lieu,
                artistes=artistes,
                billetterie_url=billetterie
            ))
    
    return festivals


def export_to_json(festivals: list[Festival], filename: str = "festivals_2026.json") -> str:
    """Exporte les festivals en JSON dans le dossier data/."""
    os.makedirs("data", exist_ok=True)
    filepath = os.path.join("data", filename)
    data = [
        {
            "nom": f.nom,
            "dates": f.dates,
            "lieu": f.lieu,
            "artistes": f.artistes,
            "billetterie": f.billetterie_url
        }
        for f in festivals
    ]
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Exporté vers {filepath}")
    return filepath


def upload_to_s3(
    file_path: str,
    bucket_name: str,
    s3_key: str,
    region_name: str = "eu-west-3"
) -> bool:
    """
    Upload un fichier vers S3 en utilisant les credentials du .env
    
    Variables attendues dans .env:
        AWS_ACCESS_KEY_ID=votre_access_key
        AWS_SECRET_ACCESS_KEY=votre_secret_key
    """
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if not aws_access_key or not aws_secret_key:
        print("❌ Credentials AWS non trouvés dans le fichier .env")
        print("   Assurez-vous que AWS_ACCESS_KEY_ID et AWS_SECRET_ACCESS_KEY sont définis")
        return False
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
        
        s3_client.upload_file(
            file_path,
            bucket_name,
            s3_key,
            ExtraArgs={'ContentType': 'application/json'}
        )
        
        print(f"✅ Uploadé vers s3://{bucket_name}/{s3_key}")
        return True
        
    except Exception as e:
        print(f"❌ Erreur S3: {e}")
        return False


if __name__ == "__main__":
    # Configuration S3
    BUCKET_NAME = "projet-etude-m2"
    S3_PREFIX = "data_musique/festival/"
    FILENAME = "festivals_2026.json"
    
    # 1. Scraper les festivals
    print("🔍 Scraping des festivals...")
    url = "https://www.offi.fr/tendances/concerts/les-grands-festivals-musicaux-de-lete-2026-942.html"
    festivals = scrape_festivals(url)
    print(f"   {len(festivals)} festivals trouvés")
    
    # 2. Exporter en JSON local
    print("\n💾 Export JSON local...")
    json_path = export_to_json(festivals, FILENAME)
    
    # 3. Upload vers S3
    print("\n☁️  Upload vers S3...")
    s3_key = f"{S3_PREFIX}{FILENAME}"
    upload_to_s3(json_path, BUCKET_NAME, s3_key)
    
    print("\n✨ Terminé !")