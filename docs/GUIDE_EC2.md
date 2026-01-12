# Guide EC2 - Téléchargement direct vers S3

Ce guide explique comment utiliser une instance EC2 temporaire pour télécharger les données **directement vers S3**, sans passer par votre machine locale.

## Pourquoi EC2?

### Avantages
- **Pas de téléchargement local** - Les 100 GB ne passent pas par votre connexion internet
- **Beaucoup plus rapide** - Bande passante AWS (plusieurs Gb/s)
- **Gratuit** - Transfert EC2 → S3 dans la même région = 0€
- **Économique** - Instance t3.medium: ~0.05 USD/heure
- **Automatique** - Tout se fait automatiquement au démarrage

### Coûts estimés

**MusicBrainz uniquement (~7 GB):**
- Instance t3.medium (0.5h): ~0.03 USD
- Stockage S3: ~0.16 USD/mois
- **Total: ~0.03 USD**

**Avec ListenBrainz (~100 GB):**
- Instance t3.medium (4h): ~0.20 USD
- Stockage S3: ~2.46 USD/mois
- **Total: ~0.20 USD pour le téléchargement**

## Prérequis

1. **Compte AWS configuré** avec `aws configure`
2. **Permissions IAM** pour:
   - Lancer des instances EC2
   - Créer des rôles IAM
   - Accès S3

## Utilisation

### Méthode 1: Script automatique (Recommandé)

```bash
python scripts/download_to_s3_via_ec2.py
```

Le script va:
1. Charger votre configuration S3
2. Vous demander quoi télécharger (MusicBrainz, ListenBrainz, ou les deux)
3. Créer un rôle IAM si nécessaire
4. Lancer l'instance EC2
5. Configurer le téléchargement automatique

### Méthode 2: Menu interactif

```bash
./quick_start.sh
# Puis choisir l'option "EC2"
```

## Monitoring

### Option 1: Script de monitoring automatique

```bash
python scripts/monitor_ec2_download.py
```

Ce script:
- Affiche le statut de l'instance en temps réel
- Montre les logs de téléchargement
- Vérifie les fichiers uploadés sur S3
- Vous alerte quand c'est terminé

### Option 2: Commandes AWS CLI

**Voir les logs:**
```bash
aws ec2 get-console-output --instance-id i-xxxxx --region eu-west-3
```

**Voir le statut:**
```bash
aws ec2 describe-instances --instance-ids i-xxxxx --region eu-west-3
```

**Vérifier S3:**
```bash
aws s3 ls s3://votre-bucket/raw/ --recursive --human-readable
```

### Option 3: Console AWS

1. Allez sur https://console.aws.amazon.com/ec2
2. Trouvez l'instance nommée "MusicData-Downloader"
3. Cliquez sur "Actions" → "Monitor and troubleshoot" → "Get system log"

## Que fait l'instance?

L'instance EC2 exécute automatiquement ce workflow:

1. **Démarrage** - Installation de wget et AWS CLI
2. **Téléchargement** - Pour chaque fichier:
   - Télécharge depuis MusicBrainz/ListenBrainz
   - Upload immédiatement vers S3
   - Supprime le fichier local (économie d'espace)
3. **Completion** - Crée un fichier `.download-completed` sur S3
4. **Prêt à arrêter** - L'instance attend que vous la terminiez

## Arrêt de l'instance

⚠️ **IMPORTANT**: Terminez l'instance après le téléchargement pour éviter les frais!

### Arrêter l'instance (la garder pour plus tard)
```bash
aws ec2 stop-instances --instance-ids i-xxxxx --region eu-west-3
```

### Terminer l'instance (supprimer définitivement)
```bash
aws ec2 terminate-instances --instance-ids i-xxxxx --region eu-west-3
```

💡 **Recommandation**: Terminez l'instance une fois le téléchargement terminé.

## Timeline typique

### MusicBrainz uniquement
```
0:00  - Instance démarrée
0:02  - Installation des dépendances
0:05  - Début téléchargement artist.tar.xz
0:10  - Upload artist.tar.xz vers S3
0:15  - Téléchargement des autres fichiers
0:25  - Tous les fichiers uploadés
0:25  - ✅ TERMINÉ - Vous pouvez terminer l'instance
```

### Avec ListenBrainz
```
0:00  - Instance démarrée
0:02  - Installation des dépendances
0:05  - Téléchargement MusicBrainz (20 min)
0:25  - MusicBrainz terminé
0:30  - Début ListenBrainz (~100 GB)
3:30  - Upload ListenBrainz vers S3
4:00  - ✅ TERMINÉ - Vous pouvez terminer l'instance
```

## Dépannage

### Erreur "UnauthorizedOperation"
```bash
# Vérifiez vos permissions IAM
aws sts get-caller-identity
```

Votre utilisateur doit avoir:
- `ec2:RunInstances`
- `ec2:DescribeInstances`
- `iam:CreateRole`
- `iam:AttachRolePolicy`

### Instance ne démarre pas
```bash
# Vérifier les quotas EC2
aws service-quotas get-service-quota \
  --service-code ec2 \
  --quota-code L-1216C47A \
  --region eu-west-3
```

### Logs ne s'affichent pas
Les logs peuvent prendre 2-3 minutes à apparaître. Attendez un peu puis réessayez.

### Téléchargement bloqué
```bash
# Vérifier les groupes de sécurité
aws ec2 describe-instances --instance-ids i-xxxxx --region eu-west-3 \
  --query 'Reservations[0].Instances[0].SecurityGroups'
```

L'instance doit avoir accès sortant vers internet (ports 80 et 443).

## Nettoyage après utilisation

1. **Terminer l'instance**
   ```bash
   aws ec2 terminate-instances --instance-ids i-xxxxx
   ```

2. **Supprimer le rôle IAM (optionnel)**
   ```bash
   aws iam remove-role-from-instance-profile \
     --instance-profile-name EC2-S3-Access-Profile \
     --role-name EC2-S3-Access-Role

   aws iam delete-instance-profile \
     --instance-profile-name EC2-S3-Access-Profile

   aws iam detach-role-policy \
     --role-name EC2-S3-Access-Role \
     --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

   aws iam delete-role --role-name EC2-S3-Access-Role
   ```

## Alternatives

### Option 1: DataSync
AWS DataSync peut synchroniser automatiquement, mais plus complexe à configurer.

### Option 2: Lambda
Pour des fichiers plus petits, vous pourriez utiliser Lambda, mais la limite de temps (15 min) est trop courte ici.

### Option 3: Téléchargement local
Si vous avez une bonne connexion et de l'espace disque:
```bash
python scripts/download_musicbrainz.py
python scripts/upload_to_s3.py
```

## Sécurité

### Bonnes pratiques
- ✅ Le rôle IAM donne uniquement accès S3 à l'instance
- ✅ Pas de credentials AWS stockés sur l'instance
- ✅ L'instance utilise un profil IAM
- ✅ Pas d'accès SSH nécessaire

### Améliorations possibles
- Restreindre l'accès S3 à votre bucket spécifique
- Utiliser des Security Groups plus restrictifs
- Activer CloudWatch Logs pour un monitoring avancé

## FAQ

**Q: Puis-je arrêter et reprendre le téléchargement?**
R: Oui, mais il faut modifier le script. Actuellement, il recommence depuis le début.

**Q: Combien coûte une instance qui tourne toute la journée?**
R: t3.medium = 0.05 USD/h × 24h = 1.20 USD/jour. **Terminez-la après usage!**

**Q: Les données sont-elles sécurisées?**
R: Oui, le transfert se fait en HTTPS, et les données sont dans votre bucket S3 privé.

**Q: Puis-je utiliser une instance plus puissante?**
R: Oui, modifiez `DEFAULT_INSTANCE_TYPE` dans le script. Mais t3.medium suffit largement.

**Q: Que faire si le téléchargement échoue?**
R: Relancez le script. Les fichiers déjà uploadés sur S3 ne seront pas re-téléchargés.

## Prochaines étapes

Une fois les données sur S3:
1. Configurer EMR pour le traitement Spark
2. Ou extraire les données localement pour analyse
3. Ou configurer Airflow pour automatiser le pipeline

Voir [README.md](README.md) pour la suite du workflow.