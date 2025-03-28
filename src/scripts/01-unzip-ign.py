# Importation des librairies
from pathlib import Path
import py7zr  # Pour les archives .7z
import logging
import os

# Configuration du journal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Chemin
path = Path("./src/raw_data/ign/")

# Récupération des fichiers .7z
files = list(path.glob('*.7z'))

# Vérifie si un fichier 7z est déjà extrait
def is_extracted(archive_path, destination):
    try:
        with py7zr.SevenZipFile(archive_path, 'r') as archive:
            members = archive.getnames()
            for member in members:
                member_path = destination / member
                if not member_path.exists():
                    return False
        return True
    except Exception as e:
        logging.error(f"Erreur lors de la vérification de {archive_path.name}: {e}")
        return False

# Filtre des fichiers .7z non extraits
files_filtered = [file for file in files if not is_extracted(file, path)]

# Décompression des fichiers .7z
for file in files_filtered:
    try:
        logging.info(f"Décompression de {file.name}...")
        
        # Création d'un sous-dossier avec le même nom que l'archive (sans extension)
        output_dir = path / file.stem
        output_dir.mkdir(exist_ok=True)
        
        with py7zr.SevenZipFile(file, 'r') as archive:
            archive.extractall(output_dir)
        
        logging.info(f"{file.name} a été extrait dans {output_dir}")
        
    except Exception as e:
        logging.error(f"Erreur lors de la décompression de {file.name}: {e}")

logging.info("Traitement terminé.")