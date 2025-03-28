# Importation des librairies
import requests
from requests.exceptions import HTTPError, ProxyError, Timeout, RequestException
from pathlib import Path
from dotenv import load_dotenv
import os
import concurrent.futures

# Charger les variables d'environnement
load_dotenv()

# Récupérer les informations du proxy depuis le fichier .env
proxy = os.getenv("PROXY_URL")

# Configuration du proxy
proxies = {
    "http": proxy,
    "https": proxy
} if proxy else None

def download_file(link, path_output):
    """
    Télécharge le fichier à partir du lien donné vers le répertoire de sortie.
    
    Parameters:
    link (str): URL du fichier à télécharger.
    path_output (Path): Chemin du répertoire de sortie.
    """
    try:
        # Headers pour éviter les blocages
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = requests.get(link, headers=headers, proxies=proxies, stream=True)
        response.raise_for_status()
        
        # Détermination du nom de fichier
        file_name = Path(link).name
        file_dest = path_output / file_name
        
        if not file_dest.exists():
            print(f"Début du téléchargement: {file_name}...")
            with open(file_dest, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
            print(f"Téléchargement terminé: {file_name}")
        else:
            print(f"Fichier déjà existant: {file_name}")
    
    except (HTTPError, ProxyError, Timeout, RequestException) as e:
        print(f"Erreur lors du téléchargement de {link} : {e}")

# Liste des URLs spécifiques à télécharger
specific_files = [
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_2025-02-03.7z",
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_RGAF09UTM20_GLP_2025-02-03/ADMIN-EXPRESS_3-2__SHP_RGAF09UTM20_GLP_2025-02-03.7z",
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_RGAF09UTM20_MTQ_2025-02-03/ADMIN-EXPRESS_3-2__SHP_RGAF09UTM20_MTQ_2025-02-03.7z",
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_UTM22RGFG95_GUF_2025-02-03/ADMIN-EXPRESS_3-2__SHP_UTM22RGFG95_GUF_2025-02-03.7z",
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_RGR92UTM40S_REU_2025-02-03/ADMIN-EXPRESS_3-2__SHP_RGR92UTM40S_REU_2025-02-03.7z",
    "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS/ADMIN-EXPRESS_3-2__SHP_RGM04UTM38S_MYT_2025-02-03/ADMIN-EXPRESS_3-2__SHP_RGM04UTM38S_MYT_2025-02-03.7z"
]

# Pattern qui correspond à toutes ces URLs (pour vérification)
pattern = r"ADMIN-EXPRESS_3-2__SHP_(LAMB93_FXX|RGAF09UTM20_GLP|RGAF09UTM20_MTQ|UTM22RGFG95_GUF|RGR92UTM40S_REU|RGM04UTM38S_MYT)_2025-02-03\.7z$"

# Vérification que toutes les URLs correspondent au pattern
import re
for url in specific_files:
    if not re.search(pattern, url):
        print(f"ATTENTION: L'URL {url} ne correspond pas au pattern")

# Chemin de sortie
path_output = Path("./src/raw_data/ign/")
path_output.mkdir(parents=True, exist_ok=True)

# Téléchargement des fichiers spécifiques
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:  # Réduit à 3 workers pour éviter la surcharge
    executor.map(lambda url: download_file(url, path_output), specific_files)

print("Téléchargements terminés.")