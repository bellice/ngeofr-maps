import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
import os

# Configuration
COG_YEAR = "2025"

# Projections
CRS_CONFIG = {
    "FRA": 2154,  # France métropolitaine
    "GLP": 5490,  # Guadeloupe
    "MTQ": 5490,  # Martinique
    "GUF": 2972,  # Guyane française
    "REU": 2975,  # Réunion
    "MYT": 4471,  # Mayotte
}

# Projections cibles
TARGET_PROJ_NATURAL = 3395  # Projection Mercator (position naturelle)
TARGET_PROJ_COMPACT = 2154  # Projection Lambert 93 (position compacte)

# Configuration du positionnement pour le style compact
SIDE_MAX_BOX = [100000, 100000, 100000, 100000, 100000]  # Taille maximale pour chaque DROM
SPACE_BETWEEN_BOX = 40000  # Espace entre les boîtes

# Coordonnées de destination pour chaque DROM
COORD_DEST = [
    [120000, 6500000],
    [120000, 6500000 - (SIDE_MAX_BOX[0] + SPACE_BETWEEN_BOX)],
    [120000, 6500000 - 2 * (SIDE_MAX_BOX[0] + SPACE_BETWEEN_BOX)],
    [120000, 6500000 - 3 * (SIDE_MAX_BOX[0] + SPACE_BETWEEN_BOX)],
    [120000 + (SIDE_MAX_BOX[0] + SPACE_BETWEEN_BOX), 6500000 - 3 * (SIDE_MAX_BOX[0] + SPACE_BETWEEN_BOX)]
]

def main(input_dir=None, output_dir=None, is_generalized=False):
    """
    Fonction principale qui charge, transforme et exporte les données géographiques.
    
    Args:
        input_dir (str/Path): Répertoire contenant les fichiers d'entrée
        output_dir (str/Path): Répertoire où seront stockés les fichiers de sortie
        is_generalized (bool): Indique si on traite les fichiers généralisés
    """
    # Définition des chemins relatifs
    base_dir = Path(__file__).parent.parent  # Racine du projet (ngeofr-maps)
    default_data_dir = base_dir / "src" / "processed_data"
    
    # Gestion des chemins d'entrée/sortie
    input_dir = Path(input_dir) if input_dir else default_data_dir
    output_dir = Path(output_dir) if output_dir else default_data_dir
    
    # Création des répertoires de sortie
    if is_generalized:
        input_dir = input_dir / "gen"
        output_dir = output_dir / "gen"
    else:
        output_dir = output_dir / "standard"
        
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Suffixe pour les fichiers généralisés
    gen_suffix = "-gen" if is_generalized else ""
    
    # Vérification si les fichiers de sortie existent déjà
    natural_output = get_output_filename("natural", output_dir, gen_suffix)
    compact_output = get_output_filename("compact", output_dir, gen_suffix)
    
    both_exist = natural_output.exists() and compact_output.exists()
    
    if both_exist:
        print(f"Les fichiers existent déjà dans {output_dir}. Traitement ignoré.")
        return
    
    print(f"Chargement des territoires{' généralisés' if is_generalized else ''}...")
    geometries = load_territories(input_dir, gen_suffix)
    
    if not geometries:
        print("Erreur : Aucun territoire chargé. Vérifiez le répertoire d'entrée.")
        return
    
    if not natural_output.exists():
        print("Traitement en position naturelle...")
        fr_drom_natural = transform_natural(geometries)
        export_geometries(fr_drom_natural, COG_YEAR, "natural", output_dir, gen_suffix)
    else:
        print(f"Le fichier {natural_output.name} existe déjà. Traitement ignoré.")
    
    if not compact_output.exists():
        print("Traitement en position compacte...")
        fr_drom_compact = transform_compact(geometries)
        export_geometries(fr_drom_compact, COG_YEAR, "compact", output_dir, gen_suffix)
    else:
        print(f"Le fichier {compact_output.name} existe déjà. Traitement ignoré.")

def get_output_filename(style, output_dir, gen_suffix=""):
    """
    Génère le nom du fichier de sortie en fonction du style et du suffixe.
    """
    if style == "compact":
        filename = f"com-frdrom-compact-{COG_YEAR}{gen_suffix}.parquet"
    else:
        filename = f"com-frdrom-{COG_YEAR}{gen_suffix}.parquet"
    
    return Path(output_dir) / filename

def load_territories(input_dir, gen_suffix=""):
    """
    Charge les fichiers géographiques pour chaque territoire spécifié.
    """
    geometries = {}
    for territory in CRS_CONFIG.keys():
        filename = f"com-{territory.lower()}-{COG_YEAR}{gen_suffix}.parquet"
        file_path = Path(input_dir) / filename
        
        if not file_path.exists():
            print(f"Avertissement : Fichier introuvable pour {territory} ({filename})")
            continue
        
        gdf = gpd.read_file(file_path)
        geometries[territory] = gdf
        print(f"  Chargé: {filename}")
    
    return geometries

def transform_natural(geometries):
    """
    Transforme les géométries en projection Mercator (position naturelle).
    """
    fra_gdf = geometries['FRA']
    drom_territories = ['GLP', 'MTQ', 'GUF', 'REU', 'MYT']
    
    fra_3395 = fra_gdf.to_crs(epsg=TARGET_PROJ_NATURAL)
    
    drom_3395 = []
    for terr in drom_territories:
        if terr in geometries:
            drom_3395.append(geometries[terr].to_crs(epsg=TARGET_PROJ_NATURAL))
    
    fr_drom_natural = gpd.GeoDataFrame(
        pd.concat([fra_3395] + drom_3395, ignore_index=True),
        crs=f"EPSG:{TARGET_PROJ_NATURAL}"
    )
    
    return fr_drom_natural

def transform_compact(geometries):
    """
    Réorganise les DROM dans une position compacte près de la France métropolitaine.
    """
    fra_gdf = geometries['FRA'].set_crs(epsg=TARGET_PROJ_COMPACT, allow_override=True)
    
    drom_territories = ['GLP', 'MTQ', 'GUF', 'REU', 'MYT']
    droms = [geometries[t].set_crs(epsg=TARGET_PROJ_COMPACT, allow_override=True) 
             for t in drom_territories if t in geometries]
    
    droms_transformed = []
    for i, gdf in enumerate(droms):
        scale, translation = calculate_transformation_parameters(
            gdf, 
            SIDE_MAX_BOX[i], 
            COORD_DEST[i]
        )
        
        transformed = transform_geometry(gdf, scale, translation)
        droms_transformed.append(transformed)
    
    return gpd.GeoDataFrame(
        pd.concat([fra_gdf] + droms_transformed, ignore_index=True),
        crs=f"EPSG:{TARGET_PROJ_COMPACT}"
    ).reset_index(drop=True)

def calculate_transformation_parameters(gdf, target_size, target_center):
    """
    Calcule les paramètres de transformation (échelle et translation).
    """
    bounds = gdf.total_bounds
    current_width = bounds[2] - bounds[0]
    current_height = bounds[3] - bounds[1]
    
    scale_factor = target_size / max(current_width, current_height)
    current_center = np.array([(bounds[0] + bounds[2])/2, (bounds[1] + bounds[3])/2])
    translation_vector = np.array(target_center) - (current_center * scale_factor)
    
    return scale_factor, translation_vector

def transform_geometry(gdf, scale_factor, translation):
    """
    Applique une transformation géométrique (mise à l'échelle + translation).
    """
    transformed = gdf.copy()
    transformed.geometry = transformed.geometry.scale(
        xfact=scale_factor,
        yfact=scale_factor,
        origin=(0, 0)
    ).translate(
        xoff=translation[0],
        yoff=translation[1]
    )
    return transformed

def export_geometries(gdf, proj_year, style, output_dir, gen_suffix=""):
    """
    Exporte les géométries transformées.
    """
    if style == "compact":
        filename = f"com-frdrom-compact-{proj_year}{gen_suffix}.parquet"
    else:
        filename = f"com-frdrom-{proj_year}{gen_suffix}.parquet"
    
    output_path = Path(output_dir) / filename
    
    gdf.reset_index(drop=True).to_parquet(
        output_path,
        compression='gzip',
        index=False,
        schema_version="1.0.0"
    )
    
    file_size = output_path.stat().st_size / (1024 * 1024)
    print(f"Exporté : {filename} ({file_size:.2f} MB)")

if __name__ == "__main__":
    # Traitement des fichiers standards
    print("\n=== TRAITEMENT DES FICHIERS STANDARDS ===")
    main(is_generalized=False)
    
    # Traitement des fichiers généralisés
    print("\n=== TRAITEMENT DES FICHIERS GÉNÉRALISÉS ===")
    main(is_generalized=True)