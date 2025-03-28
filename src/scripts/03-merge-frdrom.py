import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path

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

def main(input_dir="./src/processed_data/", output_dir="./src/processed_data/"):
    """
    Fonction principale qui charge, transforme et exporte les données géographiques.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print("Chargement des territoires...")
    geometries = load_territories(input_dir)
    
    if not geometries:
        print("Erreur : Aucun territoire chargé. Vérifiez le répertoire d'entrée.")
        return
    
    print("Traitement en position naturelle...")
    fr_drom_natural = transform_natural(geometries)
    export_geometries(fr_drom_natural, COG_YEAR, "natural", output_dir)
    
    print("Traitement en position compacte...")
    fr_drom_compact = transform_compact(geometries)
    export_geometries(fr_drom_compact, COG_YEAR, "compact", output_dir)

def load_territories(input_dir):
    """
    Charge les fichiers géographiques pour chaque territoire spécifié.
    """
    geometries = {}
    for territory in CRS_CONFIG.keys():
        filename = f"com-{territory.lower()}-{COG_YEAR}.parquet"
        file_path = Path(input_dir) / filename
        
        if not file_path.exists():
            print(f"Avertissement : Fichier introuvable pour {territory}")
            continue
        
        gdf = gpd.read_file(file_path)
        geometries[territory] = gdf
    
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
        pd.concat([fra_gdf] + droms_transformed, ignore_index=True),  # Seul changement ici
        crs=f"EPSG:{TARGET_PROJ_COMPACT}"
    ).reset_index(drop=True)  # Et ici

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

def export_geometries(gdf, proj_year, style, output_dir):
    """
    Exporte les géométries transformées avec une nomenclature optimisée.
    Format : frdrom-[style]-[année].parquet
    """
    # Version naturelle : frdrom-2025.parquet
    # Version compacte : frdrom-compact-2025.parquet
    filename = f"com-frdrom-{style}-{proj_year}.parquet" if style == "compact" else f"frdrom-{proj_year}.parquet"
    output_path = Path(output_dir) / filename
    
    # Export avec vérifications
    gdf.reset_index(drop=True).to_parquet(
        output_path,
        compression='gzip',
        index=False,
        schema_version="1.0.0"  # Conservation des métadonnées géospatiales
    )
    
    file_size = output_path.stat().st_size / (1024 * 1024)
    print(f"Exporté : {filename} ({file_size:.2f} MB)")

if __name__ == "__main__":
    main()