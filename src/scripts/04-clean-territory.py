import geopandas as gpd
from pathlib import Path
import logging
from shapely.validation import make_valid

# Configuration
INPUT_DIR = Path("./src/processed_data/temp")
OUTPUT_DIR = Path("./src/processed_data/gen")
OUTPUT_DIR.mkdir(exist_ok=True)
SURFACE_THRESHOLD = 100000  # 100 000 m²

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_file(input_path: Path) -> gpd.GeoDataFrame:
    """Charge, valide et traite un fichier Parquet."""
    logger.info(f"Traitement de {input_path.name}...")
    
    gdf = gpd.read_parquet(input_path)
    
    # Validation
    required_cols = {'com_insee', 'com_nom', 'geometry'}
    if missing := required_cols - set(gdf.columns):
        raise ValueError(f"Colonnes manquantes : {missing}")
    if gdf.crs is None:
        raise ValueError("CRS non défini")
    
    # Traitement
    gdf = gdf[['com_insee', 'com_nom', 'geometry']].copy()
    gdf['surface'] = gdf.geometry.area
    
    counts = gdf.groupby(['com_insee', 'com_nom']).size().reset_index(name='count')
    gdf = gdf.merge(counts, on=['com_insee', 'com_nom'])
    
    gdf = gdf[~((gdf['count'] > 1) & (gdf['surface'] < SURFACE_THRESHOLD))]
    
    dissolved = gdf.dissolve(by=['com_insee', 'com_nom'], aggfunc='first').reset_index()
    dissolved = dissolved[['com_insee', 'com_nom', 'geometry']]
    dissolved.geometry = dissolved.geometry.apply(lambda geom: make_valid(geom) if not geom.is_valid else geom)
    
    return dissolved

def get_output_filename(input_filename: str) -> str:
    """Génère le nom de fichier de sortie avec '-gen'."""
    if not input_filename.endswith("-temp.parquet"):
        raise ValueError(f"Format de fichier invalide : {input_filename}")
    return input_filename.replace("-temp.parquet", "-gen.parquet")

def main():
    """Parcourt et traite tous les fichiers .parquet."""
    for parquet_file in INPUT_DIR.glob("*.parquet"):
        try:
            output_filename = get_output_filename(parquet_file.name)
            output_path = OUTPUT_DIR / output_filename
            
            if output_path.exists():
                logger.warning(f"Écrasement du fichier existant : {output_filename}")
                
            processed_gdf = process_file(parquet_file)
            processed_gdf.to_parquet(output_path, compression="gzip", index=False)
            logger.info(f"Exporté : {output_filename}")
            
        except Exception as e:
            logger.error(f"Erreur avec {parquet_file.name} : {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()