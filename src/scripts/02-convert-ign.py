import geopandas as gpd
from pathlib import Path
import re

# Configuration
COG_YEAR = "2025"
BASE_PATHS = [Path("./src/raw_data/ign/")]
OUTPUT_DIR = Path("./src/processed_data/")
OUTPUT_DIR.mkdir(exist_ok=True)

CRS_CONFIG = {
    "FRA": 2154, "GLP": 5490, "MTQ": 5490,
    "GUF": 2972, "REU": 2975, "MYT": 4471
}

def find_shapefile(territory: str) -> Path:
    """Trouve le fichier COMMUNE.shp avec debug des chemins."""
    patterns = {
        "FRA": "**/ADMIN-EXPRESS_3-2__SHP_LAMB93_FXX_*/**/COMMUNE.shp",
        "GLP": "**/ADMIN-EXPRESS_3-2__SHP_RGAF09UTM20_GLP_*/**/COMMUNE.shp",
        "MTQ": "**/ADMIN-EXPRESS_3-2__SHP_RGAF09UTM20_MTQ_*/**/COMMUNE.shp",
        "GUF": "**/ADMIN-EXPRESS_3-2__SHP_UTM22RGFG95_GUF_*/**/COMMUNE.shp",
        "REU": "**/ADMIN-EXPRESS_3-2__SHP_RGR92UTM40S_REU_*/**/COMMUNE.shp",
        "MYT": "**/ADMIN-EXPRESS_3-2__SHP_RGM04UTM38S_MYT_*/**/COMMUNE.shp",
    }
    for base_path in BASE_PATHS:
        files = list(base_path.glob(patterns[territory]))
        if files:
            return max(files, key=lambda f: f.stat().st_mtime)
    raise FileNotFoundError(f"Aucun fichier COMMUNE.shp trouvé pour {territory}")

def process_territory(territory: str) -> gpd.GeoDataFrame:
    """Charge et valide les données."""
    shp_path = find_shapefile(territory)
    print(f"Chargement de {shp_path}...")
    
    # Lecture avec reset_index
    gdf = gpd.read_file(shp_path).reset_index(drop=True)
    
    # Nettoyage
    gdf.columns = [re.sub(r'\W+', '_', col).lower() for col in gdf.columns]
    gdf = gdf.rename(columns={'insee_com': 'com_insee', 'nom': 'com_nom'})
    gdf = gdf[['com_insee', 'com_nom', 'geometry']]
    
    # Validation
    gdf['com_insee'] = gdf['com_insee'].astype(str).str.zfill(5)
    if any(gdf['com_insee'].str.len() != 5):
        raise ValueError("Code INSEE invalide")
    
    # Correction géométries + reset_index
    if not gdf.geometry.is_valid.all():
        gdf.geometry = gdf.geometry.buffer(0)
    
    # Reprojection et tri final
    return (
        gdf.to_crs(epsg=CRS_CONFIG[territory])
           .sort_values('com_insee')
           .reset_index(drop=True)
    )

def export_geoparquet(gdf: gpd.GeoDataFrame, territory: str) -> None:
    """Exporte en GeoParquet avec gestion de l'index."""
    filename = f"com-{territory.lower()}-{COG_YEAR}.parquet"
    
    # Export sans index
    gdf.to_parquet(
        OUTPUT_DIR / filename,
        compression="gzip",
        schema_version="1.0.0",
        index=False  # ← Désactive l'export de l'index
    )
    print(f"Export réussi : {filename}")

def main():
    for territory in CRS_CONFIG.keys():
        print(f"\n--- Traitement de {territory} ---")
        try:
            gdf = process_territory(territory)
            print(f"Communes: {len(gdf)} | CRS: EPSG:{CRS_CONFIG[territory]}")
            export_geoparquet(gdf, territory)
        except FileNotFoundError:
            print(f"⚠️ Données manquantes pour {territory}. Vérifiez les fichiers IGN.")
        except Exception as e:
            print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    main()