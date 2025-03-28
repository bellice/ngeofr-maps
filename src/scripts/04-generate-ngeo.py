import geopandas as gpd
import pandas as pd
from pathlib import Path
import duckdb
from shapely.ops import unary_union
import os

# User Configuration
INPUT_DIR = "./src/processed_data/"
DB_PATH = "O:/Document/carto-engine/ngeofr/public/ngeo2025.duckdb"
OUTPUT_DIR = "./public/"
COG_YEAR = "2025"

# Dynamically get all .parquet files in the input directory
GEOMETRIES_PATHS = [f for f in os.listdir(INPUT_DIR) if f.endswith(".parquet")]

MESHES = [
    {"id_col": "dep_insee", "name_col": "dep_nom", "mesh_type": "dep", "query": "SELECT com_insee, dep_insee, dep_nom FROM ngeofr"},
    {"id_col": "reg_insee", "name_col": "reg_nom", "mesh_type": "reg", "query": "SELECT com_insee, reg_insee, reg_nom FROM ngeofr"},
    {"id_col": "arr_insee", "name_col": "arr_nom", "mesh_type": "arr", "query": "SELECT com_insee, arr_insee, arr_nom FROM ngeofr"},
    {"id_col": "epci_siren", "name_col": "epci_nom", "mesh_type": "epci", "query": "SELECT com_insee, epci_siren, epci_nom FROM ngeofr"},
    {"id_col": "ept_siren", "name_col": "ept_nom", "mesh_type": "ept", "query": "SELECT com_insee, ept_siren, ept_nom FROM ngeofr"}
]

def load_data_from_duckdb(db_path, query):
    conn = duckdb.connect(db_path)
    df = conn.execute(query).df()
    conn.close()
    return df

def extract_territory_prefix(geom_file):
    """
    Extract territory prefix from the geometry file name.
    For example: 'com-frdrom-2025.parquet' -> 'frdrom'
    """
    return geom_file.split('-')[1]  # Get the part between 'com-' and '-2025.parquet'

def create_output_directory(prefix, style):
    """
    Create the output directory dynamically based on the territory and style (compact or natural).
    """
    directory = f"{prefix}"
    if style == "compact":
        directory += "-compact"
    output_path = Path(OUTPUT_DIR) / directory  # Just one level for territory and style
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path

def process_mesh(geometries_df, data_df, id_col, name_col, output_dir, year, style, mesh_type, territory):
    print(f"\nProcessing {mesh_type} mesh:")

    # Vérification si le maillage "epciept" ne concerne que les territoires "fra" et "frdrom"
    if mesh_type == "epciept" and territory not in ['fra', 'frdrom']:
        print(f"Skipping {mesh_type} mesh for territory {territory}. This mesh is only for 'fra' and 'frdrom'.")
        return  # Si le territoire n'est pas 'fra' ou 'frdrom', on ignore ce maillage
    
    # Construction du nom du fichier d'export à partir des paramètres
    style_prefix = "compact" if style == "compact" else ""

    # List of filenames to be checked for existence
    filenames = [
        f"{mesh_type}-{territory}{f'-{style_prefix}' if style_prefix else ''}-{year}-surface.parquet",
        f"{mesh_type}-{territory}{f'-{style_prefix}' if style_prefix else ''}-{year}-centroid.parquet",
        f"{mesh_type}-{territory}{f'-{style_prefix}' if style_prefix else ''}-{year}-boundary.parquet"
    ]

    # Vérification si au moins un des fichiers est manquant
    missing_files = [filename for filename in filenames if not (Path(output_dir) / filename).exists()]

    # Si au moins un fichier est manquant, on effectue le traitement
    if missing_files:
        print(f"Missing files: {', '.join(missing_files)}. Proceeding with processing.")
        
        # Fusionner les géométries et les données
        merged_gdf = geometries_df.merge(data_df[['com_insee', id_col, name_col]], on='com_insee', how='left')
        
        # Dissoudre les géométries selon l'ID du maillage
        dissolved_gdf = merged_gdf.dissolve(by=id_col, aggfunc='first').reset_index()
        dissolved_gdf = dissolved_gdf[[id_col, name_col, 'geometry']]

        # Vérifier si la géométrie est valide et s'il y a des données non nulles
        if dissolved_gdf.empty or dissolved_gdf['geometry'].isnull().all():
            print(f"No valid geometries for {mesh_type}-{territory}, skipping processing.")
            return  # Si le DataFrame est vide ou que toutes les géométries sont nulles, on arrête le traitement
        
        def export_gdf(gdf, filename):
            output_path = Path(output_dir) / filename
            
            # Créer le dossier de sortie si nécessaire
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Exporter le fichier
            gdf.to_file(output_path, driver='Parquet')
            print(f"Exported: {filename}")
        
        # Exporter le fichier de surface
        export_filename = filenames[0]  # Le premier fichier est le fichier surface
        export_gdf(dissolved_gdf, export_filename)

        # Exporter le fichier centroid
        point_gdf = dissolved_gdf.copy()
        point_gdf.geometry = dissolved_gdf.geometry.centroid
        point_gdf.loc[~point_gdf.geometry.within(dissolved_gdf.geometry), 'geometry'] = dissolved_gdf.geometry.apply(lambda geom: geom.representative_point())
        export_filename = filenames[1]  # Le deuxième fichier est le fichier centroid
        export_gdf(point_gdf, export_filename)

        # Exporter le fichier boundary
        boundary_gdf = dissolved_gdf.copy()
        boundary_gdf.geometry = dissolved_gdf.geometry.boundary
        export_filename = filenames[2]  # Le troisième fichier est le fichier boundary
        export_gdf(boundary_gdf, export_filename)

    else:
        print(f"All files for {mesh_type}-{territory} already exist, skipping processing.")



def main():
    for geom_file in GEOMETRIES_PATHS:
        style = "compact" if "compact" in geom_file else "natural"
        territory = extract_territory_prefix(geom_file)  # Extract territory prefix dynamically
        
        # Create the appropriate output directory for this territory and style
        output_dir = create_output_directory(territory, style)
        
        geometries_df = gpd.read_file(Path(INPUT_DIR) / geom_file)
        geometries_df = geometries_df[[col for col in geometries_df.columns if not col.startswith("geometry_bbox")]]
        
        epci_ept_query = open("O://Document/carto-engine/ngeofr/src/shared/sql/query_epci_ept.sql").read()
        epci_ept_data = load_data_from_duckdb(DB_PATH, epci_ept_query)
        MESHES.append({"id_col": "epci_siren", "name_col": "epci_nom", "mesh_type": "epciept", "data_df": epci_ept_data})
        
        for mesh_config in MESHES:
            data_df = load_data_from_duckdb(DB_PATH, mesh_config['query']) if 'query' in mesh_config else mesh_config['data_df']
            process_mesh(geometries_df, data_df, mesh_config['id_col'], mesh_config['name_col'], output_dir, COG_YEAR, style, mesh_config['mesh_type'], territory)

if __name__ == "__main__":
    main()
