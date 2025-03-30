import geopandas as gpd
import pandas as pd
from pathlib import Path
import duckdb
from shapely.ops import unary_union
import os

# Configuration
INPUT_DIRS = [
    "O:/Document/carto-engine/ngeofr-maps/src/processed_data/standard",
    "O:/Document/carto-engine/ngeofr-maps/src/processed_data/gen"
]
DB_PATH = "O:/Document/carto-engine/ngeofr/public/ngeo2025.duckdb"
OUTPUT_DIR = "./public/"
COG_YEAR = "2025"

# Get all parquet files from both directories
GEOMETRIES_PATHS = []
for input_dir in INPUT_DIRS:
    for file in os.listdir(input_dir):
        if file.endswith(".parquet"):
            is_gen = "gen" in input_dir  # True if from gen folder
            GEOMETRIES_PATHS.append((input_dir, file, is_gen))

MESHES = [
    {"id_col": "dep_insee", "name_col": "dep_nom", "mesh_type": "dep", "query": "SELECT com_insee, dep_insee, dep_nom FROM ngeofr"},
    {"id_col": "reg_insee", "name_col": "reg_nom", "mesh_type": "reg", "query": "SELECT com_insee, reg_insee, reg_nom FROM ngeofr"},
    {"id_col": "arr_insee", "name_col": "arr_nom", "mesh_type": "arr", "query": "SELECT com_insee, arr_insee, arr_nom FROM ngeofr"},
    {"id_col": "epci_siren", "name_col": "epci_nom", "mesh_type": "epci", "query": "SELECT com_insee, epci_siren, epci_nom FROM ngeofr"},
    {"id_col": "ept_siren", "name_col": "ept_nom", "mesh_type": "ept", "query": "SELECT com_insee, ept_siren, ept_nom FROM ngeofr"},
    {"id_col": "com_insee", "name_col": "com_nom", "mesh_type": "com", "query": "SELECT com_insee, com_nom FROM ngeofr"}
]

def load_data_from_duckdb(db_path, query):
    conn = duckdb.connect(db_path)
    df = conn.execute(query).df()
    conn.close()
    return df

def extract_territory_prefix(geom_file):
    """Extract territory prefix from filename"""
    return geom_file.split('-')[1]

def create_output_directory(prefix, style):
    """Create output directory"""
    directory = f"{prefix}"
    if style == "compact":
        directory += "-compact"
    output_path = Path(OUTPUT_DIR) / directory
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path

def is_valid_geometry(gdf):
    """Check if geometries are valid and not empty"""
    if gdf.empty:
        return False
    if 'geometry' not in gdf.columns:
        return False
    if gdf.geometry.is_empty.any() or gdf.geometry.isna().any():
        return False
    return True

def process_mesh(geometries_df, data_df, id_col, name_col, output_dir, year, style, mesh_type, territory, is_gen):
    print(f"\nProcessing {mesh_type} mesh ({'generalized' if is_gen else 'standard'}):")

    # Skip specific mesh types for certain territories
    if mesh_type in ["epciept", "ept", "epci"] and territory not in ['fra', 'frdrom']:
        print(f"Skipping {mesh_type} mesh for territory {territory} (not applicable)")
        return

    # Check for empty or invalid data
    if not is_valid_geometry(geometries_df) or data_df.empty:
        print(f"Empty or invalid data for {mesh_type}-{territory}, skipping")
        return

    # Generate filenames with -gen suffix if needed
    gen_suffix = "-gen" if is_gen else ""
    style_prefix = "-compact" if style == "compact" else ""
    
    filenames = [
        f"{mesh_type}-{territory}{style_prefix}-{year}-surface{gen_suffix}.parquet",
        f"{mesh_type}-{territory}{style_prefix}-{year}-centroid{gen_suffix}.parquet",
        f"{mesh_type}-{territory}{style_prefix}-{year}-boundary{gen_suffix}.parquet"
    ]

    # Check if files already exist
    if all((Path(output_dir) / f).exists() for f in filenames):
        print(f"Files for {mesh_type}-{territory} exist, skipping")
        return

    # Process data
    if mesh_type == "com":
        merged_gdf = geometries_df.copy()
        if id_col not in merged_gdf.columns:
            merged_gdf[id_col] = merged_gdf.index
        if name_col not in merged_gdf.columns:
            merged_gdf = merged_gdf.merge(data_df, on=id_col, how='left')
    else:
        merged_gdf = geometries_df.merge(data_df[['com_insee', id_col, name_col]], on='com_insee', how='left')
    
    if not is_valid_geometry(merged_gdf):
        print(f"Invalid geometries after merge for {mesh_type}-{territory}, skipping")
        return

    if mesh_type != "com":
        merged_gdf = merged_gdf.dissolve(by=id_col, aggfunc='first').reset_index()
        if not is_valid_geometry(merged_gdf):
            print(f"Invalid geometries after dissolve for {mesh_type}-{territory}, skipping")
            return
    
    # Prepare output data
    cols_to_keep = [col for col in [id_col, name_col, 'geometry'] if col in merged_gdf.columns]
    dissolved_gdf = merged_gdf[cols_to_keep]

    # Export files
    def export(gdf, filename):
        if not is_valid_geometry(gdf):
            print(f"Skipping {filename} (empty or invalid geometries)")
            return
        output_path = Path(output_dir) / filename
        gdf.to_parquet(output_path)
        print(f"Exported: {filename}")

    # Export surface
    export(dissolved_gdf, filenames[0])

    # Export centroid
    if is_valid_geometry(dissolved_gdf):
        point_gdf = dissolved_gdf.copy()
        point_gdf.geometry = dissolved_gdf.geometry.centroid
        point_gdf.loc[~point_gdf.geometry.within(dissolved_gdf.geometry), 'geometry'] = dissolved_gdf.geometry.apply(lambda geom: geom.representative_point())
        export(point_gdf, filenames[1])
    else:
        print(f"Skipping centroid export for {mesh_type}-{territory} (invalid base geometries)")

    # Export boundary
    if is_valid_geometry(dissolved_gdf):
        boundary_gdf = dissolved_gdf.copy()
        boundary_gdf.geometry = dissolved_gdf.geometry.boundary
        export(boundary_gdf, filenames[2])
    else:
        print(f"Skipping boundary export for {mesh_type}-{territory} (invalid base geometries)")

def main():
    # Add epciept query
    epci_ept_query = open("O://Document/carto-engine/ngeofr/src/shared/sql/query_epci_ept.sql").read()
    epci_ept_data = load_data_from_duckdb(DB_PATH, epci_ept_query)
    MESHES.append({"id_col": "epci_siren", "name_col": "epci_nom", "mesh_type": "epciept", "data_df": epci_ept_data})
    
    # Process all files
    for input_dir, filename, is_gen in GEOMETRIES_PATHS:
        style = "compact" if "compact" in filename else "natural"
        territory = extract_territory_prefix(filename)
        
        # Create output directory
        output_dir = create_output_directory(territory, style)
        
        # Load geometry file
        geom_path = Path(input_dir) / filename
        try:
            geometries_df = gpd.read_file(geom_path)
            geometries_df = geometries_df[[col for col in geometries_df.columns if not col.startswith("geometry_bbox")]]
        except Exception as e:
            print(f"Error loading {filename}: {str(e)}")
            continue
        
        # Process each mesh type
        for mesh_config in MESHES:
            try:
                data_df = load_data_from_duckdb(DB_PATH, mesh_config['query']) if 'query' in mesh_config else mesh_config['data_df']
                process_mesh(
                    geometries_df, data_df, 
                    mesh_config['id_col'], mesh_config['name_col'], 
                    output_dir, COG_YEAR, style, 
                    mesh_config['mesh_type'], territory, is_gen
                )
            except Exception as e:
                print(f"Error processing {mesh_config['mesh_type']} for {filename}: {str(e)}")

if __name__ == "__main__":
    main()