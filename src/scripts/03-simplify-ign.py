import geopandas as gpd
import os
from pathlib import Path
import re


# Nouveau test
# https://grass.osgeo.org/grass-stable/manuals/v.generalize.html
# => reumann tolerance 100 iteration 2
# => douglas tolerance 100 
# => distance weighting anticipation 11 angle 0.5
# => chaiken 50

# Pour FRA ajouter
# reumann tolerance 100 iteration 2
# douglas tolerance 100
# douglas tolerance 5

# sliding averaging anticipation 11


# Config Windows
# GRASS_BIN = r"C:\Program Files\QGIS 3.30.3\bin\grass82.bat"  # À adapter!



# Projections
CRS_CONFIG = {
    "FRA": 2154,  # France métropolitaine
    "GLP": 5490,  # Guadeloupe
    "MTQ": 5490,  # Martinique
    "GUF": 2972,  # Guyane française
    "REU": 2975,  # Réunion
    "MYT": 4471,  # Mayotte
}

def extract_territory_code(filename):
    """
    Extract territory code from filename, excluding 'frdrom' and 'frdrom-compact'.
    
    Examples:
    - 'com-fra-2025.parquet' -> 'FRA'
    - 'com-guf-2025.parquet' -> 'GUF'
    """
    # Exclude 'frdrom' and 'frdrom-compact'
    if 'frdrom' in filename.lower():
        return None
    
    # Try to extract territory code
    match = re.search(r'-((?:fra|glp|mtq|guf|reu|myt)(?:-compact)?)-', filename.lower())
    if match:
        territory_code = match.group(1).split('-')[0].upper()
        return territory_code
    
    return None

def simplify_geometries(input_path, simplification_distance=200):
    """
    Simplify geometries while preserving topology with a single distance.
    
    Parameters:
    - input_path: Path to the input Parquet file
    - simplification_distance: Simplification distance in meters
    """
    # Read the original geometries
    gdf = gpd.read_file(input_path)
    
    # Get the input directory and filename
    input_dir = Path(input_path).parent
    filename = Path(input_path).stem
    file_extension = Path(input_path).suffix
    
    # Extract territory code and get appropriate CRS
    territory_code = extract_territory_code(filename)
    
    # Skip if no valid territory code
    if territory_code is None:
        print(f"Skipping {filename} - No valid territory code")
        return
    
    target_crs = CRS_CONFIG.get(territory_code, 2154)  # Default to France if not found
    
    # Ensure the CRS is projected 
    if gdf.crs is None or gdf.crs.is_geographic:
        # Reproject to the territory-specific CRS
        gdf = gdf.to_crs(epsg=target_crs)
    
    # Create a copy of the GeoDataFrame
    simplified_gdf = gdf.copy()
    
    # Simplify geometries while preserving topology
    simplified_gdf.geometry = simplified_gdf.geometry.simplify(
        tolerance=simplification_distance, 
        preserve_topology=True
    )
    
    # Create the output filename
    output_filename = f"{filename}-simplified-{simplification_distance}m{file_extension}"
    output_path = input_dir / output_filename
    
    # Export the simplified geometries
    simplified_gdf.to_file(output_path, driver='Parquet')
    print(f"Exported simplified geometries: {output_path}")
    print(f"Used CRS: EPSG:{target_crs}")

def main():
    # Input directory where raw Parquet files are stored
    input_dir = "./src/processed_data/"
    
    # Simplification distance (in meters)
    simplification_distance = 200
    
    # Process all Parquet files in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".parquet"):
            input_path = os.path.join(input_dir, filename)
            
            try:
                simplify_geometries(input_path, simplification_distance)
            except Exception as e:
                print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    main()