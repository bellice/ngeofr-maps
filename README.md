# ngeofr-maps

## Description

**ngeofr-maps** propose une chaîne de traitement des données géographiques communales françaises (hexagone + DROM) issues de l'IGN. Ce projet produit des données géométriques optimisées pour :
- la cartographie web interactive
- l'analyse spatiale
- les systèmes d'information géographique

## Statut des millésimes du COG

![COG 2025](https://img.shields.io/badge/COG%202025-✅%20Disponible-brightgreen)

## Table des matières
- [Installation](#installation)
- [Structure du projet](#structure-du-projet)
- [Projections géographiques](#projections-géographiques)
- [Format des données](#format-des-données)
- [FAQ](#faq)
- [Méthodologie](#méthodologie)
- [Sources utilisées](#sources-utilisées)
- [Licence](#licence)


## Installation
Pour installer le projet **ngeofr-maps**, clonez le dépôt :

```bash
git clone https://github.com/votre-repo/ngeofr-maps.git
```


## Structure du projet

```bash
ngeofr-maps/
├── src/
│   ├── raw_data/              # Données brutes téléchargées (.7z)
│   ├── processed_data/        # Données transformées
│   │   ├── standard/          # Fichiers haute précision
│   │   ├── gen/               # Fichiers généralisés
│   │   └── temp/              # Fichiers temporaires
│   └── scripts/               # Scripts de traitement
└── public/                    # Sortie finale
```

## Projections géographiques

### Par territoire

| Territoire          | Projection             | EPSG |
|---------------------|------------------------|------|
| France métropolitaine | RGF93 / Lambert-93   | 2154 |
| Guadeloupe          | RGAF09 / UTM zone 20N  | 5490 |
| Martinique          | RGAF09 / UTM zone 20N  | 5490 |
| Guyane              | RGFG95 / UTM zone 22N  | 2972 |
| Réunion             | RGR92 / UTM zone 40S   | 2975 |
| Mayotte             | RGM04 / UTM zone 38S   | 4471 |

### Par ensemble France et DROM

| Type       | Projection              | EPSG |
|------------|-------------------------|------|
| Naturelle  | WGS 84 / World Mercator | 3395 |
| Compacte   | RGF93 / Lambert-93      | 2154 |

## Format des données

### Schéma des fichiers Parquet

| Colonne     | Type        | Description                  |
|-------------|-------------|------------------------------|
| com_insee   | VARCHAR(5)  | Code INSEE (5 chiffres)      |
| com_nom     | VARCHAR     | Nom officiel de la commune   |
| geometry    | GEOMETRY    | Polygone/Point/Linestring    |


## FAQ

🚧 En cours de rédaction...

## Méthodologie

### Collecte des données
Les données communales sont téléchargées automatiquement depuis le portail IGN via le script `00-extract-ign.py`. Ce dernier récupère les archives .7z pour chaque territoire (métropole et DROM) en utilisant l'API publique.


### Traitement des données
1. **Préparation** (`01-unzip-ign.py`)  
   Extraction des shapefiles depuis les archives compressées, avec conservation de la structure originale des répertoires.

2. **Conversion standard** (`02-convert-ign.py`)  
   Transformation des Shapefiles en GeoParquet avec :
   - Reprojection dans le CRS approprié pour chaque territoire
   - Normalisation des noms de colonnes (`com_insee`, `com_nom`)
   - Validation des géométries (correction par `buffer(0)`)

3. **Généralisation** (`04-clean-territory.py`)  
   Production de versions simplifiées pour la cartographie web :
   - Suppression des polygones de moins de 100 000 m²
   - Fusion des multipolygones par commune
   - Export dans le dossier `gen/` avec suffixe `-gen`


4. **Assemblage territorial** (`05-merge-frdrom.py`)  
   Combinaison des territoires :
   - **Naturelle** : position géographique réelle
   - **Compacte** : DROM repositionnés près de l'hexagone


5. **Production finale** (`06-generate-ngeo.py`)  
    Génération de jeux de données pour les niveaux administratifs français :

    | Code    | Niveau complet                                      | 
    |---------|----------------------------------------------------|
    | `reg`   | Régions                                             | 
    | `dep`   | Départements                                        | 
    | `arr`   | Arrondissements                                     | 
    | `epci`  | Établissements Publics de Coopération Intercommunale | 
    | `epciept`| EPCI + Établissements Publics Territoriaux         | 
    | `ept`   | Établissements Publics Territoriaux                 | 
    | `com`   | Communes                                            | 

    Chaque niveau administratif est exporté en trois versions géométriques :
    1. **Surface** : Polygones complets (`*-surface.parquet`)
    2. **Centroïde** : Points représentatifs (`*-centroid.parquet`)
    3. **Frontière** : Contours (`*-boundary.parquet`)

### Version des données
Deux niveaux de précision sont produits :

| Version       | Dossier          | Usage typique                  | Exemple de fichier              |
|---------------|------------------|--------------------------------|----------------------------------|
| **Standard**  | `standard/` | SIG, Analyse précise          | `com-fra-2025.parquet`          |
| **Généralisé**| `gen/`     | Cartographie web, Visualisation | `com-fra-2025-gen.parquet`      |


## Sources utilisées

[![Source IGN](https://img.shields.io/badge/Source-IGN-blue)](https://www.ign.fr/)
[![Source: Natural Earth](https://img.shields.io/badge/Source-Natural_Earth-blue)](https://www.naturalearthdata.com/)

## Licence
Ce projet est sous licence MIT - voir le fichier [LICENSE](./LICENSE) pour plus de détails