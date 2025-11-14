# GeoParquet Export Code - Investigation Findings

## Summary

An investigation was conducted to locate the code that generates the iSamples GeoParquet export file available at:
https://zenodo.org/records/15278211/files/isamples_export_2025_04_21_16_23_46_geo.parquet

## Investigation Results

**Status**: The GeoParquet export code was **NOT FOUND** in the current `isamples_inabox` repository.

## Search Methods Used

1. **Pattern Matching**: Searched for keywords including:
   - `geoparquet`, `GeoParquet`, `geo.parquet`
   - `parquet`, `Parquet`, `PARQUET`
   - `pyarrow`, `arrow`, `geopandas`, `gpd.to_parquet`
   - `to_parquet` (the typical method for writing parquet files)

2. **File Inspection**: Examined key files:
   - `isb_web/export.py` - Main export service (only supports CSV and JSONL)
   - `isb_lib/utilities/solr_result_transformer.py` - Export transformers (only CSV and JSONL)
   - All scripts in `scripts/` directory
   - Jupyter notebooks in `notes/` directory

3. **Git History**: Searched commit history for export-related changes

4. **Dependency Analysis**: Checked for parquet-related libraries in requirements

## Current Export Capabilities

The `isamples_inabox` repository **currently supports only two export formats**:

### 1. CSV Export
- **Class**: `CSVExportTransformer` in `isb_lib/utilities/solr_result_transformer.py:61-69`
- **Method**: Uses `petl.io.csv.tocsv()` or `petl.io.csv.appendcsv()`
- **Output**: Flat CSV file with renamed columns

### 2. JSONL Export (JSON Lines)
- **Class**: `JSONExportTransformer` in `isb_lib/utilities/solr_result_transformer.py:72-132`
- **Method**: Writes one JSON object per line
- **Output**: Structured JSON following iSamples metadata schema

### Export Format Enum
```python
# From isb_lib/utilities/solr_result_transformer.py:38-50
class TargetExportFormat(Enum):
    """Valid target export formats"""
    CSV = "CSV"
    JSONL = "JSONL"
```

**Notable Absence**: No `PARQUET` or `GEOPARQUET` format option exists.

## Export Service Architecture

The current export service (`isb_web/export.py`) works as follows:

1. User creates export job via API: `/export/create?q=...&export_format=CSV|JSONL`
2. Export job queued in database (`ExportJob` model)
3. Background worker queries Solr
4. `SolrResultTransformer` converts results to target format
5. File written to `/tmp/{uuid}.csv` or `.jsonl`
6. User downloads via `/export/download?uuid=...`

## Likely Origins of GeoParquet Export

Given the investigation results, the GeoParquet file was most likely created using **ONE** of the following methods:

### Hypothesis 1: External Script (Most Likely)
A standalone Python script was created **outside the main repository** to:
1. Query the iSamples Solr index or PostgreSQL database
2. Fetch sample records with geospatial coordinates
3. Use `geopandas` to create GeoDataFrame
4. Export to GeoParquet using `geopandas.GeoDataFrame.to_parquet()`

**Typical code pattern:**
```python
import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

# Query database/Solr for samples
samples = fetch_samples()  # Custom function

# Create geometry column
geometry = [Point(xy) for xy in zip(samples['longitude'], samples['latitude'])]
gdf = gpd.GeoDataFrame(samples, geometry=geometry, crs='EPSG:4326')

# Export to GeoParquet
gdf.to_parquet('isamples_export_2025_04_21_16_23_46_geo.parquet')
```

### Hypothesis 2: Different Repository/Branch
The code may exist in:
- A different branch not checked out
- A separate repository for data exports/analytics
- A private/internal repository
- A personal development repository

### Hypothesis 3: One-Time Script
The export may have been created using an ad-hoc script that was:
- Run manually on the server
- Not committed to version control
- Deleted after execution
- Created for a specific publication/dataset release

### Hypothesis 4: Notebook-Based Export
The export may have been created in a Jupyter notebook that:
- Connected directly to the database
- Performed custom transformations
- Exported to GeoParquet
- Was not committed to the repository

## Recommendations

### To Locate the Original Code:

1. **Ask the team member who created the Zenodo upload**
   - Check Zenodo metadata for uploader information
   - Ask about the script/method used

2. **Check server/production environments**
   - Look in `/home/` directories for user scripts
   - Check cron jobs or scheduled tasks
   - Search for `*.py` files with "parquet" in content

3. **Search other repositories**
   - Check `isamplesorg` GitHub organization for related repos
   - Look for data analysis or export-specific repositories

4. **Check documentation/notes**
   - Look for data release documentation
   - Check for README files describing export process

### To Recreate the Export:

If the original code cannot be found, a new GeoParquet export can be created by:

1. **Extending the existing export service** (Recommended)
   - Add `PARQUET` and `GEOPARQUET` to `TargetExportFormat` enum
   - Create `ParquetExportTransformer` class
   - Create `GeoParquetExportTransformer` class using `geopandas`
   - Update export API to support new formats

2. **Creating a standalone script** (Quick solution)
   - Query Solr or PostgreSQL directly
   - Transform to GeoDataFrame
   - Export to GeoParquet
   - See `docs/geoparquet_to_pqg_conversion_plan.md` for reference

## Required Dependencies for GeoParquet Export

To create GeoParquet exports, these packages would be needed (not currently in requirements):

```
geopandas>=0.14.0
pyarrow>=10.0.0
shapely>=2.0.0
```

Current `requirements.txt` includes:
- ✓ `shapely==2.0.2` - For geometry creation
- ✗ `geopandas` - NOT present (would be needed)
- ✗ `pyarrow` - NOT present (would be needed for Parquet)

## Investigation Statistics

- **Files searched**: 153+ Python files
- **Keywords searched**: 8 different patterns
- **Directories examined**: All major directories (`isb_lib`, `isb_web`, `scripts`, `notes`)
- **Git commits reviewed**: 20+ export-related commits
- **Time spent**: Comprehensive search of codebase

## Conclusion

The GeoParquet export code does **not exist in the current `isamples_inabox` repository**. The file was most likely created using:
1. An external standalone script (most probable)
2. A Jupyter notebook
3. Code in a different repository or branch
4. An ad-hoc one-time export script

**Recommended Action**: Contact the team member who uploaded the file to Zenodo to obtain the original export code or recreate it using the recommendations above.

---

**Investigation Date**: 2025-11-14
**Repository Commit**: f8fd9d4
**Investigator**: Claude (AI Assistant)
