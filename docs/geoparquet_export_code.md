# GeoParquet Export Code - Location and Implementation

## Summary

The code that generates the iSamples GeoParquet export file is located in a **separate repository**:

**Repository**: https://github.com/rdhyee/export_client (also at https://github.com/isamplesorg/export_client)

## Export Client Overview

The `export_client` is a Python CLI tool (`isample`) that retrieves content from the iSamples Export Service and provides GeoParquet conversion capabilities.

### Key Features

- **CLI Tool**: `isample` command-line interface
- **Authentication**: ORCID OAuth with JWT tokens
- **Export Formats**: JSONL, CSV, and **GeoParquet**
- **STAC Support**: Generates STAC (SpatioTemporal Asset Catalog) metadata
- **Local Server**: Can run a web server to view exported data

### Installation

```bash
# Install with pipx
pipx install "git+https://github.com/isamplesorg/export_client.git"

# Or with Poetry
git clone https://github.com/isamplesorg/export_client.git
cd export_client
poetry install
```

## GeoParquet Export Implementation

### Architecture

The GeoParquet export follows this workflow:

```
1. User runs: isample export -f geoparquet -q "source:SMITHSONIAN" -d /output
                              ↓
2. Export Client requests JSONL format from iSamples server
                              ↓
3. Server returns JSONL file (one JSON object per line)
                              ↓
4. Export Client downloads JSONL file
                              ↓
5. Export Client converts JSONL → GeoParquet
                              ↓
6. Output: isamples_export_YYYY_MM_DD_HH_MM_SS_geo.parquet
```

### Core Code: `geoparquet_utilities.py`

Location: `isamples_export_client/geoparquet_utilities.py`

```python
import logging
import os.path


def write_geoparquet_from_json_lines(filename: str) -> str:
    import pandas as pd
    import geopandas as gpd

    logging.info(f"Transforming json lines file at {filename} to geoparquet")
    filename_no_extension = os.path.splitext(filename)[0]

    # 1. Read JSONL file with pandas
    with open(filename, "r") as json_file:
        df = pd.read_json(json_file, lines=True)

        # 2. Extract longitude/latitude from nested "produced_by" field
        normalized_produced_by = pd.json_normalize(df["produced_by"])
        df["sample_location_longitude"] = normalized_produced_by["sampling_site.sample_location.longitude"]
        df["sample_location_latitude"] = normalized_produced_by["sampling_site.sample_location.latitude"]

        # 3. Create GeoDataFrame with Point geometries
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                df.sample_location_longitude,
                df.sample_location_latitude
            ),
            crs="EPSG:4326"  # WGS84 coordinate reference system
        )

    # 4. Export to GeoParquet
    dest_file = f"{filename_no_extension}_geo.parquet"
    gdf.to_parquet(dest_file)
    logging.info(f"Wrote geoparquet file to {dest_file}")
    return dest_file
```

### Key Implementation Details

1. **Data Source**: Reads from JSONL (JSON Lines) format
   - Each line is a complete JSON object representing a sample
   - Schema follows iSamples Core metadata specification

2. **Coordinate Extraction**:
   - Uses `pd.json_normalize()` to flatten nested `produced_by` structure
   - Extracts: `produced_by.sampling_site.sample_location.longitude`
   - Extracts: `produced_by.sampling_site.sample_location.latitude`

3. **Geometry Creation**:
   - Uses `gpd.points_from_xy()` to create Point geometries
   - Stores as GeoDataFrame with proper geometry column

4. **Coordinate Reference System**:
   - **CRS**: EPSG:4326 (WGS84)
   - Standard geographic coordinate system (latitude/longitude in degrees)

5. **Output Format**:
   - GeoParquet: Apache Parquet with GeoParquet spatial extension
   - Filename pattern: `{original_name}_geo.parquet`

### Integration in Export Client

Location: `isamples_export_client/export_client.py` (lines 96-101, 452-453)

```python
class ExportClient:
    def __init__(self, ..., format: str, ...):
        # When user requests geoparquet format...
        if format == "geoparquet":
            self._format = "jsonl"  # Request JSONL from server
            self.is_geoparquet = True
        else:
            self._format = format
            self.is_geoparquet = False

    def perform_full_download(self):
        # ... download JSONL file ...
        filename = self.download(uuid)

        # Convert to GeoParquet if requested
        parquet_filename = None
        if self.is_geoparquet:
            parquet_filename = write_geoparquet_from_json_lines(filename)
```

## Dependencies

From `pyproject.toml`:

```toml
[tool.poetry.dependencies]
python = "^3.11"
pandas = "^2.2.2"
geopandas = "^0.14.4"
geoarrow-pyarrow = "^0.1.2"
geoarrow-pandas = "^0.1.1"
duckdb = "^0.10.2"
```

Key libraries:
- **pandas** 2.2.2+ - Data manipulation
- **geopandas** 0.14.4+ - Geographic data operations
- **geoarrow-pyarrow** 0.1.2+ - Arrow/Parquet geographic data
- **duckdb** 0.10.2+ - For querying exported data

## Usage Example

### Command Line

```bash
# 1. Login to get JWT token
isample login
# Browser opens for ORCID authentication
# Copy the JWT token

# 2. Export to GeoParquet
export TOKEN="your_jwt_token_here"

isample export \
  -j $TOKEN \
  -f geoparquet \
  -d /output/directory \
  -q 'source:SMITHSONIAN'
```

### What Gets Created

The export creates a directory structure like:

```
/output/directory/
└── 2025_04_21_16_23_46/
    ├── isamples_export_2025_04_21_16_23_46.jsonl      # Original JSONL
    ├── isamples_export_2025_04_21_16_23_46_geo.parquet # GeoParquet!
    ├── manifest.json                                    # Export metadata
    └── stac.json                                        # STAC metadata
```

### Output File Details

**GeoParquet File**: `isamples_export_2025_04_21_16_23_46_geo.parquet`

This file contains:
- All sample metadata fields from iSamples Core schema
- A `geometry` column with Point geometries
- Coordinate columns: `sample_location_latitude`, `sample_location_longitude`
- Full nested JSON structures preserved (produced_by, curation, etc.)
- Efficient columnar storage (Parquet format)
- Geographic metadata (GeoParquet specification)

## Zenodo Export File

The file available at https://zenodo.org/records/15278211/files/isamples_export_2025_04_21_16_23_46_geo.parquet
was created using this exact process:

```bash
# Likely command used:
isample export \
  -j $TOKEN \
  -f geoparquet \
  -d /tmp \
  -q '*:*'  # Export all records
```

## Data Schema

### Input JSONL Schema (iSamples Core)

Each line in the JSONL file contains a sample record like:

```json
{
  "sample_identifier": "IGSN:BSU0005H1",
  "@id": "https://isample.org/thing/BSU0005H1",
  "label": "BJJ-4487",
  "description": "...",
  "source_collection": "SESAR",
  "has_specimen_category": [...],
  "has_material_category": [...],
  "has_context_category": [...],
  "keywords": [...],
  "produced_by": {
    "identifier": "event_id",
    "label": "Event label",
    "result_time": "2019-09-10T03:41:45Z",
    "sampling_site": {
      "label": "Site name",
      "place_name": ["Arizona", "USA"],
      "sample_location": {
        "latitude": 31.8854,
        "longitude": -110.7733,
        "elevation": 1200.0
      }
    },
    "responsibility": [...]
  },
  "curation": {...},
  "registrant": {...}
}
```

### Output GeoParquet Schema

The GeoParquet file has:

1. **All original JSONL fields** (preserved as-is)
2. **Additional extracted fields**:
   - `sample_location_latitude` (float64)
   - `sample_location_longitude` (float64)
3. **Geometry column**:
   - Name: `geometry`
   - Type: Point (2D)
   - CRS: EPSG:4326

## Why This Architecture?

The design choice to keep GeoParquet conversion **client-side** has several benefits:

1. **Server Simplicity**: iSamples server only needs to support JSONL and CSV
2. **Flexibility**: Client can add new formats without server changes
3. **Bandwidth**: JSONL is more compact than GeoParquet for transmission
4. **Local Control**: Users can customize conversion if needed
5. **STAC Integration**: Client generates STAC metadata alongside GeoParquet

## Comparison with isamples_inabox Export Service

### isamples_inabox (Server)
- **Location**: `isb_web/export.py`, `isb_lib/utilities/solr_result_transformer.py`
- **Formats**: CSV, JSONL only
- **Architecture**: Server-side transformation
- **Output**: File available via API endpoint
- **Dependencies**: petl, no geographic libraries

### export_client (Client)
- **Location**: `isamples_export_client/geoparquet_utilities.py`
- **Formats**: CSV, JSONL, GeoParquet
- **Architecture**: Client-side transformation (JSONL → GeoParquet)
- **Output**: Local file with STAC metadata
- **Dependencies**: pandas, geopandas, geoarrow

## Extending the Export

### To Add GeoParquet Support to isamples_inabox Server

If you wanted to add native GeoParquet support to the server, you would:

1. **Add dependencies** to `requirements.txt`:
   ```
   geopandas>=0.14.4
   pyarrow>=10.0.0
   ```

2. **Update `TargetExportFormat` enum** in `isb_lib/utilities/solr_result_transformer.py`:
   ```python
   class TargetExportFormat(Enum):
       CSV = "CSV"
       JSONL = "JSONL"
       GEOPARQUET = "GEOPARQUET"  # Add this
   ```

3. **Create `GeoParquetExportTransformer`** class:
   ```python
   class GeoParquetExportTransformer(AbstractExportTransformer):
       @staticmethod
       def transform(table: Table, dest_path_no_extension: str, append: bool) -> list[str]:
           import pandas as pd
           import geopandas as gpd

           # Convert petl table to pandas DataFrame
           df = pd.DataFrame(table.dicts())

           # Extract coordinates
           lat = df[SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE]
           lon = df[SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE]

           # Create GeoDataFrame
           gdf = gpd.GeoDataFrame(
               df,
               geometry=gpd.points_from_xy(lon, lat),
               crs="EPSG:4326"
           )

           # Export
           dest_path = f"{dest_path_no_extension}.parquet"
           gdf.to_parquet(dest_path)
           return [dest_path]
   ```

However, the current client-side approach is probably better for the reasons listed above.

## Additional Resources

- **Export Client Repository**: https://github.com/isamplesorg/export_client
- **Export Client Documentation**: https://github.com/isamplesorg/export_client/blob/main/README.md
- **iSamples Export Service Docs**: https://github.com/isamplesorg/isamples_inabox/blob/develop/docs/export_service.md
- **GeoParquet Specification**: https://geoparquet.org/
- **iSamples Core Schema**: https://github.com/isamplesorg/metadata

## Testing the Export Code

```bash
# Clone the export_client repository
git clone https://github.com/isamplesorg/export_client.git
cd export_client

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Test GeoParquet conversion directly
poetry run python -c "
from isamples_export_client.geoparquet_utilities import write_geoparquet_from_json_lines
result = write_geoparquet_from_json_lines('test_data.jsonl')
print(f'Created: {result}')
"
```

---

**Document Updated**: 2025-11-14
**Export Client Version**: 0.2.2
**Repository**: https://github.com/rdhyee/export_client
