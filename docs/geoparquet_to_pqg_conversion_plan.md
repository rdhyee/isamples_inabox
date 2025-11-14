# Conversion Plan: iSamples GeoParquet to PQG Format

## Overview

This document provides a detailed plan for converting the iSamples GeoParquet export file
(`isamples_export_2025_04_21_16_23_46_geo.parquet`) into the PQG (Property Graph in DuckDB) format
as documented at https://github.com/isamplesorg/pqg.

## Background

### Source Format: GeoParquet
- **File**: `isamples_export_2025_04_21_16_23_46_geo.parquet` (available on Zenodo: https://zenodo.org/records/15278211)
- **Format**: Apache Parquet with GeoParquet spatial extension
- **Content**: iSamples sample metadata including geospatial coordinates
- **Schema**: Based on iSamples Core metadata schema (see `isb_lib/models/isb_core_record.py`)

### Target Format: PQG
- **Library**: Python library for property graphs using DuckDB backend
- **Architecture**: Single-table design with nodes and edges
- **Requirements**: Python 3.11+, dataclasses-based models
- **Graph Model**: Nodes (entities) with properties + Edges (relationships)

## Understanding PQG Structure

### PQG Nodes Structure
Each node in PQG contains:
- `row_id`: Auto-incrementing primary key
- `pid`: Unique persistent identifier (string)
- `otype`: Object/node type classification
- `label`: Human-readable name
- `description`: Optional text description
- `altids`: Alternative identifiers (list)
- Custom properties as defined by dataclass

### PQG Edges Structure
Edges follow Subject-Predicate-Object model:
- `s`: Source node reference (internal integer ID)
- `p`: Relationship/predicate type (string)
- `o`: Target node reference(s) - array of integer IDs
- `n`: Optional named graph designation

### Key PQG Features
- **Automatic decomposition**: Nested objects become separate nodes with edges
- **Geographic support**: Spatial data can be included
- **Export formats**: Parquet, GeoJSON, Graphviz
- **Columnar storage**: Fast queries via DuckDB

## iSamples Data Model Analysis

### Core Entity: Sample (Thing)

Based on `isb_lib/models/isb_core_record.py` and the export service, each sample contains:

**Primary Identifiers:**
- `sample_identifier` (id) - Main sample ID (e.g., "IGSN:BSU0005H1")
- `@id` (isb_core_id) - iSamples internal identifier
- `source_collection` - Source authority (SESAR, GEOME, etc.)

**Descriptive Metadata:**
- `label` - Short name/label
- `description` - Full description
- `keywords` - List of keywords
- `informal_classification` - Free-text classification

**Controlled Vocabularies:**
- `has_specimen_category` - Sample object type (array)
- `has_material_category` - Material classification (array)
- `has_context_category` - Geological context (array)

**Sampling Event (produced_by):**
- `identifier` - Sampling event ID
- `label`, `description` - Event metadata
- `result_time` - When sample was collected
- `has_feature_of_interest` - What was sampled
- `responsibility` - Array of {role, name} objects (collectors, owners)
- `sampling_site` - Nested location information:
  - `place_name` - Array of place names
  - `label`, `description` - Site metadata
  - `sample_location`:
    - `latitude`, `longitude` - Coordinates (decimal degrees)
    - `elevation` - Elevation in meters

**Curation:**
- `label`, `description` - Curation information
- `curation_location` - Where sample is stored
- `responsibility` - Curators (array)
- `access_constraints` - Access restrictions

**Administrative:**
- `registrant` - {name} who registered the sample
- `sampling_purpose` - Purpose of sampling
- `related_resource` - Links to related resources
- `authorized_by`, `complies_with` - Authorization info
- `last_modified_time` - Source update timestamp

## Conversion Strategy

### Graph Model Design

The iSamples data will be decomposed into a property graph with the following node types and relationships:

```
┌──────────────┐
│   Sample     │
│  (otype:     │
│   "Sample")  │
└──────┬───────┘
       │
       │ has_material_category
       ├──────────────────────────────► ┌───────────────────┐
       │                                 │ MaterialCategory  │
       │ has_specimen_category           │ (otype:           │
       ├──────────────────────────────► │  "Vocabulary")    │
       │                                 └───────────────────┘
       │ has_context_category
       ├──────────────────────────────► ┌───────────────────┐
       │                                 │ ContextCategory   │
       │                                 │ (otype:           │
       │                                 │  "Vocabulary")    │
       │                                 └───────────────────┘
       │ produced_by
       ├──────────────────────────────► ┌───────────────────┐
       │                                 │  SamplingEvent    │
       │                                 │  (otype:          │
       │                                 │   "Event")        │
       │                                 └─────────┬─────────┘
       │                                           │
       │                                           │ at_site
       │                                           ├────────► ┌──────────────┐
       │                                           │          │ SamplingSite │
       │                                           │          │ (otype:      │
       │                                           │          │  "Place")    │
       │                                           │          │ + geometry   │
       │                                           │          └──────────────┘
       │                                           │
       │                                           │ has_responsibility
       │                                           └────────► ┌──────────────┐
       │                                                      │   Person/Org │
       │                                                      │   (otype:    │
       │                                                      │   "Agent")   │
       │                                                      └──────────────┘
       │ curated_by
       ├──────────────────────────────► ┌───────────────────┐
       │                                 │   Curation        │
       │                                 │   (otype:         │
       │                                 │    "Activity")    │
       │                                 └───────────────────┘
       │ registered_by
       └──────────────────────────────► ┌───────────────────┐
                                         │  Registrant       │
                                         │  (otype: "Agent") │
                                         └───────────────────┘
```

### Node Types (otype values)

1. **Sample** - Core sample entity
2. **SamplingEvent** - The event that produced the sample
3. **SamplingSite** - Geographic location (with geometry)
4. **Person** or **Organization** - Agents (collectors, curators, registrants)
5. **VocabularyTerm** - Controlled vocabulary terms (material, specimen, context categories)
6. **Curation** - Curation activity
7. **Keyword** - Keywords for search
8. **RelatedResource** - Links to external resources

### Relationship Types (predicate values)

- `produced_by` - Sample → SamplingEvent
- `at_site` - SamplingEvent → SamplingSite
- `has_responsibility` - Event/Curation → Person/Organization (with role property)
- `has_material_category` - Sample → VocabularyTerm
- `has_specimen_category` - Sample → VocabularyTerm
- `has_context_category` - Sample → VocabularyTerm
- `has_keyword` - Sample → Keyword
- `curated_by` - Sample → Curation
- `registered_by` - Sample → Person/Organization
- `related_to` - Sample → RelatedResource

## Implementation Steps

### Phase 1: Setup and Dependencies

1. **Install required packages:**
```bash
pip install duckdb pyarrow geopandas pqg
```

2. **Create project structure:**
```
conversion_project/
├── src/
│   ├── models.py          # PQG dataclass definitions
│   ├── loader.py          # Load GeoParquet data
│   ├── transformer.py     # Transform to PQG format
│   └── exporter.py        # Export PQG graph
├── scripts/
│   └── convert.py         # Main conversion script
├── tests/
│   └── test_conversion.py # Unit tests
└── README.md
```

### Phase 2: Define PQG Data Models

Create dataclass models in `src/models.py`:

```python
from dataclasses import dataclass, field
from typing import Optional, List
from pqg import Base

@dataclass
class Sample(Base):
    """Main sample node"""
    pid: str  # sample_identifier
    otype: str = "Sample"
    label: str = ""
    description: str = ""
    altids: List[str] = field(default_factory=list)  # e.g., isb_core_id
    source_collection: str = ""
    informal_classification: List[str] = field(default_factory=list)
    last_modified_time: Optional[str] = None

@dataclass
class SamplingEvent(Base):
    """Sampling event that produced the sample"""
    pid: str  # Constructed from sample_id + "_event"
    otype: str = "SamplingEvent"
    label: str = ""
    description: str = ""
    result_time: Optional[str] = None
    has_feature_of_interest: str = ""

@dataclass
class SamplingSite(Base):
    """Geographic location with spatial data"""
    pid: str  # Constructed from coordinates or site_label
    otype: str = "SamplingSite"
    label: str = ""
    description: str = ""
    place_names: List[str] = field(default_factory=list)
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    elevation: Optional[float] = None
    # PQG supports geometry - can store as WKT or GeoJSON
    geometry: Optional[str] = None

@dataclass
class Agent(Base):
    """Person or organization"""
    pid: str  # Name-based or unique ID
    otype: str = "Agent"  # Could be "Person" or "Organization"
    label: str = ""
    role: Optional[str] = None  # Role in specific context

@dataclass
class VocabularyTerm(Base):
    """Controlled vocabulary term"""
    pid: str  # Vocabulary identifier URI
    otype: str = "VocabularyTerm"
    label: str = ""
    category: str = ""  # "material", "specimen", or "context"

@dataclass
class Curation(Base):
    """Curation information"""
    pid: str  # Constructed from sample + curation info
    otype: str = "Curation"
    label: str = ""
    description: str = ""
    location: str = ""
    access_constraints: List[str] = field(default_factory=list)

@dataclass
class Keyword(Base):
    """Keyword for search"""
    pid: str  # The keyword itself
    otype: str = "Keyword"
    label: str = ""
```

### Phase 3: Load GeoParquet Data

Create data loader in `src/loader.py`:

```python
import geopandas as gpd
import pyarrow.parquet as pq

class GeoParquetLoader:
    """Load iSamples GeoParquet export"""

    def __init__(self, parquet_path: str):
        self.parquet_path = parquet_path

    def load(self) -> gpd.GeoDataFrame:
        """Load GeoParquet file as GeoDataFrame"""
        gdf = gpd.read_parquet(self.parquet_path)
        print(f"Loaded {len(gdf)} samples")
        print(f"Columns: {gdf.columns.tolist()}")
        return gdf

    def get_schema(self):
        """Examine parquet schema"""
        parquet_file = pq.ParquetFile(self.parquet_path)
        return parquet_file.schema
```

### Phase 4: Transform to PQG Format

Create transformer in `src/transformer.py`:

```python
from typing import List, Dict, Set
import json
from pqg import Graph
from .models import (
    Sample, SamplingEvent, SamplingSite, Agent,
    VocabularyTerm, Curation, Keyword
)

class ISamplesToPQGTransformer:
    """Transform iSamples data to PQG property graph"""

    def __init__(self):
        self.graph = Graph()
        self.seen_pids: Set[str] = set()  # Track created nodes

    def transform_sample(self, row: dict) -> Sample:
        """Transform a single sample record to Sample node"""
        sample = Sample(
            pid=row['sample_identifier'],
            label=row.get('label', ''),
            description=row.get('description', ''),
            altids=[row.get('@id', '')],  # isb_core_id as altid
            source_collection=row.get('source_collection', ''),
            informal_classification=self._to_list(
                row.get('informal_classification', [])
            ),
            last_modified_time=row.get('last_modified_time')
        )
        self.graph.add_node(sample)
        return sample

    def transform_sampling_event(self, sample_pid: str,
                                   produced_by: dict) -> SamplingEvent:
        """Transform sampling event from produced_by field"""
        event_pid = produced_by.get('identifier',
                                     f"{sample_pid}_event")

        event = SamplingEvent(
            pid=event_pid,
            label=produced_by.get('label', ''),
            description=produced_by.get('description', ''),
            result_time=produced_by.get('result_time'),
            has_feature_of_interest=produced_by.get(
                'has_feature_of_interest', ''
            )
        )
        self.graph.add_node(event)

        # Create edge: Sample produced_by SamplingEvent
        self.graph.add_edge(sample_pid, 'produced_by', event_pid)

        return event

    def transform_sampling_site(self, event_pid: str,
                                  sampling_site: dict) -> SamplingSite:
        """Transform sampling site with geographic data"""
        # Use coordinates or label to create unique PID
        lat = sampling_site.get('sample_location', {}).get('latitude')
        lon = sampling_site.get('sample_location', {}).get('longitude')

        if lat and lon:
            site_pid = f"site_{lat}_{lon}"
        else:
            site_pid = f"site_{sampling_site.get('label', 'unknown')}"

        # Create Point geometry if coordinates available
        geometry = None
        if lat and lon:
            geometry = f"POINT({lon} {lat})"  # WKT format

        site = SamplingSite(
            pid=site_pid,
            label=sampling_site.get('label', ''),
            description=sampling_site.get('description', ''),
            place_names=self._to_list(sampling_site.get('place_name', [])),
            latitude=lat,
            longitude=lon,
            elevation=sampling_site.get('sample_location', {}).get(
                'elevation'
            ),
            geometry=geometry
        )

        if site_pid not in self.seen_pids:
            self.graph.add_node(site)
            self.seen_pids.add(site_pid)

        # Create edge: SamplingEvent at_site SamplingSite
        self.graph.add_edge(event_pid, 'at_site', site_pid)

        return site

    def transform_agents(self, context_pid: str,
                         relationship_type: str,
                         responsibilities: List[dict]):
        """Transform responsibility records to Agent nodes"""
        for resp in responsibilities:
            name = resp.get('name', '')
            role = resp.get('role', '')

            # Create agent PID from name (could enhance with ORCID if available)
            agent_pid = f"agent_{name.replace(' ', '_').lower()}"

            if agent_pid not in self.seen_pids:
                agent = Agent(
                    pid=agent_pid,
                    label=name,
                    role=role
                )
                self.graph.add_node(agent)
                self.seen_pids.add(agent_pid)

            # Create edge with role as property
            self.graph.add_edge(
                context_pid,
                relationship_type,
                agent_pid,
                properties={'role': role}
            )

    def transform_vocabulary_terms(self, sample_pid: str,
                                     terms: List[dict],
                                     category: str,
                                     relationship: str):
        """Transform controlled vocabulary terms"""
        for term in terms:
            term_id = term.get('identifier', '')
            if not term_id:
                continue

            term_pid = term_id  # Use vocabulary URI as PID

            if term_pid not in self.seen_pids:
                vocab_term = VocabularyTerm(
                    pid=term_pid,
                    label=term_id.split('/')[-1],  # Extract label from URI
                    category=category
                )
                self.graph.add_node(vocab_term)
                self.seen_pids.add(term_pid)

            # Create edge: Sample → VocabularyTerm
            self.graph.add_edge(sample_pid, relationship, term_pid)

    def transform_keywords(self, sample_pid: str, keywords: List[dict]):
        """Transform keywords"""
        for kw in keywords:
            keyword_text = kw.get('keyword', '')
            if not keyword_text:
                continue

            kw_pid = f"keyword_{keyword_text.lower().replace(' ', '_')}"

            if kw_pid not in self.seen_pids:
                keyword = Keyword(
                    pid=kw_pid,
                    label=keyword_text
                )
                self.graph.add_node(keyword)
                self.seen_pids.add(kw_pid)

            self.graph.add_edge(sample_pid, 'has_keyword', kw_pid)

    def transform_curation(self, sample_pid: str, curation: dict):
        """Transform curation information"""
        if not curation or not any(curation.values()):
            return  # Skip empty curation

        curation_pid = f"{sample_pid}_curation"

        curation_node = Curation(
            pid=curation_pid,
            label=curation.get('label', ''),
            description=curation.get('description', ''),
            location=curation.get('curation_location', ''),
            access_constraints=self._to_list(
                curation.get('access_constraints', [])
            )
        )
        self.graph.add_node(curation_node)

        # Create edge: Sample curated_by Curation
        self.graph.add_edge(sample_pid, 'curated_by', curation_pid)

        # Transform curators as agents
        if 'responsibility' in curation:
            self.transform_agents(
                curation_pid,
                'has_curator',
                curation['responsibility']
            )

    def transform_row(self, row: dict):
        """Transform a single GeoParquet row to graph nodes/edges"""
        # Parse JSON fields if they're strings
        row = self._parse_json_fields(row)

        # 1. Create Sample node
        sample = self.transform_sample(row)
        sample_pid = sample.pid

        # 2. Transform produced_by (sampling event and site)
        if 'produced_by' in row and row['produced_by']:
            produced_by = row['produced_by']
            event = self.transform_sampling_event(sample_pid, produced_by)

            # 3. Transform sampling site
            if 'sampling_site' in produced_by:
                self.transform_sampling_site(
                    event.pid,
                    produced_by['sampling_site']
                )

            # 4. Transform event responsibilities (collectors, etc.)
            if 'responsibility' in produced_by:
                self.transform_agents(
                    event.pid,
                    'has_responsibility',
                    produced_by['responsibility']
                )

        # 5. Transform vocabulary terms
        if 'has_specimen_category' in row:
            self.transform_vocabulary_terms(
                sample_pid,
                row['has_specimen_category'],
                'specimen',
                'has_specimen_category'
            )

        if 'has_material_category' in row:
            self.transform_vocabulary_terms(
                sample_pid,
                row['has_material_category'],
                'material',
                'has_material_category'
            )

        if 'has_context_category' in row:
            self.transform_vocabulary_terms(
                sample_pid,
                row['has_context_category'],
                'context',
                'has_context_category'
            )

        # 6. Transform keywords
        if 'keywords' in row:
            self.transform_keywords(sample_pid, row['keywords'])

        # 7. Transform curation
        if 'curation' in row:
            self.transform_curation(sample_pid, row['curation'])

        # 8. Transform registrant
        if 'registrant' in row and row['registrant']:
            registrant = row['registrant']
            if isinstance(registrant, dict):
                name = registrant.get('name', '')
                agent_pid = f"agent_{name.replace(' ', '_').lower()}"

                if agent_pid not in self.seen_pids:
                    agent = Agent(pid=agent_pid, label=name)
                    self.graph.add_node(agent)
                    self.seen_pids.add(agent_pid)

                self.graph.add_edge(
                    sample_pid,
                    'registered_by',
                    agent_pid
                )

    def _to_list(self, value):
        """Ensure value is a list"""
        if isinstance(value, str):
            return [value]
        elif isinstance(value, list):
            return value
        else:
            return []

    def _parse_json_fields(self, row: dict) -> dict:
        """Parse JSON string fields to dicts/lists"""
        for key, value in row.items():
            if isinstance(value, str) and value.startswith('{'):
                try:
                    row[key] = json.loads(value)
                except:
                    pass
            elif isinstance(value, str) and value.startswith('['):
                try:
                    row[key] = json.loads(value)
                except:
                    pass
        return row

    def get_graph(self) -> Graph:
        """Return the constructed graph"""
        return self.graph
```

### Phase 5: Main Conversion Script

Create `scripts/convert.py`:

```python
#!/usr/bin/env python3
"""
Convert iSamples GeoParquet export to PQG format

Usage:
    python scripts/convert.py \\
        --input isamples_export_2025_04_21_16_23_46_geo.parquet \\
        --output isamples_graph.duckdb \\
        --export-geojson samples.geojson \\
        --limit 1000
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from loader import GeoParquetLoader
from transformer import ISamplesToPQGTransformer

def main():
    parser = argparse.ArgumentParser(
        description='Convert iSamples GeoParquet to PQG format'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='Input GeoParquet file path'
    )
    parser.add_argument(
        '--output',
        default='isamples_graph.duckdb',
        help='Output DuckDB file path'
    )
    parser.add_argument(
        '--export-geojson',
        help='Optional: Export geographic nodes as GeoJSON'
    )
    parser.add_argument(
        '--export-parquet',
        help='Optional: Export graph as Parquet'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of samples to process (for testing)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    # 1. Load GeoParquet
    print(f"Loading GeoParquet from {args.input}...")
    loader = GeoParquetLoader(args.input)
    gdf = loader.load()

    if args.verbose:
        print(f"Schema: {loader.get_schema()}")
        print(f"Sample columns: {gdf.columns.tolist()}")

    # Limit if requested
    if args.limit:
        print(f"Limiting to {args.limit} samples for testing")
        gdf = gdf.head(args.limit)

    # 2. Transform to PQG
    print("Transforming to PQG property graph...")
    transformer = ISamplesToPQGTransformer()

    for idx, row in gdf.iterrows():
        if args.verbose and idx % 1000 == 0:
            print(f"Processed {idx} samples...")

        transformer.transform_row(row.to_dict())

    graph = transformer.get_graph()

    # 3. Save graph to DuckDB
    print(f"Saving graph to {args.output}...")
    graph.save(args.output)

    # 4. Export additional formats if requested
    if args.export_geojson:
        print(f"Exporting geographic data to {args.export_geojson}...")
        graph.export_geojson(args.export_geojson)

    if args.export_parquet:
        print(f"Exporting graph to Parquet: {args.export_parquet}...")
        graph.export_parquet(args.export_parquet)

    # 5. Print statistics
    print("\nConversion complete!")
    print(f"Nodes: {graph.node_count()}")
    print(f"Edges: {graph.edge_count()}")
    print(f"Node types: {graph.node_types()}")
    print(f"Relationship types: {graph.relationship_types()}")

if __name__ == '__main__':
    main()
```

### Phase 6: Testing and Validation

Create `tests/test_conversion.py`:

```python
import pytest
from src.loader import GeoParquetLoader
from src.transformer import ISamplesToPQGTransformer
from src.models import Sample, SamplingEvent, SamplingSite

def test_sample_transformation():
    """Test basic sample transformation"""
    row = {
        'sample_identifier': 'IGSN:TEST001',
        '@id': 'https://isample.org/thing/TEST001',
        'label': 'Test Sample',
        'description': 'A test sample',
        'source_collection': 'TEST',
        'informal_classification': ['rock']
    }

    transformer = ISamplesToPQGTransformer()
    sample = transformer.transform_sample(row)

    assert sample.pid == 'IGSN:TEST001'
    assert sample.label == 'Test Sample'
    assert 'https://isample.org/thing/TEST001' in sample.altids

def test_sampling_site_with_coordinates():
    """Test sampling site with geographic coordinates"""
    sampling_site = {
        'label': 'Test Site',
        'description': 'A test location',
        'place_name': ['California', 'USA'],
        'sample_location': {
            'latitude': 37.7749,
            'longitude': -122.4194,
            'elevation': 100.0
        }
    }

    transformer = ISamplesToPQGTransformer()
    site = transformer.transform_sampling_site('event_1', sampling_site)

    assert site.latitude == 37.7749
    assert site.longitude == -122.4194
    assert site.geometry == 'POINT(-122.4194 37.7749)'
    assert 'California' in site.place_names

def test_full_row_transformation():
    """Test complete row transformation with all components"""
    row = {
        'sample_identifier': 'IGSN:TEST002',
        '@id': 'https://isample.org/thing/TEST002',
        'label': 'Full Test Sample',
        'description': 'Complete test',
        'source_collection': 'TEST',
        'has_material_category': [
            {'identifier': 'http://vocab.org/Rock'}
        ],
        'produced_by': {
            'identifier': 'event_test_002',
            'label': 'Test Sampling Event',
            'result_time': '2025-01-15',
            'responsibility': [
                {'name': 'John Doe', 'role': 'Collector'}
            ],
            'sampling_site': {
                'label': 'Test Location',
                'sample_location': {
                    'latitude': 40.7128,
                    'longitude': -74.0060
                }
            }
        },
        'keywords': [{'keyword': 'geology'}],
        'registrant': {'name': 'Jane Smith'}
    }

    transformer = ISamplesToPQGTransformer()
    transformer.transform_row(row)
    graph = transformer.get_graph()

    # Verify nodes were created
    assert graph.node_count() > 0
    # Verify edges were created
    assert graph.edge_count() > 0
```

## Execution Plan

### Step-by-Step Execution

1. **Download GeoParquet file:**
```bash
# Download from Zenodo
wget https://zenodo.org/records/15278211/files/isamples_export_2025_04_21_16_23_46_geo.parquet
```

2. **Setup Python environment:**
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install duckdb pyarrow geopandas pqg
```

3. **Test with small subset:**
```bash
python scripts/convert.py \\
    --input isamples_export_2025_04_21_16_23_46_geo.parquet \\
    --output test_graph.duckdb \\
    --limit 100 \\
    --verbose
```

4. **Run full conversion:**
```bash
python scripts/convert.py \\
    --input isamples_export_2025_04_21_16_23_46_geo.parquet \\
    --output isamples_full_graph.duckdb \\
    --export-geojson isamples_sites.geojson \\
    --verbose
```

5. **Validate results:**
```bash
# Use DuckDB CLI to explore
duckdb isamples_full_graph.duckdb
# Run queries to verify data
```

## Expected Challenges and Solutions

### Challenge 1: Large Data Volume
**Problem**: GeoParquet file may contain millions of samples
**Solution**:
- Process in batches
- Use streaming/iterative processing
- Monitor memory usage
- Consider parallel processing for large datasets

### Challenge 2: Nested JSON Structures
**Problem**: GeoParquet may store complex nested JSON
**Solution**:
- Implement robust JSON parsing in `_parse_json_fields()`
- Handle both string and native JSON types
- Add error handling for malformed JSON

### Challenge 3: Duplicate Node Detection
**Problem**: Same agents/locations may appear multiple times
**Solution**:
- Use `seen_pids` set to track created nodes
- Create consistent PID generation for agents (name-based)
- For sites, use coordinate-based PIDs

### Challenge 4: Missing Geographic Data
**Problem**: Not all samples may have coordinates
**Solution**:
- Make latitude/longitude optional in SamplingSite
- Create site PIDs from labels when coordinates missing
- Skip geometry field if coordinates unavailable

### Challenge 5: Vocabulary Term URIs
**Problem**: Controlled vocabulary may use full URIs
**Solution**:
- Use full URI as PID
- Extract human-readable label from URI
- Store category type for filtering

## Query Examples (Post-Conversion)

Once converted to PQG, you can query the graph using DuckDB SQL:

```sql
-- Find all samples from SESAR
SELECT * FROM nodes
WHERE otype = 'Sample'
AND source_collection = 'SESAR';

-- Find all sampling sites in a region
SELECT * FROM nodes
WHERE otype = 'SamplingSite'
AND latitude BETWEEN 30 AND 40
AND longitude BETWEEN -120 AND -110;

-- Find samples by material category
SELECT s.*
FROM nodes s
JOIN edges e ON s.pid = e.s
JOIN nodes v ON e.o[1] = v.row_id
WHERE s.otype = 'Sample'
AND e.p = 'has_material_category'
AND v.category = 'material';

-- Find all samples collected by a specific person
SELECT s.*
FROM nodes s
JOIN edges e1 ON s.pid = e1.s
JOIN edges e2 ON e1.o[1] IN (SELECT row_id FROM nodes WHERE pid IN (SELECT o[1] FROM edges WHERE s = e1.o[1]))
JOIN nodes agent ON agent.row_id = e2.o[1]
WHERE s.otype = 'Sample'
AND agent.label = 'John Doe'
AND agent.role = 'Collector';
```

## Performance Considerations

- **Batch size**: Process 10,000-50,000 records per batch
- **Memory**: Monitor with `--limit` during testing
- **Indexing**: PQG/DuckDB handles indexing automatically
- **Export time**: Full dataset may take 30-60 minutes
- **Storage**: Expect 2-3x size increase due to graph structure

## Next Steps

1. Obtain the GeoParquet export code from the iSamples team (not found in current repository)
2. Implement the data models and transformer classes
3. Test with small subset (100-1000 samples)
4. Validate graph structure and relationships
5. Run full conversion
6. Create sample queries for common use cases
7. Document query patterns for end users

## References

- **PQG Documentation**: https://github.com/isamplesorg/pqg
- **iSamples GeoParquet**: https://zenodo.org/records/15278211
- **iSamples Metadata Schema**: See `isb_lib/models/isb_core_record.py`
- **GeoParquet Specification**: https://geoparquet.org/

---

**Document Version**: 1.0
**Last Updated**: 2025-11-14
**Author**: Claude (AI Assistant)
