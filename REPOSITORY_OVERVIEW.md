# iSamples in a Box - Repository Overview

## Table of Contents
1. [Project Purpose](#project-purpose)
2. [Architecture Overview](#architecture-overview)
3. [Key Components](#key-components)
4. [Getting Started](#getting-started)
5. [Data Flow](#data-flow)
6. [Key Scripts and Entry Points](#key-scripts-and-entry-points)
7. [API Endpoints](#api-endpoints)
8. [Development Workflow](#development-workflow)
9. [Testing](#testing)
10. [Deployment](#deployment)

## Project Purpose

**iSamples in a Box** (ISB) is a comprehensive Python-based system for aggregating, managing, and providing access to geological and environmental sample metadata from multiple authoritative sources. The system enables researchers and institutions to:

- **Harvest** sample data from multiple repositories (SESAR, GEOME, Smithsonian, OpenContext)
- **Store** sample records in a PostgreSQL database with full metadata
- **Index** relationships and searchable metadata in Apache Solr for fast querying
- **Expose** data through a REST API using FastAPI
- **Browse** samples through a web UI
- **Mint** identifiers (DataCite DOIs) with ORCID authentication
- **Search** geospatially using H3 hexagon-based heatmaps

**Current Version:** 0.5.1
**License:** Apache 2.0
**Python Version:** 3.11+

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  SESAR  │  GEOME  │  Smithsonian  │  OpenContext            │
└────┬────────┬───────────┬──────────────┬─────────────────────┘
     │        │           │              │
     │   Source Adapters (isb_lib/*_adapter.py)
     │        │           │              │
     ▼        ▼           ▼              ▼
┌────────────────────────────────────────────────────────────┐
│              Metadata Transformers                          │
│         (isamples_metadata/*Transformer.py)                 │
└────────────────────────┬───────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │     PostgreSQL Database        │
        │  (SQLModel ORM - Thing model)  │
        └────────────────┬───────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │      Apache Solr Index         │
        │   (isb_core_records collection)│
        └────────────────┬───────────────┘
                         │
                         ▼
        ┌────────────────────────────────┐
        │   FastAPI Web Service          │
        │   (isb_web/main.py)            │
        │   - REST API                    │
        │   - Web UI (Jinja2 templates)  │
        │   - Export Service             │
        └────────────────────────────────┘
```

## Key Components

### 1. Core Library (`isb_lib/`)

The heart of the system, containing business logic and utilities:

- **`core.py`** (803 lines) - Core utilities including date parsing, validation, vocabulary management
- **Source Adapters**:
  - `sesar_adapter.py` - SESAR (System for Earth Sample Registration)
  - `geome_adapter.py` - GEOME (Genomic Observatories Metadatabase)
  - `smithsonian_adapter.py` - Smithsonian Institution collections
  - `opencontext_adapter.py` - Open Context archaeological data
- **`models/`** - SQLModel ORM definitions:
  - `thing.py` - Core `Thing` model representing a sample
  - `isb_core_record.py` - Extended metadata model
  - `export_job.py` - Export job tracking
  - `namespace.py` - Identifier namespaces
- **`identifiers/`** - Identifier minting (DataCite DOIs, N2T ARKs)
- **`vocabulary/`** - Controlled vocabulary management
- **`utilities/`** - Helper utilities (H3 geospatial, Solr transformations)
- **`sitemaps/`** - Sitemap generation for search engines
- **`authorization/`** - User authentication and authorization

### 2. Web Service (`isb_web/`)

FastAPI-based REST API and web interface:

- **`main.py`** (931 lines) - Main FastAPI application with all routes
- **`sqlmodel_database.py`** (630 lines) - Database access object (DAO)
- **`isb_solr_query.py`** - Solr query builder and executor
- **`export.py`** - Data export service (CSV, JSONL)
- **`manage.py`** - User and identifier management
- **`auth.py`** - ORCID OAuth authentication
- **`templates/`** - Jinja2 HTML templates for web UI
- **`static/`** - CSS, JavaScript, controlled vocabulary JSON files

### 3. Metadata Transformation (`isamples_metadata/`)

Transforms source data to standardized iSamples schema:

- **Transformers** for each source (SESAR, GEOME, OpenContext, Smithsonian)
- **Controlled vocabularies** for consistent categorization
- **Taxonomy mappings** for biological classifications

### 4. Scripts (`scripts/`)

CLI tools for data management (22+ scripts):

**Main Entry Points:**
- `sesar_things.py` - Load and index SESAR samples
- `geome_things.py` - Load and index GEOME samples
- `opencontext_things.py` - Load OpenContext samples
- `smithsonian_things.py` - Load Smithsonian samples
- `isb_things.py` - General ISB operations

**Utility Scripts:**
- `dump_thing_json.py` - Export Thing records as JSON
- `create_sql_lite_dump.py` - Create SQLite database dumps
- `load_isamples_vocabularies.py` - Load controlled vocabularies
- `migrations/` - Database migration utilities

## Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL
- Apache Solr 8.8+
- Poetry (Python dependency management)

### Quick Setup

1. **Clone and setup Python environment:**
```bash
git clone git@github.com:isamplesorg/isamples_inabox.git
cd isamples_inabox
poetry install
```

2. **Setup PostgreSQL:**
```bash
psql postgres
CREATE DATABASE isb_1;
CREATE USER isb_writer WITH ENCRYPTED PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE isb_1 TO isb_writer;
```

3. **Setup Solr:**
```bash
solr create -c isb_core_records
python scripts/solr_schema_init/create_isb_core_schema.py
```

4. **Create configuration file (`isb.cfg`):**
```ini
db_url = "postgresql+psycopg2://isb_writer:your_password@localhost/isb_1"
solr_url = "http://localhost:8983/solr/isb_core_records/"
max_records = 1000
verbosity = "INFO"
```

5. **Load sample data:**
```bash
poetry run sesar_things --config isb.cfg load -m 5000
poetry run sesar_things --config isb.cfg relations
```

6. **Start web service:**
```bash
python isb_web/main.py
# Navigate to http://localhost:8000/
```

## Data Flow

### 1. Ingestion Flow

```
Source API → Adapter → Transformer → PostgreSQL Thing Table
                                   ↓
                                   Solr Index (for search)
```

**Example: Loading SESAR data**
```bash
poetry run sesar_things --config isb.cfg load -m 5000
poetry run sesar_things --config isb.cfg relations
```

### 2. Query Flow

```
User/API Request → FastAPI (isb_web/main.py)
                 ↓
                 Solr Query (for search/filter)
                 ↓
                 PostgreSQL (for full record details)
                 ↓
                 JSON Response
```

### 3. Export Flow

```
User → Export API (/export/create?q=...&format=CSV)
     ↓
     Export Job Created (UUID returned)
     ↓
     Background Worker queries Solr
     ↓
     Results transformed (SolrResultTransformer)
     ↓
     File written (/tmp/{uuid}.csv or .jsonl)
     ↓
     User downloads via /export/download?uuid=...
```

## Key Scripts and Entry Points

### Web Service

```bash
# Start FastAPI server (dev mode)
python isb_web/main.py

# Production deployment uses uvicorn:
uvicorn isb_web.main:app --host 0.0.0.0 --port 8000
```

### Data Loading (via Poetry)

```bash
# SESAR samples
poetry run sesar_things --config isb.cfg load -m 5000
poetry run sesar_things --config isb.cfg relations

# GEOME samples
poetry run geome_things --config isb.cfg load -m 5000
poetry run geome_things --config isb.cfg relations

# OpenContext samples
poetry run opencontext_things --config isb.cfg load

# Smithsonian samples
poetry run smithsonian_things --config isb.cfg load
```

### Utility Scripts

```bash
# Dump Thing records as JSON
python scripts/dump_thing_json.py -d <db_url> -a SMITHSONIAN -c 1000 -p /output/path

# Create SQLite dump
python scripts/create_sql_lite_dump.py --config isb.cfg -q "*:*"

# Load controlled vocabularies
python scripts/load_isamples_vocabularies.py --config isb.cfg
```

## API Endpoints

The FastAPI service provides multiple API categories:

### Things API (`/thing`)
- `GET /thing/{identifier}` - Get a specific Thing by identifier
- `GET /thing` - Search Things with filtering

### Solr API (`/solr`)
- `GET /solr/search` - Direct Solr query interface
- `GET /solr/select` - Solr select handler
- `GET /solr/heatmap` - Get H3 hexagon heatmap data

### Export API (`/export`) - **Requires ORCID authentication**
- `GET /export/create?q=...&export_format=CSV|JSONL` - Create export job
- `GET /export/status?uuid=...` - Check export job status
- `GET /export/download?uuid=...` - Download completed export

### Vocabularies API (`/vocabularies`)
- `GET /vocabularies` - List all controlled vocabularies
- `GET /vocabularies/{vocab_name}` - Get specific vocabulary

### Management API (`/manage`) - **Requires authentication**
- `GET /manage/login` - ORCID OAuth login
- `POST /manage/identifiers` - Mint new identifiers

### Metrics API (`/metrics`)
- `GET /metrics` - Prometheus-compatible metrics

## Development Workflow

### Code Quality Tools

The project enforces code quality through:

1. **flake8** - Linting (max complexity 10)
```bash
flake8 isb_lib isb_web scripts tests
```

2. **mypy** - Type checking
```bash
mypy isb_lib isb_web scripts
```

3. **black** - Code formatting (recommended)
```bash
black isb_lib isb_web scripts tests
```

4. **pytest** - Unit testing (71% coverage minimum required)
```bash
pytest --cov --cov-fail-under=71
```

### Git Workflow

- Main branch: `main` (production)
- Development branch: `develop`
- Feature branches: Create from `develop`
- Pull requests must pass CI/CD checks (GitHub Actions)

### CI/CD

GitHub Actions workflows:
- `.github/workflows/python-app.yml` - Unit tests + linting on every PR
- `.github/workflows/python-integration-test.yaml` - Integration tests

## Testing

### Unit Tests

```bash
# Run all tests with coverage
pytest --cov --cov-fail-under=71

# Run specific test file
pytest tests/test_core.py

# Run with verbose output
pytest -v
```

### Integration Tests

Integration tests verify end-to-end functionality:

```bash
# Run integration tests (requires running Solr + PostgreSQL)
pytest integration_tests/
```

See `docs/indexing_integration_test.md` for details.

## Deployment

### Docker Deployment

The project includes Docker support for containerized deployment:

```bash
# Build Docker image
docker build -t isamples_inabox .

# Run with docker-compose (includes PostgreSQL + Solr)
docker-compose up
```

### Production Considerations

1. **Database**: Use managed PostgreSQL service (AWS RDS, Google Cloud SQL)
2. **Solr**: Run in SolrCloud mode with ZooKeeper for high availability
3. **Web Service**: Deploy behind reverse proxy (Nginx) with HTTPS
4. **Secrets**: Use environment variables for sensitive configuration
5. **Monitoring**: Enable Prometheus metrics endpoint (`/metrics`)

### Environment Variables

Key environment variables for production:

```bash
db_url=postgresql+psycopg2://user:pass@host:5432/dbname
solr_url=http://solr-host:8983/solr/isb_core_records/
ORCID_CLIENT_ID=your_orcid_client_id
ORCID_CLIENT_SECRET=your_orcid_secret
ORCID_ISSUER=https://orcid.org
orcid_superusers=0000-0001-2345-6789,0000-0002-3456-7890
```

## Data Model

### Core Entity: Thing

The `Thing` model (in `isb_lib/models/thing.py`) represents a sample:

**Key Fields:**
- `id` - Globally unique identifier (format: `scheme:value`)
- `authority_id` - Source authority (SESAR, GEOME, etc.)
- `resolved_content` - Full JSON metadata from source
- `resolved_status` - HTTP status of last fetch
- `item_type` - Type of sample
- `tcreated` - Creation timestamp
- `tstamp` - Last update timestamp
- Plus 30+ additional metadata fields

### ISBCoreRecord

Extended metadata following iSamples Core schema:
- Sample identifiers and labels
- Geospatial information (lat/lon, elevation, H3 hexagons)
- Sampling context (site, purpose, method)
- Material and specimen classifications
- Curation information
- Related resources

## Documentation

Additional documentation in `docs/`:

- `python_setup.md.html` - Python environment setup
- `authentication_and_identifiers.md` - ORCID OAuth and DOI minting
- `export_service.md` - Export API usage
- `SOLR_Performance_Testing.md` - Performance benchmarking
- `sitemaps_and_transport.md` - Sitemap generation
- `hypothesis_integration.md` - Web annotation integration
- `flat_file_import.md` - CSV import procedures

## Support and Contributing

- **Issues**: Report bugs at https://github.com/isamplesorg/isamples_inabox/issues
- **Contributing**: Submit pull requests to `develop` branch
- **License**: Apache 2.0

## Common Tasks

### Add a new sample source

1. Create adapter in `isb_lib/` (e.g., `newsource_adapter.py`)
2. Create transformer in `isamples_metadata/` (e.g., `NewSourceTransformer.py`)
3. Create CLI script in `scripts/` (e.g., `newsource_things.py`)
4. Add entry point to `pyproject.toml`
5. Update documentation

### Export data

```bash
# Via API (requires ORCID authentication)
curl -H "Authorization: Bearer <JWT>" \
  "https://central.isample.xyz/isamples_central/export/create?q=source:SESAR&export_format=jsonl"

# Returns: {"status":"created","uuid":"..."}

# Check status
curl -H "Authorization: Bearer <JWT>" \
  "https://central.isample.xyz/isamples_central/export/status?uuid=..."

# Download when complete
curl -H "Authorization: Bearer <JWT>" \
  "https://central.isample.xyz/isamples_central/export/download?uuid=..."
```

### Query samples

```bash
# Search via Solr API
curl "http://localhost:8000/solr/search?q=keywords:geology&rows=10"

# Get specific Thing
curl "http://localhost:8000/thing/igsn:XXXXX"

# Get geospatial heatmap
curl "http://localhost:8000/solr/heatmap?q=*:*&h3_resolution=4"
```

## Technology Stack Summary

- **Language**: Python 3.11+
- **Web Framework**: FastAPI 0.104.0 + Uvicorn
- **Database**: PostgreSQL (SQLAlchemy/SQLModel ORM)
- **Search**: Apache Solr 8.8+
- **Authentication**: OAuth2 (ORCID), JWT
- **Geospatial**: Shapely, H3, GeoJSON
- **Data Processing**: PETL, Pandas
- **Testing**: pytest (71% coverage minimum)
- **Dependency Management**: Poetry
- **Code Quality**: flake8, mypy, black

---

**Last Updated**: 2025-11-14
**Project Repository**: https://github.com/isamplesorg/isamples_inabox
