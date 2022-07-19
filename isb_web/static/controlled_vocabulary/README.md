# Static Controlled Vocabulary Hierarchy Files
How to generate static controlled hierarchy files, [material_hierarchy.json](./material_hierarchy.json), [sampledFeature_hierarchy.json](./sampledFeature_hierarchy.json), and [specimenType_hierarchy.json](./specimenType_hierarchy.json).

## Required Dependencies
```
click
rdflib
logging
```

## Scripts
The python script: [create_hierarchy_json.py](../../../scripts/taxonomy/create_hierarchy_json.py)
```
# Generate material_hierarchy.json
python scripts/taxonomy/create_hierarchy_json.py https://w3id.org/isample/vocabulary/material/0.9/materialsvocabulary > isb_web/static/controlled_vocabulary/material_hierarchy.json

# Generate sampledFeature_hierarchy.json
python scripts/taxonomy/create_hierarchy_json.py https://raw.githubusercontent.com/isamplesorg/metadata/develop/src/vocabularies/sampledFeature.ttl > isb_web/static/controlled_vocabulary/sampledFeature_hierarchy.json

# Generate specimenType_hierarchy.json
python scripts/taxonomy/create_hierarchy_json.py https://raw.githubusercontent.com/isamplesorg/metadata/develop/src/vocabularies/specimenType.ttl > isb_web/static/controlled_vocabulary/specimenType_hierarchy.json
```
