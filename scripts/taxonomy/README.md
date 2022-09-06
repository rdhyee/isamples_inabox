# Taxonomy classification

This README is to describe how to apply the classification model on the records.
For the detailed development process of the classification model, please refer to [here](https://github.com/isamplesorg/content-classification).

## Setup
1. Download the classification model <br/>
Download the classification model based on the collection type, label type from [here](https://drive.google.com/drive/folders/1FreG1_ivysxPMXH0httxw4Ihftx-R2N6). Unzip this folder into the `assets` folder. You can name the folder up to your choice,
but it should match the `"FINE_TUNED_MODEL"` value of the config file in the `assets` folder.
<br/><br/>
2. Run the classification model on the records <br/>
You can use a config file or command line arguments to run the classification script. The config file should contain
db_url, solr_url, and authority_id (collection type) value. <br/>
e.g. :
```
python3 scripts/taxonomy/classification.py --config isb.cfg
```