# Taxonomy classification

This README is to describe how to apply the classification model on the records.
For the detailed development process of the classification model, please refer to [here](https://github.com/isamplesorg/content-classification).

## Setup
1. Download the classification model <br/>
Download the classification model based on the collection type, label type from [here](https://drive.google.com/drive/folders/1FreG1_ivysxPMXH0httxw4Ihftx-R2N6) or you can access it from `/var/local/data/models` in mars. 
Unzip this folder into the root directory. You can name the folder up to your choice, such as naming it as `metadata_models` under `isamples_inabox`.
As result, you should have `sesar-material`, `opencontext-material`, `opencontext-sample` folder and `config.json` files under the `metadata_models`.

2. Match the config model location to its actual location.
Each model has its `config.json` that is used to load the model in our script. Make sure that `"FINE_TUNED_MODEL"` field of the config.json matches the actual location of the model directory. <br/>
If you are running locally, set as `"FINE_TUNED_MODEL":"/Users/.../isamples_inabox/metadata_models/opencontext-material"` 
If you are in a container, you should change the above value to point to the model directory in the container environment.
<br/>

3. Run the classification model on the records 
You can use a config file or command line arguments to run the classification script. The config file should contain db_url, solr_url, and authority_id (collection type) value. <br/>
e.g. :
```
python3 scripts/taxonomy/classification.py --config isb.cfg
```