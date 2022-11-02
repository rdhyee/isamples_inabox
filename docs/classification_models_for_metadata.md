# Classification models for metadata enhancement 

To improve the metadata of the collections, machine learning models are used to predict the missing field values in records.  

## Development Process 
The process of integrating machine learning models is as follows:
1. Use mappings to map the source labels of a collection to a higher level of labels, which in our case is the iSamples controlled vocabularies.
2. Train a BERT model on the mapped records from 1. 
3. (SESAR) After experts' feedback, improve model performance and add rules to integrate expert knowledge in the classification. 
4. Integrate the classification. The code that does the classification and loads the trained models are in `isamples_metadata/taxonomy`. 
5. Check if the reindexing works as expected by using test records in `integration_tests`. 

## Mappings 
The mappings that we used to reformat the data is in this [spreadsheet](https://docs.google.com/spreadsheets/d/17p_q8XEOcAGDEwzj_hU-q8aUuNMT6Q3nTirQiCgGVR4/edit#gid=0). This spreadhseet tracks the current progress of metadata (in specific the controlled vocabularies) for each collection and vocabulary type.

## Download machine learning models 
We currently have machine learning models for the below collection and vocabulary: 
Collection    | Vocabulary
------------- | -------------
SESAR         | material
OpenContext   | material
OpenContext   | materialSample

In order to download the machine learning models, 
```
scp -r -P 1657 username@mars.cyverse.org:/var/local/data/models
```
and unzip it into the root directory under `metadata_models`. You should have a folder, and config.json file for each model. 

## Training process of machine learning models
The detailed process of training and testing the machine learning models is in [this respository](https://github.com/isamplesorg/content-classification). 
