# README for isamples_inabox

**Note:** This work is in active development and likely to change 
considerably, including split / merge to other repositories.

Provides implementation of scripts for harvesting content from 
repositories, placing the content in a postgres database, relations
in a Solr index, a Fast-API implementation for accessing the content 
and a simple UI for viewing. 

## Installation

Install Postgres and Solr, e.g.:
```
brew install postgres
brew install solr
```

Edit solr configuration to start in cloud mode:

edit `/usr/local/Cellar/solr/8.8.2/homebrew.mxcl.solr.plist` and add:
```
<string>-c</string>  
```
under `<key>ProgramArguments</key>`

Start Solr and postgres:
```
brew services start postgres
brew services start solr
```

Create a python virtual environment, checkout the source, and run poetry install.

e.g.:
```
mkvirtualenv isb-dev
git clone git@github.com:isamplesorg/isamples_inabox.git
cd isamples_inabox
git checkout -b origin/develop
poetry install
```

Create a database, e.g:
```
psql postgres
CREATE DATABASE isb_1;
CREATE USERr isb_writer WITH ENCRYPTED PASSWORD 'some_password';
GRANT ALL PRIVILEGES ON DATABASE isb_1 TO isb_writer;
```

Create a config file, e.g.:
```
# cat isb.cfg
db_url = "postgresql+psycopg2://isb_writer:some_password@localhost/isb_1"
max_records = 1000
verbosity="INFO"
```

Create a solr collection `isb_rel`:
```
solr create -c isb_rel
```

Adjust the solr schema. This bit is hacky - open `notes/solr_manage.ipynb` 
and run the blocks down to the one with the `createField` calls. 

## Operation

Populate the database with 5000 SESAR records:

```
workon isb-dev  #activate virtual environment if necessary
sesar_things --config isb.cfg load -m 5000
```

Populate the solr index with relations in those SESAR records:
```
sesar_things --config isb.cfg relations
```

## Web service

Run the fastAPI server in dev mode like:
```
workon isb-dev  #activate virtual environment if necessary
python isb_web/main.py
```

Navigate to http://localhost:8000/


