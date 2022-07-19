# README for isamples_inabox

**Note:** This work is in active development and likely to change 
considerably, including split / merge to other repositories.

Provides implementation of scripts for harvesting content from 
repositories, placing the content in a postgres database, relations
in a Solr index, a Fast-API implementation for accessing the content 
and a simple UI for viewing. 

## Installation

### Python environment
Follow the instructions in [python_setup.md.html](docs/python_setup.md.html)

#### Python tooling
Before code is allowed to merge, it needs to pass flake8 and mypy checks.  You may follow the instructions on how to 
install and run these locally.  Additionally, you may run the black python formatter on new code to ensure the 
formatting matches the rest of the project.

### Tunneling postgres and solr configuration
If you don't want to worry about running everything locally, you can tunnel remote services with ssh.
For example, with these two ssh tunnels:
#### solr
```
ssh <username>@mars.cyverse.org -p 1657 -L8984:localhost:9983
```
#### postgres
```
ssh <username>@mars.cyverse.org -p 1657 -L6432:localhost:5432
```
we would end up with iSB config that looks like
```
db_url = "postgresql+psycopg2://isb_writer:<password>@localhost:6432/isb_1"
solr_url = "http://localhost:8984/solr/isb_core_records/"
```
Note that you'll want that config file to be named `isb_web_config.env` and located in the working directory of where 
you run whatever script you're working on. After that, run the script with 

```
poetry run python example_script.py --config isb_web_config.env
```

### Local postgres and solr configuration
If you'd prefer to run everything locally, follow this.

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

### Python virtual environment creation

Create a python virtual environment, checkout the source, and run poetry install.

e.g.:
```
mkvirtualenv isb-dev
git clone git@github.com:isamplesorg/isamples_inabox.git
cd isamples_inabox
git checkout -b origin/develop
poetry install
```

If running locally, create a database, e.g:
```
psql postgres
CREATE DATABASE isb_1;
CREATE USER isb_writer WITH ENCRYPTED PASSWORD 'some_password';
GRANT ALL PRIVILEGES ON DATABASE isb_1 TO isb_writer;
```

Create a config file, e.g.:
```
# cat isb.cfg
db_url = "postgresql+psycopg2://isb_writer:some_password@localhost/isb_1"
max_records = 1000
verbosity="INFO"
```

If running locally, create a solr collection `isb_core_records`:
```
solr create -c isb_core_records
```

Then run the schema creation script against your local solr instance:

```
python scripts/solr_schema_init/create_isb_core_schema.py
```

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


