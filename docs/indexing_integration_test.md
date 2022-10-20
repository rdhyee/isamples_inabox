# Setting up an integration test with a list of identifiers
## Prerequisites

1. A full database of iSamples content (running locally or on a server where it can be dumped -- like mars)
2. A running Docker container with the iSB stack.  There should be no data in the stack, but it should have the database and solr schema created.  Simply bringing up the container should be enough to create the schemas.

## DB Permissions
Note that the user that runs the script needs to be a superuser or have the pg_read_server_files, pg_write_server_files, or pg_execute_server_program role.  I tested the setup on hyde and the `isb_writer` account has the necessary privileges to execute the queries.

## Dumping the data
There's a script in `scripts/indexing_integration_test/manage_things_with_ids.py` that will dump the data to a specified file.  It takes as input a file with a list of identifiers formatted like the `sample_input_file` in the same directory.  You run it like this:

```
python manage_things_with_id.py -d "db_url" dump --input_file /tmp/source_ids --output_file /tmp/dumped_things
```
## Loading the data
After you've dumped the data, you may load it back up using the `load` command in the same script:

```
python manage_things_with_id.py -d "db_url" load --input_file /tmp/dumped_things
```

Note that you can't run this more than once on the same database or you'll get referential integrity errors.
Also note that the input file needs to exist in the container *where the database server is running*, but you 
run the script itself (and pass the file path argument) in the isb container.

## Running the indexer
Once the database is loaded back up, you can run the indexer to populate the solr index using the things db.  You run it (depending on the provider) like this:

```
/usr/local/bin/python scripts/opencontext_things.py --config ./isb.cfg populate_isb_core_solr -I
/usr/local/bin/python scripts/geome_things.py --config ./isb.cfg populate_isb_core_solr -I
/usr/local/bin/python scripts/sesar_things.py --config ./isb.cfg populate_isb_core_solr -I
/usr/local/bin/python scripts/smithsonian_things.py --config ./isb.cfg populate_isb_core_solr -I
```

Once it's done you should be able to hit solr using the local URL and query the data like usual.

## Running the unit test
After the container is up
`apt-get install emacs` (or your favorite editor in case you need to edit things)
`export INPUT_SOLR_URL="http://localhost:8000/thing/select"`
`pip3 install pytest`
`pytest test_solr_search_results.py -k "test_solr_integration_test"`