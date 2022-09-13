# Running tests and code coverage locally
The python unit tests in this directory are all written and run using [PyTest](https://pytest.org).
You can run them locally using an IDE like PyCharm, or run them on the command line with PyCharm.

## Running on the command-line
You'll want to make sure your python virtual environment is activated, and the other thing you need to 
set is your `PYTHONPATH` to point at the local checkout, like this:

```
export PYTONPATH=../
```

Then, you can just run `pytest` from the root of this directory.

## Running coverage locally

If you want to run coverage locally, there are a couple of cmd-line switches to get going.  You'll run mostly
the same way, but add these switches:

```
pytest --cov --cov-fail-under=68 --cov-report html
```

This will fail the tests if the code coverage isn't greater than the number specified by `--cov-fail-under`.  To 
look at the report, do something like `open htmlcov/index.html` and look at the results.  For full documentation on
the coverage switches, have a look at [pytest-cov](https://pytest-cov.readthedocs.io/en/latest/).