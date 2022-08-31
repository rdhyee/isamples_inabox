Example of various data types in a CSV file for testing frictionless.io validation.

Validate with:
```
$ frictionless validate data
```

It should show a validation error for row 4 with an incorrect controlled value for field "vocabval".

Requires frictionless `datapackage` installed, e.g.:

```
pip install datapackage
```

See:

- https://frictionlessdata.io/introduction/
- https://libraries.frictionlessdata.io/docs/table-schema/python
- https://libraries.frictionlessdata.io/docs/data-package/python
