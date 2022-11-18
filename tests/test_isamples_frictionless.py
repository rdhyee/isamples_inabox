import isamples_frictionless
import csv


def test_insert_identifiers_into_template():
    test_identifiers = ["123", "456"]
    formatted_csv = isamples_frictionless.insert_identifiers_into_template(test_identifiers)
    assert formatted_csv is not None
    test_identifiers.append("id")
    for row in csv.reader(formatted_csv.splitlines()):
        assert row is not None
        print(f"row is {row}")
        assert row[0] in test_identifiers
