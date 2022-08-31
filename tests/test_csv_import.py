import pytest
from frictionless import validate, Resource
import json

from isb_lib.data_import import csv_import

CSV_items = [
    (
        "./test_data/isb_core_csv_documents/simple_isamples.tsv",
        "./test_data/isb_core_csv_documents/simple_isamples.json"
    ),
    (
        "./test_data/isb_core_csv_documents/simple_isamples.csv",
        "./test_data/isb_core_csv_documents/simple_isamples.json"
    ),
    (
        "./test_data/isb_core_csv_documents/simple_isamples.xls",
        "./test_data/isb_core_csv_documents/simple_isamples.json"
    ),
    (
        "./test_data/isb_core_csv_documents/simple_isamples.xlsx",
        "./test_data/isb_core_csv_documents/simple_isamples.json"
    )
]


@pytest.mark.parametrize("csv_file_path", CSV_items)
def test_load_csv(csv_file_path: tuple):
    records = csv_import.create_isamples_package(csv_file_path[0])
    report = validate(records.to_dict(), type="package")
    assert report.valid
    first_resource: Resource = records.resources[0]
    for row in first_resource:
        if row is None:
            break
        print(f"row is {row}")
        print(f"row errors are {row.errors}")
        assert row.errors is None or len(row.errors) == 0


def _check_serialized_json_dict(dict1: dict, dict2: dict):
    """Simple method that will try to serialize non-str to str in order to compare against a serialized JSON dict"""
    for k, v in dict1.items():
        if type(v) is dict:
            _check_serialized_json_dict(v, dict2[k])
        else:
            assert str(v) == str(dict2[k])


@pytest.mark.parametrize("csv_file_path,json_file_path", CSV_items)
def test_unflatten_csv_row(csv_file_path: str, json_file_path: str):
    records = csv_import.create_isamples_package(csv_file_path)
    json_contents = json_dict_from_file_path(json_file_path)
    first_resource: Resource = records.resources[0]
    row = iter(first_resource).__next__()
    unflattened_row = csv_import.unflatten_csv_row(row)
    assert unflattened_row is not None
    _check_serialized_json_dict(unflattened_row, json_contents)


def json_dict_from_file_path(json_file_path: str) -> dict:
    with open(json_file_path) as json_file:
        json_str = json_file.read()
        json_contents = json.loads(json_str)
    return json_contents


@pytest.mark.parametrize("csv_file_path,json_file_path", CSV_items)
def test_isb_core_dicts_from_isamples_package(csv_file_path: str, json_file_path: str):
    records = csv_import.create_isamples_package(csv_file_path)
    isb_core_dicts = csv_import.isb_core_dicts_from_isamples_package(records)
    assert len(isb_core_dicts) == 2
    json_dict = json_dict_from_file_path(json_file_path)
    _check_serialized_json_dict(isb_core_dicts[0], json_dict)
