import typing
import requests
import isb_lib.core

# Timestamp from when we did the initial Smithsonian import
SMITHSONIAN_TIMESTAMP = "2021-07-23T14:08:46Z"


def smithsonian_fetch_solr_records(
    url: typing.AnyStr = "http://localhost:8983/solr/isb_core_records/", rows: int = 50000, rsession=requests.session()
) -> typing.List[typing.Dict]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": "(NOT sourceUpdatedTime:*)",
        "rows": rows,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    dict = res.json()
    docs = dict["response"]["docs"]
    return docs


def main():
    rsession = requests.session()
    while True:
        records = smithsonian_fetch_solr_records(rsession=rsession)
        if len(records) == 0:
            break
        records_to_update = []
        for record in records:
            record["sourceUpdatedTime"] = SMITHSONIAN_TIMESTAMP
            records_to_update.append(record)
        isb_lib.core.solrAddRecords(rsession, records_to_update, "http://localhost:8983/solr/isb_core_records/")
        isb_lib.core.solrCommit(rsession, "http://localhost:8983/solr/isb_core_records/")


if __name__ == "__main__":
    main()