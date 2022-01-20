import requests
import logging


def getLogger():
    return logging.getLogger("isb_web")


SOLR_RESERVED_CHAR_LIST = [
    "+",
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "~",
    "*",
    "?",
    ":",
]


def escapeSolrQueryTerm(term):
    term = term.replace("\\", "\\\\")
    for c in SOLR_RESERVED_CHAR_LIST:
        term = term.replace(c, "\{}".format(c))  # noqa: W605 -- this is the correct way to escape SOLR
    return term


def getRelationsSolr(
    rsession: requests.Session,
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    url: str = "http://localhost:8983/solr/isb_rel/",
):
    q = []
    if s is not None:
        q.append(f"s:{escapeSolrQueryTerm(s)}")
    if p is not None:
        q.append(f"p:{escapeSolrQueryTerm(p)}")
    if o is not None:
        q.append(f"o:{escapeSolrQueryTerm(o)}")
    if source is not None:
        q.append(f"source:{escapeSolrQueryTerm(source)}")
    if name is not None:
        q.append(f"name:{escapeSolrQueryTerm(name)}")
    if len(q) == 0:
        q.append("*:*")
    headers = {"Accept": "application/json"}
    params = {
        "q": " AND ".join(q),
        "wt": "json",
        "rows": limit,
        "start": offset,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params).json()
    return res.get("response", {}).get("docs", [])


def getPredicateCountsSolr(
    rsession: requests.Session, url="http://localhost:8983/solr/isb_rel/"
):
    params = {"q": "*:*", "rows": "0", "facet": "true", "facet.field": "p"}
    headers = {"Accept": "application/json"}
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params).json()
    fc = res.get("facet_counts", {}).get("facet_fields", {}).get("p", [])
    result = []
    for i in range(0, len(fc), 2):
        entry = {"predicate": fc[i], "count": fc[i + 1]}
        result.append(entry)
    return result
