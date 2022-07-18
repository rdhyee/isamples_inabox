import click
import typing
import rdflib
import logging
import json

logging.basicConfig(level=logging.INFO)
L = logging.getLogger("")


MAPPING = {
    "http://www.w3.org/2004/02/skos/core": {
        "uri": "http://www.w3.org/2009/08/skos-reference/skos.rdf",
        "format": "xml"
    }
}


NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "obo": "http://purl.obolibrary.org/obo/",
    "geosciml": "http://resource.geosciml.org/classifier/cgi/lithology"
}


# Prefix text common for the SPARQL queries
PFX = """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""


# https://rdflib.readthedocs.io/en/stable/intro_to_parsing.html
# "turtle" is "text/turtle" media type
REQUEST_FORMAT = "turtle"


class OwlImporter:
    '''Implements a graph loader that recursively follows owl:imports.
    '''

    def __init__(self, mapping: dict = {}, default_format: str = "turtle"):
        self.mapping = mapping
        self.default_format = default_format
        self._visited: typing.List[str] = []

    def resolve_uri(self, uri: typing.Union[str, rdflib.URIRef]):
        '''Resolve URI to a graph.
        '''
        uri = str(uri)
        g = rdflib.ConjunctiveGraph()
        if uri in self._visited:
            return g
        # get URL from mapping
        # defaulting to the uri if a mapping entry is not available
        url = self.mapping.get(uri, {}).get("uri", uri)
        # get the format for the document, defaulting to default_format
        _format = self.mapping.get(uri, {}).get("format", self.default_format)
        try:
            g.parse(url, format=_format)
            self._visited.append(uri)
        except Exception as e:
            L.error(f"Unable to parse {uri} ({e})")
        return g

    def load(
        self,
        uri: str,
        follow_imports: bool = True
    ):
        '''Load a graph from uri. If follow_imports, then
        owl:imports are followed recursively.
        '''
        try:
            g = self.resolve_uri(uri)
            for import_uri in g.objects(predicate=rdflib.OWL.imports):
                L.debug(f"IMPORT: {import_uri} FOR {uri}")
                gres = self.load(import_uri)
                if gres is not None:
                    g += gres
            return g
        except Exception as e:
            L.error(f"Could not resolve {uri} ({e})")


def loadGraph(url):
    owler = OwlImporter(mapping=MAPPING, default_format=REQUEST_FORMAT)
    g = owler.load(url)
    # bind some namespace prefixes for convenience
    for k, v in NS.items():
        g.bind(k, v)
    return g


def skosT(term):
    # Create a skos:term
    return rdflib.URIRef(f"{NS['skos']}{term}")


def rdfT(term):
    return rdflib.URIRef(f"{NS['rdf']}{term}")


def rdfsT(term):
    return rdflib.URIRef(f"{NS['rdfs']}{term}")


def listVocabularies(g):
    '''List the vocabularies in the provided graph
    '''
    q = PFX + """SELECT ?s
    WHERE {
        ?s rdf:type skos:ConceptScheme.
    }"""
    qres = g.query(q)
    res = []
    for r in qres:
        res.append(r[0])
    return res


def getVocabularyRoot(g, v):
    """Get top concept of the vocabulary v in graph g
    """
    q = rdflib.plugins.sparql.prepareQuery(PFX + """SELECT ?s
    WHERE {
        ?s skos:topConceptOf ?vocabulary .
    }""")
    qres = g.query(q, initBindings={'vocabulary': v})
    res = []
    for row in qres:
        res.append(row[0])
    return res


def getNarrowerV(g, v, r):
    """Return list of narrower terms of r within skos vocabulary v of the graph g
    """
    q = rdflib.plugins.sparql.prepareQuery(PFX + """SELECT ?s
    WHERE {
        ?s skos:inScheme ?vocabulary .
        ?s skos:broader ?parent .
    }""")
    qres = g.query(q, initBindings={'vocabulary': v, 'parent': r})
    res = []
    for row in qres:
        res.append(row[0])
    return res


def getNarrower(g, r):
    """Return list of narrower terms of r within the graph g
    """
    q = rdflib.plugins.sparql.prepareQuery(PFX + """SELECT ?s
    WHERE {
        ?s skos:broader ?parent .
    }""")
    qres = g.query(q, initBindings={'parent': r})
    res = []
    for row in qres:
        res.append(row[0])
    return res


def getObjects(g, s, p):
    """Return objects from graph g with subject s and predicate p
    """
    q = rdflib.plugins.sparql.prepareQuery(PFX + """SELECT ?o
    WHERE {
        ?subject ?predicate ?o .
    }""")
    qres = g.query(q, initBindings={'subject': s, 'predicate': p})
    res = []
    for row in qres:
        res.append(row[0])
    return res


def getLabel(g, s):
    '''Return a label for the term s.

    First try for skos:prefLabel, then skos:label, and falling back to
    the value at the end of the term URI
    '''
    try:
        label = getObjects(g, s, skosT("prefLabel"))
        return label[0]
    except Exception:
        pass
    try:
        label = getObjects(g, s, skosT("label"))
        return label[0]
    except Exception:
        pass
    parts = g.qname(s).split(":")
    return parts[-1]


def termTree(g, v, r, depth=0):
    """Iterator yielding text rendering of narrower terms starting
    with term r of the vocabulary v within graph g

    Each row is label + qname indented according to depth
    """
    label = getLabel(g, r)
    res = f"{'    '*depth}- {label} ({g.qname(r)})"
    yield res
    for term in getNarrower(g, r):
        for res in termTree(g, v, term, depth=depth + 1):
            yield res


def termTreeJSON(g, v, r, depth=0):
    """Iterator yielding label object rendering of narrower terms starting
    with term r of the vocabulary v within graph g

    """
    label = getLabel(g, r)

    # defaults to en if language not provided
    lang = label.language if isinstance(label, rdflib.term.Literal) else "en"
    label = str(label).strip()
    identifier = str(r).strip()

    obj = {}
    obj[identifier] = {}
    obj[identifier]["label"] = {lang: label}
    obj[identifier]["children"] = []

    for term in getNarrower(g, r):
        child_identifier, child_obj = termTreeJSON(g, v, term, depth=depth + 1)
        obj[identifier]["children"].append({child_identifier: child_obj})
    return identifier, obj[identifier]  # return key, value


def getUrl(labelType):
    """Return the url to fetch vocabulary"""
    W3ID_BASE_URL = "https://w3id.org/isample/vocabulary/"
    GITHUB_BASE_URL = "https://raw.githubusercontent.com/"
    META_PARAM = "isamplesorg/metadata/develop/src/vocabularies/"

    MATERIAL_URL = W3ID_BASE_URL + "material/0.9/materialsvocabulary"
    SAMPLED_FEATURE_URL = GITHUB_BASE_URL + META_PARAM + "sampledFeature.ttl"
    SPECIMEN_TYPE_URL = GITHUB_BASE_URL + META_PARAM + "specimenType.ttl"
    OC_MATERIAL_URL = GITHUB_BASE_URL + META_PARAM + "OpenContextMaterial.ttl"

    url = None
    if labelType == "material":
        url = MATERIAL_URL
    elif labelType == "specimen":
        url = SPECIMEN_TYPE_URL
    elif labelType == "context":
        url = SAMPLED_FEATURE_URL
    elif labelType == "contextMaterial":
        url = OC_MATERIAL_URL

    return url


def createJSON(url):
    """Return json string given the vocabulary source file
    """

    g = loadGraph(url)
    vocabs = listVocabularies(g)
    for vocab in vocabs:
        roots = getVocabularyRoot(g, vocab)
        for root in roots:
            key, value = termTreeJSON(g, vocab, root)
    data = {}
    data[key] = value
    json_data = json.dumps(data, indent=4)

    return json_data


def updateMapping(g, v, r, parent, label_to_full):
    """Recursively gets the full hierarchy of a label
    that includes the broader terms
    and adds that to the label_to_full mapping
    e.g.) Frozen Water ==> Material>Any ice>Frozen water
    """

    label = getLabel(g, r)
    label = str(label).strip()  # remove white space

    full_hierarchy = parent + ">" + label if len(parent) > 0 else label
    label_to_full[label] = full_hierarchy

    for term in getNarrower(g, r):
        # for each child node, update the mapping
        updateMapping(g, v, term, full_hierarchy, label_to_full)


def getHierarchyMapping(labelType):
    """" For the label type, returns the hierarchy mapping
    """
    url = getUrl(labelType)

    label_to_full = {}  # initialize hierarchy mapping
    g = loadGraph(url)
    vocabs = listVocabularies(g)
    for vocab in vocabs:
        print(f"Vocabulary: {vocab}")
        roots = getVocabularyRoot(g, vocab)
        for root in roots:
            print(f"Root: {root}")
            updateMapping(g, vocab, root, "", label_to_full)

    # for key, value in label_to_full.items():
    #     print(key,"\t",value)

    return label_to_full


def getFullLabel(label, mapping):
    """Returns the full hierarchy of the given label"""
    label = label[0]
    if label in mapping:
        return mapping[label]
    else:
        return "Not Provided"


@click.command()
@click.argument('url')  # url of vocabulary source file
def main(url: str):

    json = createJSON(url)
    print(json)


if __name__ == "__main__":
    main()
