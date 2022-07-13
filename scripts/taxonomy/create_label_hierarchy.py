import typing
import rdflib
import logging
import json

logging.basicConfig(level=logging.INFO)
L = logging.getLogger("")


MAPPING = {
    "http://www.w3.org/2004/02/skos/core": {
        "uri":"http://www.w3.org/2009/08/skos-reference/skos.rdf",
        "format":"xml"
    } 
}


NS = {
    "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#", 
    "rdfs":"http://www.w3.org/2000/01/rdf-schema#",
    "owl":"http://www.w3.org/2002/07/owl#",
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
    
    def __init__(self, mapping:dict={}, default_format:str="turtle"):
        self.mapping = mapping
        self.default_format = default_format
        self._visited = []
    
    def resolve_uri(self, uri:typing.Union[str, rdflib.URIRef]):
        '''Resolve URI to a graph.     
        '''
        uri = str(uri)
        g = rdflib.ConjunctiveGraph()
        if uri in self._visited:
            return g
        # get URL from mapping, defaulting to the uri if a mapping entry is not available
        url = self.mapping.get(uri,{}).get("uri", uri)
        # get the format for the document, defaulting to default_format
        _format = self.mapping.get(uri,{}).get("format", self.default_format)
        try:
            g.parse(url, format=_format)
            self._visited.append(uri)
        except Exception as e:
            L.error(f"Unable to parse {uri} ({e})")
        return g
        
    def load(
        self, 
        uri:str, 
        follow_imports:bool=True
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
    for k,v in NS.items():
        g.bind(k,v)
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
    qres = g.query(q, initBindings={'vocabulary': v, 'parent':r})
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
    qres = g.query(q, initBindings={'parent':r})
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
    qres = g.query(q, initBindings={'subject':s, 'predicate':p})
    res = []
    for row in qres:
        res.append(row[0])
    return res

def extractLanguage(s):
    '''Return a label for the term s.
    
    First try for skos:prefLabel, then skos:label, and falling back to
    the value at the end of the term URI
    '''
    try:
        return 
    except Exception as e:
        pass
    try:
        label = getObjects(g, s, skosT("label"))
        return label[0]
    except Exception as e:
        pass
    parts = g.qname(s).split(":")
    return parts[-1]


def getLabel(g, s):
    '''Return a label for the term s.
    
    First try for skos:prefLabel, then skos:label, and falling back to
    the value at the end of the term URI
    '''
    try:
        label = getObjects(g, s, skosT("prefLabel"))
        return label[0]
    except Exception as e:
        pass
    try:
        label = getObjects(g, s, skosT("label"))
        return label[0]
    except Exception as e:
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
        for res in termTree(g, v, term, depth=depth+1):
            yield res


def termTreeJSON(g, v, r, depth=0):
    """Iterator yielding label object rendering of narrower terms starting 
    with term r of the vocabulary v within graph g
    
    """
    label = getLabel(g,r)
    lang = label.language if isinstance(label, rdflib.term.Literal) else "en" # defaults to en if language not provided

    label = str(label).strip()
    identifier = str(r).strip()

    obj={}
    obj[identifier] = {}
    obj[identifier]["label"] = {lang: label}
    obj[identifier]["children"]= []

    for term in getNarrower(g, r):
        child_identifier,child_obj = termTreeJSON(g, v, term, depth=depth+1)
        obj[identifier]["children"].append({child_identifier:child_obj})
    return identifier, obj[identifier] # return key, value 


def createJSON(url):
    """Return json string given the vocabulary source file
    """

    g = loadGraph(url)
    vocabs = listVocabularies(g)
    for vocab in vocabs:
        roots = getVocabularyRoot(g, vocab)
        for root in roots:
            key, value= termTreeJSON(g, vocab, root)
    data = {}
    data[key] = value
    json_data = json.dumps(data, indent=4)

    return json_data


def main():
    material_url = "https://w3id.org/isample/vocabulary/material/0.9/materialsvocabulary"
    sampledFeature_url = "https://raw.githubusercontent.com/isamplesorg/metadata/develop/src/vocabularies/sampledFeature.ttl"
    openContextMaterial_url = "https://raw.githubusercontent.com/isamplesorg/metadata/develop/src/vocabularies/OpenContextMaterial.ttl"
    specimenType_url = "https://raw.githubusercontent.com/isamplesorg/metadata/develop/src/vocabularies/specimenType.ttl"

    material_hierarchy = createJSON(material_url)
    sampledFeature_hierarchy = createJSON(sampledFeature_url)
    openContextMaterial_hierarchy = createJSON(openContextMaterial_url)
    specimenType_hierarchy = createJSON(specimenType_url)

    print(material_hierarchy)
    print(sampledFeature_hierarchy)
    print(openContextMaterial_hierarchy)
    print(specimenType_hierarchy)
    

if __name__ == "__main__":
    main()