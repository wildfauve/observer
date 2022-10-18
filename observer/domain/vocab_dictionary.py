from typing import Dict, Tuple, Optional

from observer.util import fn

"""
This is a vocab mapping module.  The vocab is laid out in a tree structure aligned, at the root to each of the column-level concepts
within the CBOR.  The branches are specific routes into the concept.  There is a special branch defined as "*" which is used for concepts
whose vocab is common across concepts.

Use the term_for function to determine the name in the table for all nodes in a tree.  For example, the following:

> StructField(V.term_for("*.fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasAmount"), DecimalType(20, 6), True),

defines a dataframe structured field for the leaf node fibo-fnd-acc-cur:hasPrice.fibo-fnd-acc-cur:hasAmount.  That maps to 
{'hasDataProductTerm': "amount"}, which means that the name of that property in the structure will be "amount".  Where as the tree path 
helps us understand the Ontology path to the concept.  

"""

vocab = {
    "": {},
    "run": {
        "hasDataProductTerm": "run",
        "sfo-lin:hasRunTime": {"hasDataProductTerm": "hasRunTime"},
        "sfo-lin:isRunOf": {"hasDataProductTerm": "isRunOf"},
        "sfo-lin:hasTrace": {"hasDataProductTerm": "hasTrace"},
        "sfo-lin:hasStartTime": {"hasDataProductTerm": "hasStartTime"},
        "sfo-lin:hasEndTime": {"hasDataProductTerm": "hasEndTime"},
        "sfo-lin:hasRunState": {"hasDataProductTerm": "hasRunState"},
        "sfo-lin:hasInputs": {
            "hasDataProductTerm": "hasInputs",
            "sfo-lin:hasLocation": {"hasDataProductTerm": "hasLocation"},
            "sfo-lin:hasName": {"hasDataProductTerm": "hasName"}
        },
        "sfo-lin:hasOutputs": {
            "hasDataProductTerm": "hasOutputs",
            "sfo-lin:hasLocation": {"hasDataProductTerm": "hasLocation"},
            "sfo-lin:hasName": {"hasDataProductTerm": "hasName"}
        },
        "sfo-lin:hasMetrics": {
            "hasDataProductTerm": "hasMetrics"
        }
    },
    "*": {
        "@id": {
            'hasDataProductTerm': "id"
        },
        "@type": {
            'hasDataProductTerm': "type"
        },
        "lcc-lr:hasTag": {
            'hasDataProductTerm': "label",
            'hasMeta': {'term': "lcc-lr:hasTag"}
        }
    }

}


def term_for(path: str) -> str:
    _path_array, term = term_finder(path)
    return get_term(term)


def meta_for(path: str) -> Dict:
    path_array, term = term_finder(path)
    return get_meta(term)


def term_and_meta(path: str) -> Tuple[Optional[str]]:
    _path_array, term = term_finder(path)
    return get_term(term), get_meta(term)


def get_term(term: str) -> Optional[str]:
    if not term or not 'hasDataProductTerm' in term.keys():
        breakpoint()
    return term.get('hasDataProductTerm', None)


def get_meta(term: str) -> Optional[str]:
    if not term or not 'hasMeta' in term.keys():
        return {}
    return term.get('hasMeta', None)


def term_finder(path):
    path_array = path.split(".")
    term = fn.deep_get(vocab, path_array)
    if not term:
        breakpoint()
    return path_array, term
