# backends/sparql_mapper.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from typing import Dict

# Core prefix block to prepend when queries miss required prefixes
PREFIX_BLOCK = """PREFIX ex: <http://example.org/>
PREFIX schema: <https://schema.org/>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
PREFIX sf: <http://www.opengis.net/ont/sf#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

# Known neighborhood tokens (lowercased) for lightweight filtering/upgrades
NEIGHBORHOODS = ["syntagma", "plaka", "monastiraki", "kolonaki", "koukaki", "exarchia", "psyrri"]
NEIGHBORHOODS_RE = "|".join(NEIGHBORHOODS)

# Keep label-based locatedIn rewrite disabled (using explicit IRIs instead)
REWRITE_LOCATED_IN_TO_LABEL_FILTER = False

# Optional mapping from neighborhood token → full IRI (left empty by default)
NEIGHBOR_IRIS: Dict[str, str] = {}

# Property and class normalization maps (align LLM outputs to dataset schema)
PROPERTY_MAP: Dict[str, str] = {
    r"\bschema:priceRange\b": "ex:averagePricePerPerson",
    r"\bschema:price\b": "ex:averagePricePerPerson",
    r"\bprice\b": "ex:averagePricePerPerson",

    r"\bschema:hasWifi\b": "ex:hasWifi",
    r"\bwifi\b": "ex:hasWifi",

    r"\bschema:outdoorSeating\b": "ex:hasOutdoorSeating",
    r"\boutdoor\b": "ex:hasOutdoorSeating",

    r"\bschema:vegetarianFriendly\b": "ex:veggieFriendly",
    r"\bschema:veganFriendly\b": "ex:veggieFriendly",
    r"\bvegan\b": "ex:veggieFriendly",
    r"\bvegetarian\b": "ex:veggieFriendly",

    r"\bschema:noise\b": "ex:noiseLevel",
    r"\bnoise\b": "ex:noiseLevel",

    r"\bschema:accessibility\b": "ex:accessibility",
    r"\baccessible\b": "ex:accessibility",

    r"\bschema:alcohol\b": "schema:servesAlcohol",
    r"\balcohol\b": "schema:servesAlcohol",
    r"\bdrinks\b": "schema:servesAlcohol",

    r"\bschema:aggregateRating\b": "ex:avgRating",
    r"\bschema:rating\b": "ex:avgRating",
    r"\brating\b": "ex:avgRating",

    r"\bschema:name\b": "rdfs:label",
    r"\bex:name\b": "rdfs:label",
}

CLASS_MAP: Dict[str, str] = {
    r"\bschema:Cafe\b": "schema:CafeOrCoffeeShop",
    r"\bschema:CoffeeShop\b": "schema:CafeOrCoffeeShop",
    r"\blocal:Cafe\b": "schema:CafeOrCoffeeShop",
    r"\bkg:Cafe\b": "schema:CafeOrCoffeeShop",
    r"\bns:Cafe\b": "schema:CafeOrCoffeeShop",

    r"\bschema:Bar\b": "schema:BarOrPub",
    r"\blocal:Bar\b": "schema:BarOrPub",
    r"\bkg:Bar\b": "schema:BarOrPub",
    r"\bns:Bar\b": "schema:BarOrPub",

    r"\blocal:Restaurant\b": "schema:Restaurant",
    r"\bkg:Restaurant\b": "schema:Restaurant",
    r"\bns:Restaurant\b": "schema:Restaurant",
}

# Fix bare/unknown prefixes like ':Restaurant' into schema-qualified forms
PREFIX_FIXES: Dict[str, str] = {
    r"(?<![A-Za-z0-9_])\:Restaurant\b": "schema:Restaurant",
    r"(?<![A-Za-z0-9_])\:Cafe\b": "schema:CafeOrCoffeeShop",
    r"(?<![A-Za-z0-9_])\:Bar\b": "schema:BarOrPub",
    r"(?<![A-Za-z0-9_])\:Place\b": "schema:Place",
}

# Markdown fence stripper for ```sparql blocks
FENCE_RE = re.compile(r"^```[a-zA-Z]*\s*|\s*```$", re.MULTILINE)

def _strip_md_fences(text: str) -> str:
    """Remove markdown code fences from LLM output."""
    return FENCE_RE.sub("", text or "").strip()

def _apply_map(s: str, mapping: Dict[str, str]) -> str:
    """Apply a dictionary of regex → replacement across the string."""
    for pat, repl in mapping.items():
        s = re.sub(pat, repl, s)
    return s

def _upgrade_known_neighborhoods_to_iris(s: str) -> str:
    """Swap bare neighborhood tokens for configured IRIs when available."""
    for word, iri in NEIGHBOR_IRIS.items():
        s = re.sub(rf'(?<![<":?A-Za-z0-9_]){re.escape(word)}(?![>":A-Za-z0-9_])', iri, s)
    return s

def _quote_bareword_objects(s: str) -> str:
    """Quote plain-object tokens that are not vars/IRIs/prefixed names/numbers/booleans."""
    triple_re = re.compile(r"(\S+)\s+(\S+)\s+([^.;{}]+)\s*\.", flags=re.MULTILINE)
    def repl(m):
        subj, pred, obj = m.group(1), m.group(2), m.group(3).strip()
        if (
            obj.startswith("?") or obj.startswith("<") or obj.startswith('"')
            or ":" in obj
            or re.match(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$", obj)
            or obj.lower() in ("true", "false")
        ):
            return m.group(0)
        return f'{subj} {pred} "{obj}" .'
    return triple_re.sub(repl, s)

def _rewrite_located_in_athens_to_label_filter(s: str) -> str:
    """Optional rewrite: locatedIn 'Athens' → label regex filter over known neighborhoods."""
    if not REWRITE_LOCATED_IN_TO_LABEL_FILTER:
        return s
    pat = re.compile(r'(\?\w+)\s+(?:kg|local|ns):locatedIn\s+"Athens"\s*\.\s*', flags=re.IGNORECASE)
    idx = 0
    def repl(m):
        nonlocal idx
        var = m.group(1); lbl = f"?_label{idx}"; idx += 1
        return f"{var} rdfs:label {lbl} . FILTER(REGEX(LCASE({lbl}), \"{NEIGHBORHOODS_RE}\")) "
    return pat.sub(repl, s)

def ensure_prefixes_all(s: str) -> str:
    """Prepend the standard prefix block when required prefixes are missing."""
    required = ("PREFIX ex:", "PREFIX schema:", "PREFIX rdfs:")
    if all(p in s for p in required):
        return s
    return PREFIX_BLOCK + "\n" + s

def map_sparql_query(raw: str) -> str:
    """Normalize LLM SPARQL: strip fences, align schema, fix prefixes, and quote bare objects."""
    if not raw:
        return raw
    s = _strip_md_fences(raw)
    s = _apply_map(s, CLASS_MAP)
    s = _apply_map(s, PROPERTY_MAP)
    s = _apply_map(s, PREFIX_FIXES)
    s = _rewrite_located_in_athens_to_label_filter(s)
    s = _upgrade_known_neighborhoods_to_iris(s)
    s = _quote_bareword_objects(s)
    return s
