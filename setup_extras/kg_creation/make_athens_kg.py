# make_athens_kg.py — synthetic Athens KG (TTL) generator
import random
from pathlib import Path
from datetime import time
import math

# config
TOTAL = 1200
SEED  = 7
OUT   = Path("athens_large.ttl")
BASE_IRI = "http://example.org/"

# neighborhoods (label, lon, lat)
NEIGHBORHOODS = [
    ("Syntagma",     23.7347, 37.9755),
    ("Plaka",        23.7285, 37.9748),
    ("Monastiraki",  23.7247, 37.9763),
    ("Kolonaki",     23.7422, 37.9794),
    ("Koukaki",      23.7259, 37.9658),
    ("Exarchia",     23.7337, 37.9898),
    ("Psyrri",       23.7262, 37.9786),
]

# place types (schema.org)
TYPES = [
    ("Restaurant", "schema:Restaurant"),
    ("Cafe",       "schema:CafeOrCoffeeShop"),
    ("Bar",        "schema:BarOrPub"),
]

# attributes (cuisine/payment/noise)
CUISINES = [
    "Greek", "Mediterranean", "Italian", "Sushi", "Asian Fusion",
    "Burgers", "Vegan", "Middle Eastern", "Mexican", "Pizza", "Coffee"
]
PAYMENT_SETS = [
    "cash, visa, mastercard",
    "cash, visa",
    "visa, mastercard, amex",
    "cash only",
]
NOISE_LEVELS = ["quiet", "moderate", "loud"]

random.seed(SEED)  # deterministic runs

# TTL prefixes + ontology stubs
TTL_PREFIX = """@prefix ex:        <http://example.org/> .
@prefix schema:    <https://schema.org/> .
@prefix geo:       <http://www.w3.org/2003/01/geo/wgs84_pos#> .
@prefix geosparql: <http://www.opengis.net/ont/geosparql#> .
@prefix sf:        <http://www.opengis.net/ont/sf#> .
@prefix rdfs:      <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:       <http://www.w3.org/2001/XMLSchema#> .
@prefix owl:       <http://www.w3.org/2002/07/owl#> .

# ---- Object property for containment ----
ex:locatedIn a owl:ObjectProperty , owl:TransitiveProperty ;
  rdfs:domain schema:Place ;
  rdfs:range  schema:Place ;
  rdfs:label "located in"@en .

# ---- City node ----
ex:Athens a schema:Place ;
  rdfs:label "Athens" ;
  geosparql:hasGeometry [ a sf:Point ;
    geosparql:asWKT "POINT(23.727539 37.983810)"^^geosparql:wktLiteral ] .

"""

# HQ anchor
HQ_BLOCK = """ex:HQ a schema:Place ;
  rdfs:label "Company HQ (Syntagma)" ;
  geosparql:hasGeometry [ a sf:Point ;
    geosparql:asWKT "POINT(23.7347 37.9755)"^^geosparql:wktLiteral ] ;
  ex:locatedIn ex:Athens .
"""

def jitter_coord(lon, lat, max_offset_m=1200):
    """Jitter lon/lat by up to ~max_offset_m meters for variety."""
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = meters_per_deg_lat * abs(math.cos(lat * math.pi/180.0))
    dlon = (random.uniform(-max_offset_m, max_offset_m) / meters_per_deg_lon)
    dlat = (random.uniform(-max_offset_m, max_offset_m) / meters_per_deg_lat)
    return lon + dlon, lat + dlat

def rand_open_close():
    # random open/close hours per day
    open_h = random.randint(7, 12)
    close_h = random.randint(17, 23)
    if close_h <= open_h:
        close_h = min(open_h + random.randint(6, 12), 23)
    return time(open_h, random.randint(0,59)), time(close_h, random.randint(0,59))

def opening_hours_block():
    # 7-day OpeningHoursSpecification
    opens, closes = rand_open_close()
    day_iris = [
        "<https://schema.org/Monday>", "<https://schema.org/Tuesday>", "<https://schema.org/Wednesday>",
        "<https://schema.org/Thursday>", "<https://schema.org/Friday>", "<https://schema.org/Saturday>",
        "<https://schema.org/Sunday>"
    ]
    blocks = []
    for d in day_iris:
        blocks.append(f"""[ a schema:OpeningHoursSpecification ;
      schema:dayOfWeek {d} ;
      schema:opens "{opens.strftime('%H:%M:%S')}"^^xsd:time ;
      schema:closes "{closes.strftime('%H:%M:%S')}"^^xsd:time ]""")
    return ",\n    ".join(blocks)

def place_block(idx: int) -> str:
    # one POI with geometry/labels/attributes
    hood_name, hood_lon, hood_lat = random.choice(NEIGHBORHOODS)
    lon, lat = jitter_coord(hood_lon, hood_lat, 1200)
    kind_name, kind_curie = random.choice(TYPES)

    label = f"{hood_name} {kind_name} {idx}"
    address = f"{hood_name}, Athens"
    cuisine = random.choice(CUISINES)

    # price by type
    if kind_name == "Restaurant":
        price = round(random.uniform(12.0, 45.0), 2)
    elif kind_name == "Bar":
        price = round(random.uniform(8.0, 20.0), 2)
    else:  # Cafe
        price = round(random.uniform(3.0, 15.0), 2)

    has_outdoor = random.choice([True, False])
    has_wifi    = random.choice([True, False, True])   # bias to True
    veggie      = random.choice([True, False, False])  # bias to False
    accepts_res = random.choice([True, False, True])   # bias to True
    pays        = random.choice(PAYMENT_SETS)
    noise       = random.choice(NOISE_LEVELS)
    serves_alc  = True if kind_name in ("Bar", "Restaurant") else random.choice([True, False])

    rating      = round(random.uniform(3.2, 4.9), 1)
    geom_wkt    = f"POINT({lon:.6f} {lat:.6f})"
    opens_block = opening_hours_block()
    menu_iri    = f"<{BASE_IRI}menu/Place{idx}>"
    hood_iri    = f"<{BASE_IRI}hood/{hood_name}>"

    return f"""ex:Place{idx} a {kind_curie} ;
  rdfs:label "{label}" ;
  schema:address "{address}" ;
  schema:servesCuisine "{cuisine}" ;
  schema:priceCurrency "EUR" ;
  ex:averagePricePerPerson "{price}"^^xsd:decimal ;
  ex:hasOutdoorSeating {"true" if has_outdoor else "false"} ;
  ex:hasWifi {"true" if has_wifi else "false"} ;
  ex:veggieFriendly {"true" if veggie else "false"} ;
  ex:noiseLevel "{noise}" ;
  ex:accessibility "{random.choice(['wheelchair','limited','unknown'])}" ;
  schema:acceptsReservations {"true" if accepts_res else "false"} ;
  schema:paymentAccepted "{pays}" ;
  schema:servesAlcohol {"true" if serves_alc else "false"} ;
  schema:menu {menu_iri} ;
  ex:avgRating "{rating}"^^xsd:decimal ;
  geosparql:hasGeometry [ a sf:Point ; geosparql:asWKT "{geom_wkt}"^^geosparql:wktLiteral ] ;
  geo:lat "{lat:.6f}"^^xsd:decimal ;
  geo:long "{lon:.6f}"^^xsd:decimal ;
  # Location links (both neighborhood and Athens for direct querying)
  ex:locatedIn {hood_iri} ;
  ex:locatedIn ex:Athens ;
  schema:openingHoursSpecification
    {opens_block} .
"""

def main():
    # write TTL file
    random.seed(SEED)
    OUT.write_text("", encoding="utf-8")
    with OUT.open("w", encoding="utf-8") as f:
        f.write(TTL_PREFIX)
        f.write("\n")
        f.write("# HQ anchor\n")
        f.write(HQ_BLOCK + "\n")

        # neighborhood nodes (+ locatedIn Athens)
        for name, lon, lat in NEIGHBORHOODS:
            f.write(
                f"""<{BASE_IRI}hood/{name}> a schema:Place ;
  rdfs:label "{name}" ;
  geosparql:hasGeometry [ a sf:Point ; geosparql:asWKT "POINT({lon:.6f} {lat:.6f})"^^geosparql:wktLiteral ] ;
  ex:locatedIn ex:Athens .
"""
            )

        f.write("\n# Generated POIs\n")
        for i in range(1, TOTAL+1):
            f.write(place_block(i))
            f.write("\n")

if __name__ == "__main__":
    main()
