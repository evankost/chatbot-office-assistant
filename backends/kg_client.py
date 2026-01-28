# backends/kg_client.py
from __future__ import annotations
import time
import requests
import re
from typing import Optional, Dict, Any, List, Tuple

from ..config import KOBOLDCPP_URL_SPARQL, SPARQL_ENDPOINT, REQUEST_TIMEOUT_S
from .sparql_mapper import map_sparql_query, ensure_prefixes_all, PREFIX_BLOCK

# Verbose logging switch
VERBOSE_KG = True

# Limits
DEFAULT_KG_LIMIT   = 10
MAX_KG_LIMIT       = 25
DISPLAY_LIMIT_CAP  = 20

# Cuisine detection (fallback when slots lack it)
CUISINE_PATTERNS = {
    "italian": re.compile(r"\bitalian\b", re.I),
    "greek": re.compile(r"\bgreek\b", re.I),
    "japanese": re.compile(r"\bjapanese\b|\bsushi\b", re.I),
    "mexican": re.compile(r"\bmexican\b", re.I),
    "indian": re.compile(r"\bindian\b", re.I),
    "thai": re.compile(r"\bthai\b", re.I),
    "chinese": re.compile(r"\bchinese\b", re.I),
    "mediterranean": re.compile(r"\bmediterranean\b", re.I),
    "seafood": re.compile(r"\bsea\s*food\b", re.I),
    "pizza": re.compile(r"\bpizza\b", re.I),
    "burgers": re.compile(r"\bburgers?\b", re.I),
    "vegan": re.compile(r"\bvegan\b", re.I),
    "vegetarian": re.compile(r"\bvegetarian\b", re.I),
    "middle eastern": re.compile(r"\bmiddle\s+eastern\b|\blebanese\b|\bturkish\b", re.I),
}

def _detect_cuisine(user_text: str, slots: Dict[str, Any]) -> Optional[str]:
    # Pull cuisine from slots if present; otherwise infer from raw text via regexes
    c = (slots or {}).get("cuisine")
    if isinstance(c, str) and c.strip():
        return c.strip()
    t = user_text or ""
    for label, pat in CUISINE_PATTERNS.items():
        if pat.search(t):
            return label.title()
    return None

# ---------- Detail enrichment helpers ----------

def _get_place_iri(b: Dict[str, Any]) -> Optional[str]:
    """Best-effort extraction of the place IRI from a bindings row."""
    v = (b.get("place") or {}).get("value")
    if isinstance(v, str) and v.startswith("http"):
        return v
    return None

def _detail_query_for_place(place_iri: str) -> str:
    # Query for richer details about a single place IRI (schema + ex fallbacks)
    return f"""{PREFIX_BLOCK}
SELECT
  ?label ?address ?price ?rating ?cuisine
  ?phone ?website ?email ?opening ?openingSpec
  ?reservations ?payment ?priceRange ?alcohol ?diet ?menu
  ?sameAs ?reviewCount ?latitude ?longitude
WHERE {{
  BIND(<{place_iri}> AS ?place)
  OPTIONAL {{ ?place rdfs:label ?label }}
  OPTIONAL {{ ?place schema:address ?address }}
  OPTIONAL {{ ?place ex:averagePricePerPerson ?price }}
  OPTIONAL {{ ?place ex:avgRating ?rating }}
  OPTIONAL {{ ?place schema:servesCuisine ?cuisine }}
  OPTIONAL {{ ?place schema:telephone ?phone }}
  OPTIONAL {{ ?place ex:telephone ?phone }}
  OPTIONAL {{ ?place schema:url ?website }}
  OPTIONAL {{ ?place ex:url ?website }}
  OPTIONAL {{ ?place schema:email ?email }}
  OPTIONAL {{ ?place schema:openingHours ?opening }}
  OPTIONAL {{ ?place schema:openingHoursSpecification ?openingSpec }}
  OPTIONAL {{ ?place schema:acceptsReservations ?reservations }}
  OPTIONAL {{ ?place ex:acceptsReservations ?reservations }}
  OPTIONAL {{ ?place schema:paymentAccepted ?payment }}
  OPTIONAL {{ ?place ex:paymentAccepted ?payment }}
  OPTIONAL {{ ?place schema:priceRange ?priceRange }}
  OPTIONAL {{ ?place schema:servesAlcohol ?alcohol }}
  OPTIONAL {{ ?place schema:dietaryRestriction ?diet }}
  OPTIONAL {{ ?place schema:menu ?menu }}
  OPTIONAL {{ ?place schema:sameAs ?sameAs }}
  OPTIONAL {{ ?place schema:reviewCount ?reviewCount }}
  OPTIONAL {{ ?place <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?latitude }}
  OPTIONAL {{ ?place <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?longitude }}
}}
LIMIT 1
"""

# Generic fallback sweep if the above returns nothing new
_WHITELIST_PREDICATES = [
    # common schema props
    "https://schema.org/telephone",
    "https://schema.org/url",
    "https://schema.org/email",
    "https://schema.org/openingHours",
    "https://schema.org/openingHoursSpecification",
    "https://schema.org/acceptsReservations",
    "https://schema.org/paymentAccepted",
    "https://schema.org/priceRange",
    "https://schema.org/servesAlcohol",
    "https://schema.org/dietaryRestriction",
    "https://schema.org/menu",
    "https://schema.org/sameAs",
    "https://schema.org/reviewCount",
    # common ex props (project-specific)
    "http://example.org/telephone",
    "http://example.org/url",
    "http://example.org/menu",
    "http://example.org/instagram",
    "http://example.org/facebook",
    "http://example.org/tags",
]

def _detail_query_fallback(place_iri: str) -> str:
    # Lightweight predicate whitelist scan for extra facts
    in_list = " ".join(f"<{p}>" for p in _WHITELIST_PREDICATES)
    return f"""{PREFIX_BLOCK}
SELECT ?p ?o WHERE {{
  BIND(<{place_iri}> AS ?place)
  ?place ?p ?o .
  FILTER(?p IN ({in_list}))
}}
LIMIT 25
"""

def _detail_query_by_label(label: str) -> str:
    # Resolve to a place IRI by exact label match if IRI is missing
    lab = label.replace("\\", "\\\\").replace('"', '\\"')
    return f"""{PREFIX_BLOCK}
SELECT ?place WHERE {{
  ?place rdfs:label ?lab .
  FILTER(LCASE(STR(?lab)) = LCASE("{lab}"))
}}
LIMIT 1
"""

def _exec_detail(place_iri: str, state) -> Dict[str, Any] | None:
    """Fetch and cache extra details for a place IRI, with generic fallback."""
    cache = getattr(state, "kg_detail_cache", None)
    if cache is None:
        try:
            state.kg_detail_cache = {}
            cache = state.kg_detail_cache
        except Exception:
            cache = {}
    if place_iri in cache:
        return cache[place_iri]

    # pass 1: rich optionals
    q1 = _detail_query_for_place(place_iri)
    rows1, _ = _exec_query(q1, state)
    row = rows1[0] if rows1 else None

    # pass 2: generic sweep to add more keys if not much was found
    if not row or len(row.keys()) <= 5:
        q2 = _detail_query_fallback(place_iri)
        rows2, _ = _exec_query(q2, state)
        # Pack generic sweep into p__/o__ pairs for later display
        if rows2:
            for idx, r in enumerate(rows2[:20], start=1):
                p = (r.get("p") or {}).get("value")
                o = (r.get("o") or {}).get("value")
                if p and o:
                    row = row or {}
                    row[f"p__{idx}"] = {"type":"uri","value":p}
                    row[f"o__{idx}"] = {"type":"literal","value":o}

    if row:
        cache[place_iri] = row
    return row

def _verbalize_detail(base_row: Dict[str, Any], detail_row: Dict[str, Any]) -> str:
    """Prefer showing new info not already present in the list view, then append extras."""
    def get(d, k): 
        return (d.get(k) or {}).get("value")

    label = get(detail_row, "label") or get(base_row, "label") or get(base_row, "name") or get(base_row, "place") or "This place"

    known = {
        "address": get(base_row, "address"),
        "price":   get(base_row, "price") or get(base_row, "averagePricePerPerson"),
        "rating":  get(base_row, "rating") or get(base_row, "avgRating"),
        "cuisine": get(base_row, "cuisine"),
    }

    # Merge base-row fields with enriched details (favor enriched when present)
    address = get(detail_row, "address") or known["address"]
    price   = get(detail_row, "price") or known["price"]
    rating  = get(detail_row, "rating") or known["rating"]
    cuisine = get(detail_row, "cuisine") or known["cuisine"]
    phone   = get(detail_row, "phone")
    site    = get(detail_row, "website")
    email   = get(detail_row, "email")
    opening = get(detail_row, "opening") or get(detail_row, "openingSpec")
    reserv  = get(detail_row, "reservations")
    pay     = get(detail_row, "payment")
    priceR  = get(detail_row, "priceRange")
    alcohol = get(detail_row, "alcohol")
    diet    = get(detail_row, "diet")
    menu    = get(detail_row, "menu")
    sameAs  = get(detail_row, "sameAs")
    reviews = get(detail_row, "reviewCount")
    lat     = get(detail_row, "latitude")
    lon     = get(detail_row, "longitude")

    header_bits = []
    if address and address != known["address"]: header_bits.append(address)
    if cuisine and cuisine != known["cuisine"]: header_bits.append(f"cuisine {cuisine}")
    if rating and rating != known["rating"]:    header_bits.append(f"rating {rating}")
    if price and price != known["price"]:       header_bits.append(f"~€{price}/person")
    if not header_bits:
        # If nothing new was learned, still display a compact header
        if address: header_bits.append(address)
        if cuisine: header_bits.append(f"cuisine {cuisine}")
        if rating:  header_bits.append(f"rating {rating}")
        if price:   header_bits.append(f"~€{price}/person")

    # Compose extras with contact, hours, policies, and coordinates
    extras = []
    if phone:   extras.append(f"☎ {phone}")
    if email:   extras.append(f"email: {email}")
    if site:    extras.append(f"website: {site}")
    if opening: extras.append(f"hours: {opening}")
    if reserv is not None: extras.append(f"reservations: {reserv}")
    if pay:     extras.append(f"payment: {pay}")
    if priceR:  extras.append(f"price range: {priceR}")
    if alcohol: extras.append(f"alcohol: {alcohol}")
    if diet:    extras.append(f"diet: {diet}")
    if menu:    extras.append(f"menu: {menu}")
    if sameAs:  extras.append(f"profile: {sameAs}")
    if reviews: extras.append(f"reviews: {reviews}")
    if lat and lon: extras.append(f"geo: {lat},{lon}")

    # Add a few generic predicate/object pairs captured by the fallback sweep
    generic_pairs = []
    for k, v in detail_row.items():
        if not k.startswith("p__"):
            continue
        idx = k.split("__", 1)[-1]
        p = (detail_row.get(f"p__{idx}") or {}).get("value")
        o = (detail_row.get(f"o__{idx}") or {}).get("value")
        if not p or not o:
            continue
        # Skip duplicates of the standard fields shown above
        if any(s in p for s in ["averagePricePerPerson","avgRating","address","servesCuisine","rdfs#label"]):
            continue
        generic_pairs.append((p, o))
    # Keep display tidy
    for p, o in generic_pairs[:5]:
        short = p.rsplit("/", 1)[-1]  # shorten predicate IRI tail
        extras.append(f"{short}: {o}")

    lines = []
    if header_bits: lines.append(" — ".join(header_bits))
    if extras:      lines.append(" · ".join(extras))

    body = "\n".join(l for l in lines if l)
    return f"{label}:\n{body}" if body else f"{label}"

# ---------- Main entry ----------

def answer_with_kg(payload: Dict[str, Any],
                   user_text: str,
                   slots: Dict[str, Any],
                   state) -> Optional[str]:
    """
    Generate SPARQL, normalize/prefix, apply small rewrites, execute, and summarize.
    Falls back to a safe template when generation fails or yields no rows.
    """

    # Try interpreting the user’s text as a follow-up detail request about a prior list
    place_query = _extract_place_query(user_text, slots)
    if place_query:
        cached_rows = _recent_kg_bindings(state)
        if cached_rows:
            hit = _find_row_by_label_or_id(cached_rows, place_query)
            if hit:
                place_iri = _get_place_iri(hit)
                if not place_iri:
                    # Resolve IRI by exact label match if missing
                    lbl = (hit.get("label") or {}).get("value") or ""
                    if lbl:
                        q_resolve = _detail_query_by_label(lbl)
                        rows_res, _ = _exec_query(q_resolve, state)
                        if rows_res and (rows_res[0].get("place") or {}).get("value"):
                            place_iri = rows_res[0]["place"]["value"]
                if place_iri:
                    detail = _exec_detail(place_iri, state)
                    if detail:
                        return _verbalize_detail(hit, detail)
                return _verbalize_single(hit)

    policy = _persona_price_policy(state, slots)
    cuisine = _detect_cuisine(user_text, slots)
    hood = (slots or {}).get("neighborhood")

    # 1) LLM SPARQL
    sparql_raw = _generate_sparql(user_text, policy=policy, cuisine=cuisine)

    # 2) Normalize + prefixes
    if not sparql_raw:
        return _exec_template_and_summarize(slots, state, policy, cuisine)

    try:
        sparql_norm = map_sparql_query(sparql_raw)
    except Exception as e:
        print("[KG] Mapper error; using raw. Error:", e)
        sparql_norm = sparql_raw

    sparql_final = ensure_prefixes_all(sparql_norm)

    # 3) Rewriters
    sparql_final = _rewrite_cuisine_equals_to_filter(sparql_final, cuisine)
    sparql_final = _inject_neighborhood_constraint(sparql_final, hood)

    # 3a) Hard-enforce persona sorting (ASC/DESC)
    sparql_final = _enforce_order_by(sparql_final, policy["order"])

    # 3b) Limit policy
    sparql_final = _coerce_limit_if_needed(
        sparql_final,
        target=policy["limit"],
        user_forced=policy.get("user_set_limit", False),
    )

    # 4) Sanitize & validate
    sparql_final = _sanitize_vars_and_limit(sparql_final, default_limit=policy["limit"])
    ok, reason = _looks_reasonable_select(sparql_final)
    if VERBOSE_KG:
        print(f"[KG] Validation → ok={ok}, reason='{reason}'")

    # 5) Execute
    text, count = _run_and_summarize(sparql_final, state, display_limit=policy["limit"])
    if count == 0 and (cuisine or hood):
        if VERBOSE_KG:
            print("[KG] Zero rows; falling back to templated query with slots.]")
        return _exec_template_and_summarize(slots, state, policy, cuisine)
    return text

# ---------- Persona-aware sorting ----------

def _persona_price_policy(state, slots: Dict[str, Any]) -> Dict[str, Any]:
    # Determine sorting strategy and limit from user profile or slots
    up = getattr(state, "user_profile", {}) or {}
    band = (slots or {}).get("price_band") or up.get("price_band", "mid")
    explicit_sort = (slots or {}).get("sort")  # "cheap" | "best"

    user_limit = (slots or {}).get("limit")
    if isinstance(user_limit, int) and user_limit > 0:
        limit = max(1, min(MAX_KG_LIMIT, int(user_limit)))
        user_set_limit = True
    else:
        limit = DEFAULT_KG_LIMIT
        user_set_limit = False

    if explicit_sort == "cheap":
        order = "ORDER BY ASC(?price) DESC(?rating)"
    elif explicit_sort == "best":
        order = "ORDER BY DESC(?rating) ASC(?price)"
    else:
        if band == "budget":
            order = "ORDER BY ASC(?price) DESC(?rating)"
        elif band == "premium":
            order = "ORDER BY DESC(?rating) DESC(?price)"
        else:
            order = "ORDER BY DESC(?rating) ASC(?price)"

    return {"band": band, "order": order, "limit": limit, "user_set_limit": user_set_limit}

# ---------- LLM SPARQL generation ----------

def _generate_sparql(question: str, policy: Dict[str, Any], cuisine: Optional[str] = None) -> Optional[str]:
    # Prompt an LLM to produce SPARQL, with strict rules and a compact few-shot
    band = policy.get("band", "mid")
    order_hint = policy.get("order", "ORDER BY DESC(?rating) DESC(?price)")
    limit = policy.get("limit", DEFAULT_KG_LIMIT)
    cuisine_hint = f"User requested cuisine: '{cuisine}'. " if cuisine else ""

    sys = (
        "You generate SPARQL for a local Blazegraph endpoint.\n"
        "CRITICAL RULES:\n"
        "1) ALWAYS include these prefixes exactly once at the top:\n"
        f"{PREFIX_BLOCK}"
        "2) Only use existing classes/properties from the ontology (schema, ex, rdfs).\n"
        "3) Keep queries SMALL (<=6 variables) and include LIMIT.\n"
        "4) Include ?price (ex:averagePricePerPerson) and ?rating (ex:avgRating) when available.\n"
        f"5) Persona price band: '{band}'. Prefer results using this sorting: {order_hint}.\n"
        "6) If cuisine is requested, bind 'schema:servesCuisine ?cuisine' and filter case-insensitively, e.g.:\n"
        "   FILTER(CONTAINS(LCASE(STR(?cuisine)), 'italian')).\n"
        f"7) {cuisine_hint}No invented prefixes. No explanations—return only SPARQL.\n"
        f"8) Use LIMIT {limit} unless the user explicitly asked otherwise.\n"
    )
    fewshot = f"""```sparql
PREFIX ex: <http://example.org/>
PREFIX schema: <https://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?place ?label ?address ?price ?rating
WHERE {{
  ?place a schema:Restaurant ;
         ex:locatedIn ex:Athens ;
         rdfs:label ?label .
  OPTIONAL {{ ?place schema:address ?address }}
  OPTIONAL {{ ?place ex:averagePricePerPerson ?price }}
  OPTIONAL {{ ?place ex:avgRating ?rating }}
}}
{order_hint}
LIMIT 5
```"""
    messages = [
        {"role": "system", "content": sys},
        {"role": "user", "content": "Return the few-shot example only."},
        {"role": "assistant", "content": fewshot},
        {"role": "user", "content": question},
    ]
    payload = {
        "model": "qwen-sparql",
        "messages": messages,
        "stream": False,
        "max_tokens": 220,
        "temperature": 0.1,
    }
    try:
        r = requests.post(KOBOLDCPP_URL_SPARQL, json=payload, timeout=REQUEST_TIMEOUT_S)
        r.raise_for_status()
        content = (r.json().get("choices", [{}])[0].get("message", {}).get("content", "") or "").strip()
        if not content:
            return None
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
        if content.endswith("```"):
            content = content.rsplit("\n", 1)[0]
        return content.strip() or None
    except Exception as e:
        print("[KG] LLM generation error:", e)
        return None

# ---------- Template & execution ----------

def _templated_query_from_slots(slots: Dict[str, Any], policy: Dict[str, Any], cuisine: Optional[str] = None) -> str:
    # Build a minimal, deterministic query from structured slots when LLM output is missing
    typemap = {
        "restaurant": "schema:Restaurant",
        "cafe": "schema:CafeOrCoffeeShop",
        "bar": "schema:BarOrPub",
    }
    t = (slots or {}).get("type", "restaurant").lower()
    klass = typemap.get(t, "schema:Restaurant")

    limit = int(policy.get("limit", DEFAULT_KG_LIMIT))
    order_clause = policy.get("order", "ORDER BY DESC(?rating) ASC(?price)")

    hood = (slots or {}).get("neighborhood")
    hood_iri = f"<http://example.org/hood/{hood}>" if hood else None
    located_clause = f"  ?place ex:locatedIn {hood_iri} ." if hood_iri else "  ?place ex:locatedIn+ ex:Athens ."

    cuisine_opt = "  OPTIONAL { ?place schema:servesCuisine ?cuisine }\n"
    cuisine_filter = ""
    if cuisine:
        val = cuisine.lower().replace("\\", "\\\\").replace("'", "\\'")
        cuisine_filter = f"  FILTER(CONTAINS(LCASE(STR(?cuisine)), '{val}'))\n"

    query = f"""{PREFIX_BLOCK}
SELECT ?place ?label ?address ?price ?rating ?cuisine
WHERE {{
  ?place a {klass} ;
         rdfs:label ?label .
{located_clause}
  OPTIONAL {{ ?place schema:address ?address }}
  OPTIONAL {{ ?place ex:averagePricePerPerson ?price }}
  OPTIONAL {{ ?place ex:avgRating ?rating }}
{cuisine_opt}{cuisine_filter}}}
{order_clause}
LIMIT {limit}
"""
    return query

def _exec_template_and_summarize(slots, state, policy, cuisine) -> str:
    # Execute the fallback template and verbalize the list view
    templ = _templated_query_from_slots(slots, policy=policy, cuisine=cuisine)
    text, _ = _run_and_summarize(templ, state, display_limit=policy["limit"])
    return text

def _run_and_summarize(sparql: str, state, display_limit: int) -> Tuple[str, int]:
    # Execute SPARQL and keep last rows in state for follow-up detail requests
    rows, err = _exec_query(sparql, state)
    try:
        setattr(state, "last_kg_rows", rows or [])
    except Exception:
        pass
    text = _verbalize(rows, display_limit=display_limit)
    return text, len(rows or [])

# ---------- Cache helpers ----------

def _recent_kg_bindings(state) -> List[Dict[str, Any]]:
    # Retrieve the most recent KG result bindings from state/history
    rows = getattr(state, "last_kg_rows", None)
    if isinstance(rows, list) and rows:
        return rows
    try:
        for turn in reversed(getattr(state, "history", []) or []):
            for ev in reversed(getattr(turn, "tool_events", []) or []):
                if getattr(ev, "source", "") == "kg":
                    bindings = (getattr(ev, "response", {}) or {}).get("bindings") or []
                    if bindings:
                        return bindings
    except Exception:
        pass
    return []

_PLACE_TOKENS_RE = re.compile(r"[\w'\-\.]+(?:\s+[\w'\-\.]+)*", re.U)

def _extract_place_query(user_text: str, slots: Dict[str, Any]) -> Optional[str]:
    # Heuristic extraction of a place mention (quoted first, else last multi-token chunk)
    p = (slots or {}).get("place")
    if isinstance(p, str) and p.strip():
        return p.strip()
    t = (user_text or "").strip()
    if not t:
        return None
    m = re.search(r'["“](.+?)["”]', t)
    if m and m.group(1).strip():
        return m.group(1).strip()
    candidates = re.findall(_PLACE_TOKENS_RE, t)
    candidates = [c.strip() for c in candidates if c and (any(ch.isdigit() for ch in c) or len(c.split()) >= 2)]
    return candidates[-1] if candidates else None

def _norm(s: Optional[str]) -> str:
    # Lowercased, trimmed normalization
    return (s or "").strip().lower()

def _getv(b: Dict[str, Any], key: str) -> Optional[str]:
    # Helper to extract "value" from a SPARQL binding cell
    v = (b.get(key) or {}).get("value")
    return v

def _find_row_by_label_or_id(rows: List[Dict[str, Any]], q: str) -> Optional[Dict[str, Any]]:
    # Match by exact label/IRI tail; otherwise return first partial match
    qn = _norm(q)
    best = None
    for b in rows:
        label = _getv(b, "label") or _getv(b, "name") or ""
        place = _getv(b, "place") or ""
        if _norm(label) == qn or _norm(place).endswith(qn):
            return b
        if qn in _norm(label):
            best = best or b
    return best

# ---------- HTTP/SPARQL ----------

def _exec_query(sparql: str, state) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    # Run GET query against the Blazegraph endpoint; log metrics and errors
    t0 = time.time()
    rows: List[Dict[str, Any]] = []
    err: Optional[str] = None
    resp = None
    try:
        if VERBOSE_KG:
            print("[KG] Executing query:\n", sparql)
        resp = requests.get(
            SPARQL_ENDPOINT,
            params={"query": sparql, "format": "json"},
            timeout=REQUEST_TIMEOUT_S,
        )
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("results", {}).get("bindings", []) or []
        if VERBOSE_KG:
            print(f"[KG] Rows returned: {len(rows)}")
            if rows[:1]:
                print("[KG] First row preview:", rows[0])
    except Exception as e:
        err = str(e)
        print("[KG] Execution error:", err)
        try:
            if resp is not None:
                print("[KG] Raw response head:", resp.text[:400])
        except Exception:
            pass
    finally:
        elapsed_ms = int((time.time() - t0) * 1000)
        try:
            state.log_kg_result(sparql=sparql, bindings=rows or [], elapsed_ms=elapsed_ms, error=err)
        except Exception:
            pass
    return rows, err

# ---------- Rewriters ----------

_CUISINE_EQ_RE = re.compile(
    r'(?im)^\s*\?place\s+schema:servesCuisine\s+(".*?"(?:@[a-z\-]+)?|\S+)\s*\.\s*$'
)

_ORDER_BY_BLOCK_RE = re.compile(r'(?is)\bORDER\s+BY\b[^}]*?(?=(?:\bLIMIT\b|}$))')
_LIMIT_RE = re.compile(r'(?i)\bLIMIT\s+(\d+)\b')

def _enforce_order_by(s: str, order_clause: str) -> str:
    # Ensure exactly one ORDER BY, replacing or inserting before LIMIT
    if not order_clause or "ORDER BY" not in order_clause.upper():
        return s
    if _ORDER_BY_BLOCK_RE.search(s):
        return _ORDER_BY_BLOCK_RE.sub(order_clause + "\n", s, count=1)
    m = _LIMIT_RE.search(s)
    if m:
        i = m.start()
        return s[:i] + order_clause + "\n" + s[i:]
    return s.rstrip() + "\n" + order_clause + "\n"

def _rewrite_cuisine_equals_to_filter(s: str, cuisine: Optional[str]) -> str:
    # Replace hard equals on servesCuisine with a case-insensitive CONTAINS filter
    if not cuisine:
        return s
    val = cuisine.lower().replace("\\", "\\\\").replace("'", "\\'")
    replacement = (
        "  OPTIONAL { ?place schema:servesCuisine ?cuisine }\n"
        f"  FILTER(CONTAINS(LCASE(STR(?cuisine)), '{val}'))\n"
    )
    s2 = _CUISINE_EQ_RE.sub(replacement, s)
    if s2 != s:
        s2 = _ensure_select_var(s2, "?cuisine")
    return s2

def _inject_neighborhood_constraint(s: str, hood: Optional[str]) -> str:
    # Insert or replace a neighborhood constraint when present in slots
    if not hood:
        return s
    hood_iri = f"<http://example.org/hood/{hood}>"
    if hood_iri in s:
        return s
    s2 = s.replace("ex:locatedIn ex:Athens", f"ex:locatedIn {hood_iri}")
    if s2 != s:
        return s2
    return re.sub(r'(?is)(WHERE\s*{)',
                  rf"\1\n  ?place ex:locatedIn {hood_iri} .\n",
                  s, count=1)

def _coerce_limit_if_needed(s: str, target: int, user_forced: bool) -> str:
    # Increase LIMIT to target unless user explicitly set a limit
    m = _LIMIT_RE.search(s)
    if not m:
        return s
    if user_forced:
        return s
    try:
        current = int(m.group(1))
        if current < target:
            s = _LIMIT_RE.sub(f"LIMIT {target}", s, count=1)
    except Exception:
        pass
    return s

def _ensure_select_var(s: str, var: str) -> str:
    # Make sure a variable appears in the SELECT clause (DISTINCT-aware)
    if re.search(r'(?i)SELECT\s+DISTINCT', s):
        return re.sub(r'(?i)(SELECT\s+DISTINCT\s+)(.*?)\s+WHERE',
                      lambda m: f"{m.group(1)}{m.group(2)} {var} WHERE", s, count=1)
    return re.sub(r'(?i)(SELECT\s+)(.*?)\s+WHERE',
                  lambda m: f"{m.group(1)}{m.group(2)} {var} WHERE", s, count=1)

# ---------- Verbalization ----------

def _verbalize(rows: List[Dict[str, Any]], display_limit: int = DEFAULT_KG_LIMIT) -> str:
    # List-style rendering of rows with compact attributes
    if not rows:
        return "No results."
    n = max(1, min(DISPLAY_LIMIT_CAP, int(display_limit or DEFAULT_KG_LIMIT)))
    lines = []
    for b in rows[:n]:
        get = lambda k: (b.get(k) or {}).get("value")
        label = get("label") or get("name") or get("place")
        addr  = get("address")
        price = get("price") or get("averagePricePerPerson")
        rate  = get("rating") or get("avgRating")
        cuis  = get("cuisine")
        parts = [
            label,
            addr,
            f"€{price}" if price else None,
            f"rating {rate}" if rate else None,
            f"cuisine {cuis}" if cuis else None,
        ]
        parts = [p for p in parts if p]
        lines.append("• " + " — ".join(parts) if parts else "• (row)")
    return "Results:\n" + "\n".join(lines)

def _verbalize_single(b: Dict[str, Any]) -> str:
    # Single-row variant for detail lookups when enrichment isn't available
    get = lambda k: (b.get(k) or {}).get("value")
    label = get("label") or get("name") or get("place") or "This place"
    addr  = get("address")
    price = get("price") or get("averagePricePerPerson")
    rate  = get("rating") or get("avgRating")
    cuis  = get("cuisine")
    bits = []
    if addr:  bits.append(addr)
    if cuis:  bits.append(f"cuisine {cuis}")
    if rate:  bits.append(f"rating {rate}")
    if price: bits.append(f"~€{price} per person")
    body = " — ".join(bits) if bits else "Details are limited based on previous results."
    return f"{label}: {body}"

# ---------- Validation ----------

_BAD_VAR_RE = re.compile(r"\?([A-Za-z0-9_]+:)([A-Za-z0-9_]+)")

def _sanitize_vars_and_limit(s: str, default_limit: int = DEFAULT_KG_LIMIT) -> str:
    # Fix malformed ?prefix:var tokens and ensure a LIMIT exists
    def fix_var(m): return "?" + m.group(2)
    s = _BAD_VAR_RE.sub(fix_var, s)
    if not _LIMIT_RE.search(s):
        s = s.rstrip() + f"\nLIMIT {int(default_limit or DEFAULT_KG_LIMIT)}\n"
    return s

def _looks_reasonable_select(s: str) -> Tuple[bool, str]:
    # Quick static checks: SELECT presence, WHERE block, size heuristics
    if not re.search(r"^\s*SELECT\b", s, re.IGNORECASE | re.MULTILINE):
        return False, "not a SELECT"
    if "WHERE" not in s.upper():
        return False, "missing WHERE"
    m = re.search(r"(?is)SELECT\s+(.*?)\s+WHERE", s)
    sel_vars = len(re.findall(r"\?[A-Za-z_]\w*", m.group(1))) if m else 0
    body = re.split(r"(?is)\bWHERE\s*{", s, maxsplit=1)
    triples_est = 0
    if len(body) == 2:
        where_body = body[1]
        where_body = re.sub(r"(?m)^\s*#.*$", "", where_body)
        triples_est = where_body.count(".")
    if sel_vars > 12:
        return False, "too many select vars"
    if triples_est > 60:
        return False, "too many triples"
    return True, "ok"
