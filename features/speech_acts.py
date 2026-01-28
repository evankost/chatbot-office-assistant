# features/speech_acts.py
from __future__ import annotations
import re
from typing import Tuple, Dict, Optional

# Canonical venue types used downstream (KG alignment)
CANON_TYPES = {"restaurant", "cafe", "bar"}

# Synonyms → canonical venue type (broad coverage, tolerant to spelling)
VENUE_SYNONYMS: Dict[str, list[str]] = {
    # Restaurants
    "restaurant": [
        r"\brestaurants?\b", r"\bresto\b", r"\bresturant\b", r"\brestaraunt\b",
        r"\beat(?:ery|eries)\b", r"\bdiners?\b", r"\bsteakhouses?\b",
        r"\bpizzerias?\b", r"\btrattorias?\b|\btrattorie\b",
        r"\bosterias?\b|\bosterie\b",
        r"\btavernas?\b|\btaverns?\b|\btaverna\b",
        r"\bgrill\s*houses?\b|\bgrills?\b",
        r"\broasteries?\b",
        r"\bfast\s*food\b",
        r"\btake[-\s]?aways?\b|\btakeouts?\b",
        r"\bsouvlak(?:i|ia)\b|\bgyro?s?\b|\bkebabs?\b|\bmeze(dopolio)?\b|\bmezze?\b",
        r"\bbrasseries?\b", r"\bbistros?\b",
        r"\bfood\b",
    ],
    # Cafes
    "cafe": [
        r"\bcaf(?:e|é)s?\b", r"\bcoffee\s*shops?\b", r"\bcoffees?\b",
        r"\bespresso\s*bars?\b", r"\bbrunch\b|\bbreakfast\b",
        r"\bbakeries?\b|\bpatisseries?\b|\bpastry\s*shops?\b|\bconfectioner(?:y)?\b",
        r"\bgelaterias?\b|\bice\s*cream\b|\bdessert\s*bars?\b|\bcreperies?\b|\bbagels?\b|\bdonuts?\b",
        r"\bcafeterias?\b",
    ],
    # Bars
    "bar": [
        r"\bbars?\b", r"\bpubs?\b",
        r"\bbrewer(?:y|ies)\b|\btaprooms?\b",
        r"\bwine\s*bars?\b",
        r"\bcocktail\s*bars?\b|\bcocktails?\b",
        r"\boutlet\b.*\bdrinks?\b",
        r"\bouzeris?\b|\bouzeries\b|\bouzerias?\b",
        r"\blounges?\b",
    ],
}

# Precompiled synonym patterns (fast lookup)
VENUE_PATTERNS: list[tuple[re.Pattern, str]] = []
for canon, pats in VENUE_SYNONYMS.items():
    for p in pats:
        VENUE_PATTERNS.append((re.compile(p, re.I), canon))

# Neighborhood aliases → canonical KG labels
NEIGHBORHOOD_ALIASES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bsyntagma(\s+square)?\b", re.I), "Syntagma"),
    (re.compile(r"\bpláka\b|\bplaka\b", re.I), "Plaka"),
    (re.compile(r"\bmonastiraki\b", re.I), "Monastiraki"),
    (re.compile(r"\bkolonaki\b", re.I), "Kolonaki"),
    (re.compile(r"\bkoukaki\b|\bkukaki\b", re.I), "Koukaki"),
    (re.compile(r"\bexarcheia\b|\bexarchia\b", re.I), "Exarchia"),
    (re.compile(r"\bpsiri\b|\bpsyrri\b", re.I), "Psyrri"),
]

# Feature/constraint detectors (flat regex set; slots become booleans/values)
WIFI_PAT = re.compile(r"\b(wifi|wi[-\s]?fi|internet)\b", re.I)
OUTDOOR_PAT = re.compile(r"\b(outdoor|outside|terrace|patio|garden|sidewalk|veranda)\b", re.I)
VEGGIE_PAT = re.compile(r"\b(vegan|vegetarian|veg[-\s]?friendly)\b", re.I)
ALCO_PAT = re.compile(r"\b(alcohol|drinks?|cocktails?|beer|wine)\b", re.I)
RES_PAT = re.compile(r"\b(reservations?|book|table|reserve)\b", re.I)
PAY_PAT = re.compile(r"\b(cash|visa|mastercard|amex|american express|paypal|card|cards)\b", re.I)
OPEN_NOW_PAT = re.compile(r"\b(open now|open\s*(right\s*)?now|hours|opening)\b", re.I)
NEAR_PAT = re.compile(r"\bnear(by)?\b|\bclose\s*to\b|near me|nearby|close by|around here", re.I)

# Numeric constraints: price/rating/limit + sort preferences
PRICE_MAX_PAT = re.compile(r"(?:under|below|<|<=|up to|no more than)\s*(\d{1,3})\s*€?\s*", re.I)
PRICE_RANGE_PAT = re.compile(r"(\d{1,3})\s*[-–]\s*(\d{1,3})\s*€?\s*", re.I)
RATING_MIN_PAT = re.compile(r"(?:rating|stars?)[^\d]{0,6}(\d(?:\.\d)?)", re.I)
LIMIT_PAT = re.compile(r"\btop\s*(\d{1,2})\b|\bfirst\s*(\d{1,2})\b|\bnext\s*(\d{1,2})\b", re.I)
SORT_BEST_PAT = re.compile(r"\b(best|top|highest[-\s]?rated)\b", re.I)
SORT_CHEAP_PAT = re.compile(r"\b(cheap|cheapest|budget|value|affordable|inexpensive|low[-\s]?cost|good value)\b", re.I)

# Cuisine signals (normalized later to Title Case)
CUISINE_PATTERNS = {
    "italian":       r"\bitalian\b",
    "greek":         r"\bgreek\b",
    "japanese":      r"\bjapanese\b|\bsushi\b",
    "mexican":       r"\bmexican\b",
    "indian":        r"\bindian\b",
    "thai":          r"\bthai\b",
    "chinese":       r"\bchinese\b",
    "mediterranean": r"\bmediterranean\b",
    "seafood":       r"\bsea\s*food\b",
    "pizza":         r"\bpizza\b",
    "burgers":       r"\bburgers?\b",
    "vegan":         r"\bvegan\b",
    "vegetarian":    r"\bvegetarian\b",
    "middle eastern":r"\bmiddle\s+eastern\b|\blebanese\b|\bturkish\b",
}

# DB-facing keywords (read-only intents: tasks/appointments/staff)
DB_HARD = re.compile(
    r"\b(tasks?|todo|appointments?|meeting|schedule|calendar|staff|assign|resched|reschedule)\b",
    re.I,
)

# Confirmation/cancellation cues (domain-agnostic)
CONFIRM_PAT = re.compile(r"\b(yes|yeah|yep|correct|do it|go ahead|sounds good|ok(ay)?|please proceed)\b", re.I)
CANCEL_PAT = re.compile(
    r"\b("
    r"cancel(?:\s*(?:it|that))?"
    r"|never[\s\-]?mind"
    r"|nvm"
    r"|stop"
    r"|abort"
    r"|undo"
    r"|don'?t\s+(?:do|proceed)"
    r"|do\s+not\s+(?:do|proceed)"
    r")\b",
    re.I
)

# Imperatives that imply a request without a question form
IMPERATIVE_VERBS = re.compile(
    r"^(show|find|list|give|tell|check|lookup|look up|filter|summarize|book|schedule|add|create|send|draft)\b",
    re.I
)

# Small-talk / generic act patterns
GREET_PAT = re.compile(r"\b(hey|hello|hi|good\s*(morning|evening|afternoon))\b", re.I)
GOODBYE_PAT = re.compile(r"\b(bye|good\s*bye|see\s*you|later|good\s*night)\b", re.I)
AFFIRM_PAT = re.compile(r"\b(yes|y|indeed|of course|correct|sure|okay|ok|sounds good)\b", re.I)
DENY_PAT = re.compile(r"\b(no|n|never|not really|nope|cancel|stop)\b", re.I)
MOOD_GREAT_PAT = re.compile(r"\b(perfect|great|amazing|wonderful|very good|super|fantastic|happy)\b", re.I)
MOOD_UNHAPPY_PAT = re.compile(r"\b(horrible|sad|unhappy|not good|disappointed|annoyed|frustrated|upset|tired|stressed)\b", re.I)
BOT_CHALLENGE_PAT = re.compile(r"\b(are you a (bot|human)\??|am i talking to (a )?(bot|human)\??)\b", re.I)
THANKS_PAT = re.compile(r"\b(thanks?|thank you|appreciate it)\b", re.I)
APOLOGY_PAT = re.compile(r"\b(sorry|my bad|apologies|pardon)\b", re.I)

# Plan/promise/order/ask signals (for act_major/act_subtype)
PROMISE_PAT = re.compile(r"\b(i ('?ll|will|shall|can)\s*(do|handle|fix|take care))\b", re.I)
PLAN_PAT = re.compile(r"\b(let('?s)?|we could|we should)\s+(do|go|book|plan|organize|schedule)\b", re.I)
ORDER_PAT = re.compile(r"\b(order|book|schedule|create|add|assign|find|show|send|set up|make)\b", re.I)
QUESTION_PAT = re.compile(r"\?$|^\s*(can|could|would|will|do|does|did|how|what|when|where|why|which|who)\b", re.I)

# Minor typo normalization (helps downstream regexes)
def _normalize_typos(s: str) -> str:
    fixes = {
        "reastaurant": "restaurant",
        "restarant": "restaurant",
        "restauratn": "restaurant",
        "cusine": "cuisine",
        "cuisne": "cuisine",
        "cusiune": "cuisine",
        "kolonakii": "kolonaki",
        "psiri": "psyrri",
        "exarcheia": "exarchia",
        "kukaki": "koukaki",
    }
    low = s
    for wrong, right in fixes.items():
        low = re.sub(rf"\b{re.escape(wrong)}\b", right, low, flags=re.I)
    return low

# Extractors (canonical venue type / neighborhood / cuisine)
def _extract_type(t: str) -> Optional[str]:
    """Return canonical type if any synonym matches."""
    for pat, canon in VENUE_PATTERNS:
        if pat.search(t):
            return canon
    return None

def _extract_neighborhood(t: str) -> Optional[str]:
    """Return canonical neighborhood label."""
    for pat, canon in NEIGHBORHOOD_ALIASES:
        if pat.search(t):
            return canon
    return None

def _extract_cuisine(t: str) -> Optional[str]:
    for canon, pat in CUISINE_PATTERNS.items():
        if re.search(pat, t, re.I):
            return canon.title()
    return None

# Act/intent decision (directive-first; domain-first for intent)
def decide_act_and_intent(utterance: str) -> Tuple[str, str, str]:
    """
    Returns (act_major, act_subtype, intent) with precedence:
    domain (DB/KG) > small-talk > generic; ASK/REQUEST > confirm/deny > ack > state.
    """
    u = (utterance or "").strip()
    ul = u.lower()

    # Domain cues
    venue_type = _extract_type(ul)
    venue_like = bool(venue_type or any(k in ul for k in ["bar","cafe","restaurant","coffee","lunch","dinner","drinks"]))
    db_like = bool(DB_HARD.search(ul))
    has_domain = db_like or venue_like or bool(OPEN_NOW_PAT.search(ul) or NEAR_PAT.search(ul))

    # Directive cues
    is_question = bool(QUESTION_PAT.search(ul))
    is_request  = bool(ORDER_PAT.search(ul) or IMPERATIVE_VERBS.search(ul))

    # Short confirms/denials
    is_affirm = bool(AFFIRM_PAT.search(ul))
    is_deny   = bool(DENY_PAT.search(ul))

    # Pure acknowledgments (no domain/request/question; short)
    def pure_ack(pat: re.Pattern) -> bool:
        if not pat.search(ul):
            return False
        if is_question or is_request or has_domain:
            return False
        return len(ul.split()) <= 6

    # Intent (domain-first)
    if db_like:
        intent = "db_query"
    elif venue_like or OPEN_NOW_PAT.search(ul) or NEAR_PAT.search(ul):
        intent = "food_search"
    else:
        if pure_ack(GREET_PAT):         intent = "greet"
        elif pure_ack(GOODBYE_PAT):     intent = "goodbye"
        elif pure_ack(THANKS_PAT):      intent = "thanks"
        elif pure_ack(APOLOGY_PAT):     intent = "apology"
        elif is_affirm:                 intent = "affirm"
        elif is_deny:                   intent = "deny"
        elif MOOD_UNHAPPY_PAT.search(ul) or re.search(r"\bnot\s+great\b", ul):
            intent = "mood_unhappy"
        elif MOOD_GREAT_PAT.search(ul):
            intent = "mood_great"
        elif BOT_CHALLENGE_PAT.search(ul):
            intent = "bot_challenge"
        else:
            intent = "generic"

    # Act (directive-first)
    if is_question:
        act_major, act_sub = "DIRECTIVE", "ASK"
    elif is_request:
        act_major, act_sub = "DIRECTIVE", "REQUEST"
    elif is_affirm:
        act_major, act_sub = "CONSTATIVE", "CONFIRM"
    elif is_deny:
        act_major, act_sub = "CONSTATIVE", "DENY"
    elif pure_ack(GREET_PAT):
        act_major, act_sub = "ACKNOWLEDGMENT", "GREET"
    elif pure_ack(THANKS_PAT):
        act_major, act_sub = "ACKNOWLEDGMENT", "THANK"
    elif pure_ack(APOLOGY_PAT):
        act_major, act_sub = "ACKNOWLEDGMENT", "APOLOGIZE"
    elif pure_ack(GOODBYE_PAT):
        act_major, act_sub = "ACKNOWLEDGMENT", "GOODBYE"
    elif PLAN_PAT.search(ul):
        act_major, act_sub = "COMMISSIVE", "PLAN"
    elif PROMISE_PAT.search(ul):
        act_major, act_sub = "COMMISSIVE", "PROMISE"
    else:
        act_major, act_sub = "CONSTATIVE", "STATE"

    return act_major, act_sub, intent

# Public API: classify + slot extraction (includes control flags)
def analyze(text: str, state) -> Tuple[str, str, Dict]:
    """
    Returns (act_major, intent, slots) where slots include act_subtype and
    normalized domain constraints (type/neighborhood/cuisine/features/sort/limits).
    """
    t = (text or "").strip()
    t = _normalize_typos(t)
    ul = t.lower()

    # Control flags (survive small-talk early return)
    slots: Dict[str, object] = {}
    if CONFIRM_PAT.search(ul): slots["confirm"] = True
    if CANCEL_PAT.search(ul):  slots["cancel"]  = True

    # Speech act + initial intent
    act_major, act_subtype, intent = decide_act_and_intent(t)
    slots["act_subtype"] = act_subtype

    # Small-talk: return with flags only
    if intent in {"greet","goodbye","affirm","deny","mood_great","mood_unhappy","bot_challenge","thanks","apology"}:
        return act_major, intent, slots

    # Venue/KG slots
    venue_type = _extract_type(ul)
    if venue_type in CANON_TYPES:
        slots["type"] = venue_type

    hood = _extract_neighborhood(ul)
    if hood:
        slots["neighborhood"] = hood

    if NEAR_PAT.search(ul):
        slots["near"] = "HQ"

    cuisine = _extract_cuisine(ul)
    if cuisine:
        slots["cuisine"] = cuisine

    # Feature flags
    if WIFI_PAT.search(ul):     slots["wifi"] = True
    if OUTDOOR_PAT.search(ul):  slots["outdoor"] = True
    if VEGGIE_PAT.search(ul):   slots["veggie"] = True
    if ALCO_PAT.search(ul):     slots["alcohol"] = True
    if RES_PAT.search(ul):      slots["reservations"] = True
    if PAY_PAT.search(ul):      slots["payment"] = True
    if OPEN_NOW_PAT.search(ul): slots["open_now"] = True

    # Price/rating
    if (m := PRICE_MAX_PAT.search(ul)):  slots["price_max"] = int(m.group(1))
    if (m := PRICE_RANGE_PAT.search(ul)):
        slots["price_min"] = int(m.group(1)); slots["price_max"] = int(m.group(2))
    if (m := RATING_MIN_PAT.search(ul)):
        try: slots["rating_min"] = float(m.group(1))
        except Exception: pass

    # Limit/sort
    if (m := LIMIT_PAT.search(ul)):
        slots["limit"] = int(m.group(1) or m.group(2))
    if SORT_BEST_PAT.search(ul): slots["sort"] = "best"
    elif SORT_CHEAP_PAT.search(ul): slots["sort"] = "cheap"

    # Anaphora: “there/more/another” → continue food_search using last venue context
    if intent == "generic" and state is not None:
        if any(tok in ul for tok in ["there", "more", "another"]) and state.last_entities.get("venue"):
            intent = "food_search"
            le = state.last_entities["venue"]
            if le.get("type") and "type" not in slots:
                slots["type"] = le["type"]
            if le.get("neighborhood") and "neighborhood" not in slots:
                slots["neighborhood"] = le["neighborhood"]
            if state.slots.get("sort") and "sort" not in slots:
                slots["sort"] = state.slots["sort"]

    # Strong DB cues override anaphora bias
    if DB_HARD.search(ul):
        intent = "db_query"

    return act_major, intent, slots
