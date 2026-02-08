# core/router.py — request router with onboarding, etiquette, and tool orchestration
from __future__ import annotations
import json
import re
from typing import Dict, Any

# Linguistic/behavioral features
from ..features import speech_acts, repairs, sentiment, style
from ..features.context import DialogueState

# Read-only backends
from ..backends.llm_client import stream_llm_reply
from ..backends.kg_client import answer_with_kg
from ..backends.db_client import answer_with_db
from ..backends.db_client import lookup_staff_by_name_exact  # deterministic identity lookup

# Intent sets
KG_INTENTS = {"food_search", "place_info"}
DB_INTENTS = {"check_tasks", "free_slots", "db_query"}

# Legacy simple name capture (kept for compatibility)
NAME_CAPTURE = re.compile(r"\b(?:i am|i'm|my name is)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b")

# Small-talk intents that skip the name prompt
SMALLTALK_INTENTS = {"greet", "thanks", "apology", "goodbye"}

# Simple detector for follow-up detail on a previously listed place
_DETAIL_PAT = re.compile(r"\b(?:more info|details?|tell me more|what about)\b", re.I)
_NUM_IN_LABEL = re.compile(r"\b(\d{1,5})\b")

def route_request(payload: Dict[str, Any], state: DialogueState):
    """
    Router with deterministic onboarding & addressing:
      - Quick-acks return immediately (tone by role_level).
      - Ask for full name once if privacy_mode=='ask' and not verified (except small-talk).
      - If a name is provided, verify deterministically and update persona.
      - Staff directory: IT/HR (any level) and leadership (level ≤2) have cross-dept access;
        others (level ≥3) restricted to own department; anonymous users barred.
    """
    user_text = _latest_user(payload)

    clean_text, changed = repairs.apply_self_repair(user_text)
    text_for_classification = clean_text if clean_text else user_text

    # 1) Classification
    act_major, intent, slots = speech_acts.analyze(text_for_classification, state)
    slots = state.resolve_references(user_text, slots)
    act_sub = slots.get("act_subtype")
    mood = sentiment.get_mood(text_for_classification)
    try:
        mood_score = getattr(sentiment, "get_score")(text_for_classification)
    except Exception:
        mood_score = 0.0

    # 1a) Deterministic name capture + verification (DB-backed)
    name = _extract_full_name(user_text)
    just_verified = False
    if name and state.db_enabled:
        staff = lookup_staff_by_name_exact(name) or {}
        state.update_user_identity(
            name=staff.get("name", name),
            staff_id=staff.get("id"),
            role=staff.get("role"),
            role_level=staff.get("role_level"),
            department=staff.get("department"),
            privacy_mode="identified" if staff.get("id") else state.user_profile.get("privacy_mode", "ask"),
        )
        just_verified = bool(staff.get("id"))

    # 0) Determine if this turn is identity-only (name or anonymity mention)
    identity_only = bool(name) or _mentions_anonymous(user_text)

    # 1) Effective intent: reuse last domain intent on identity-only generic turns
    last_domain_intents = ["food_search", "place_info", "db_query", "check_tasks", "free_slots"]
    prev_domain = next((i for i in reversed(state.history_intents) if i in last_domain_intents), None)
    effective_intent = intent
    if intent == "generic" and identity_only and prev_domain:
        effective_intent = prev_domain

    # 2) Merge durable slots with this turn’s slots (current wins)
    durable_mem = {k: v for k, v in (state.slots or {}).items() if k not in {"confirm", "cancel", "act_subtype"}}
    merged_slots = dict(durable_mem)
    for k, v in (slots or {}).items():
        if k not in {"confirm", "cancel", "act_subtype"} and v not in (None, "", False):
            merged_slots[k] = v

    # Topic/entity bookkeeping with effective values
    state.update_topics_and_entities(effective_intent, merged_slots)

    # Log user turn
    state.add_user_turn(
        text=user_text,
        act_major=act_major,
        act_subtype=act_sub,
        intent=intent,
        slots=slots,
        mood=mood,
    )

    # 1c) One-time onboarding gate (skips small-talk)
    if state.needs_onboarding() and intent not in SMALLTALK_INTENTS:
        state.asked_name_once = True
        return _stream_text(
            "Before we continue, could you share your full name so I can personalize results? "
            "If you prefer, we can continue anonymously."
        )

    # 2) Quick acknowledgments (tone-aware)
    if act_sub in {"GREET", "THANK", "GOODBYE", "APOLOGIZE"}:
        return _stream_text(_quick_ack(act_sub, state.user_profile))

    # Cancellation
    if slots.get("cancel"):
        state.pending_action = None
        return _stream_text(_quick_ack("APOLOGIZE", state.user_profile) or "Understood. I’ve cancelled that.")

    # Lightweight confirmation
    if slots.get("confirm") and state.pending_action:
        state.pending_action = None
        return _stream_text("Confirmed.")

    # 2b) Hard gate: anonymous cannot query staff/department info
    up = state.user_profile
    text_low = (user_text or "").lower()
    is_staff_like = ("staff" in text_low) or ("employee" in text_low)
    if up.get("privacy_mode") == "anonymous" and is_staff_like and effective_intent in DB_INTENTS:
        return _stream_text("For staff directory access, please share your full name to verify your identity.")

    # 3) Build enriched payload
    enriched = dict(payload)

    # Strip external system messages
    original_msgs = enriched.get("messages", [])
    user_and_assistant_only = [m for m in original_msgs if m.get("role") != "system"]

    # 3a) Base system hint (persona + etiquette + staff-policy)
    sys_hint = _system_hint_base(act_major, act_sub, effective_intent, mood, state, user_text=user_text) + f" MoodScore={mood_score:.2f}."

    # 3b) Soft onboarding hint (guarded by deterministic gate)
    up = state.user_profile
    if up.get("privacy_mode") == "ask" and not up.get("verified"):
        sys_hint += (
            " Onboarding: Ask the user (in one short sentence) for their full name to personalize results, "
            "and explicitly offer an anonymous option. If they choose anonymous, proceed without personalization."
        )

    # One-turn etiquette guard after verification
    if just_verified and up.get("verified"):
        ln = _last_name(up.get("name") or "")
        lvl = up.get("role_level")
        if lvl is not None and lvl <= 2:
            sys_hint += (
                f" Immediate etiquette: Do NOT thank the user for sharing their name and do NOT use first or full name. "
                f"If addressing is unavoidable, use exactly 'Mr./Ms. {ln}'. Otherwise omit the name."
            )
        elif lvl is not None and 3 <= lvl <= 4:
            sys_hint += (
                " Immediate etiquette: Avoid using the user's name; if strictly necessary, prefer 'Mr./Ms. "
                f"{ln}'. Do not echo the full name."
            )
        else:
            first = (up.get("name") or "").split()[0] if up.get("name") else ""
            if first:
                sys_hint += (
                    f" Immediate etiquette: If addressing by name, you may use the first name '{first}' once; "
                    "never use the full name. Keep it brief and professional."
                )
            else:
                sys_hint += " Immediate etiquette: Do not echo the user's full name."

    # 3c) Optional clarification hint for DB; show KG results first
    clarify = None
    if act_sub in {"REQUEST", "ASK"}:
        if effective_intent in DB_INTENTS:
            clarify = repairs.maybe_clarify(act_major, effective_intent, merged_slots, state)
        elif effective_intent == "food_search":
            clarify = None
    if clarify:
        sys_hint += f" If key info is missing, ask exactly one short clarification question: {clarify!r}."

    # 3d) Tool calls (read-only)
    kg_result = None
    db_result = None

    # Try to satisfy follow-up detail from KG cache
    if _DETAIL_PAT.search(user_text or "") and getattr(state, "last_kg_rows", []):
        cached = _try_answer_from_kg_cache(user_text, state.last_kg_rows)
        if cached:
            kg_result = cached

    # Only hit KG if we didn't satisfy from cache
    if kg_result is None and effective_intent in KG_INTENTS:
        kg_result = answer_with_kg(payload, user_text, merged_slots, state)

    if effective_intent in DB_INTENTS and state.db_enabled:
        db_result = answer_with_db(payload, user_text, merged_slots, state)

    # 3e) Compose system messages
    sys_msgs = [{"role": "system", "content": sys_hint}]

    # Food search policy (persona-aware ranking) + inline context
    if kg_result and effective_intent == "food_search":
        price_hint = {
            "premium": "Prefer nicer places and higher price range when ranking.",
            "mid": "Prefer balanced/average price range.",
            "budget": "Prefer cheaper options when ranking."
        }.get(up.get("price_band", "mid"), "")
        policy = (
            "When Knowledge graph context is provided for a food/place request, "
            "first list up to 3 options as bullets: 'Name — Address (optionally price/rating)'. "
            + price_hint +
            "Then ask exactly one short follow-up, preferably a neighborhood or cuisine. "
            "Do not ask about date or price; infer price preferences from the user's persona. "
        )
        sys_msgs.append({"role": "system", "content": policy})
        sys_msgs.append({"role": "system", "content": f"Knowledge graph context:\n{kg_result}"})

    if db_result:
        sys_msgs.append({"role": "system", "content": f"Database context:\n{db_result}"})

    # Prepend system messages; exclude external system prompts
    enriched["messages"] = sys_msgs + user_and_assistant_only

    # 4) Stream reply from main LLM
    return stream_llm_reply(enriched)


# -------------------------
# Helpers
# -------------------------

# IT/HR tokens for full-access recognition
_FULL_ACCESS_DEPT_TOKENS = {
    "it", "information technology", "πληροφορική",
    "hr", "human resources", "ανθρώπινοι πόροι",
}

def _canon_dept_simple(s: str | None) -> str | None:
    """Return 'IT' or 'HR' if s matches those families; else None."""
    if not s:
        return None
    t = (s or "").strip().lower()
    if t in {"it", "information technology", "πληροφορική"}:
        return "IT"
    if t in {"hr", "human resources", "ανθρώπινοι πόροι"}:
        return "HR"
    return None

def _last_name(full: str) -> str:
    full = (full or "").strip()
    if not full:
        return ""
    parts = [p for p in full.split() if p]
    return parts[-1]

def _addressing_hint(up: Dict[str, Any]) -> str:
    """
    Addressing etiquette (EN-only):
    - Not verified: no name usage.
    - Verified & level ≤2: avoid names; if unavoidable use 'Mr./Ms. <LastName>'.
    - Verified & level 3–4: prefer no name; if needed 'Mr./Ms. <LastName>'.
    - Verified & level ≥5: first name once; never full name.
    - Never echo full name verbatim.
    """
    verified = bool(up.get("verified"))
    lvl = up.get("role_level")
    name = (up.get("name") or "").strip()
    ln = _last_name(name)

    if not verified:
        return ("Addressing: Identity not verified—do not use any name. "
                "Use neutral forms and keep responses concise.")

    if lvl is not None and lvl <= 2:
        return (f"Addressing: Senior leadership (level ≤2). "
                f"Do NOT use first or full name. If addressing is unavoidable, use exactly 'Mr./Ms. {ln}'.")
    if lvl is not None and 3 <= lvl <= 4:
        return (f"Addressing: Level 3–4. Prefer no name; if necessary, use 'Mr./Ms. {ln}'. "
                "Never use the full name.")
    if name:
        first = name.split()[0]
        return (f"Addressing: Level ≥5. You may use the first name '{first}' once if natural; "
                "do not repeat and never use the full name.")
    return "Addressing: Neutral; avoid using a name unless explicitly invited."

def _system_hint_base(act_major: str, act_sub: str, intent: str, mood: str, state: DialogueState, user_text: str = "") -> str:
    facts = state.recent_facts(k=3)
    facts_brief = _summarize_facts(facts)

    # Style from persona
    up = state.user_profile
    tone = up.get("tone", "neutral")
    verbosity = up.get("verbosity", "normal")
    style_hint = style.for_mood_and_user(mood, {"tone": tone})
    persona = state.persona_brief()

    etiquette = (
        "Language: English only. "
        + _addressing_hint(up)
        + " Avoid filler like “I've noted your preferences” or “awaiting your reply”; be crisp and concrete. "
        "Never echo the user's full name."
    )

    base = (
        "You are the company assistant. "
        f"{etiquette} "
        "Use brief clarifying questions only when essential information is missing; otherwise answer directly. "
        "Be concrete and concise. Avoid restating the user’s slots back to them. "
        f"Style: {style_hint} (verbosity={verbosity}). Act: {act_major}/{act_sub}. Intent: {intent}. "
        f"Known slots: {state.as_short_string()}. "
        f"Persona: {persona}. "
        f"Recent tool facts: {facts_brief}."
    )

    if intent == "food_search":
        band = up.get("price_band", "mid")
        band_text = {
            "budget": "Price band: budget (typical restaurants ~€12–25 per person).",
            "mid":    "Price band: mid-range (typical restaurants ~€15–35 per person).",
            "premium":"Price band: premium (typical restaurants ~€30–60 per person).",
        }.get(band, "Price band: mid-range (typical restaurants ~€15–35 per person).")
        base += " Prefer real results from Knowledge graph context when available. " + band_text
    elif intent == "db_query":
        base += " Prefer real results from Database context when available."
        # Staff-access policy hint
        text_low = (user_text or "").lower()
        is_staff_like = ("staff" in text_low) or ("employee" in text_low)
        lvl = up.get("role_level")
        dept_raw = up.get("department")
        dept_can = _canon_dept_simple(dept_raw)

        if is_staff_like:
            if dept_can in {"IT", "HR"} or (lvl is not None and lvl <= 2):
                base += " Policy: User has full cross-department access for staff listings (IT/HR or senior leadership)."
            elif (lvl is None or lvl >= 3):
                if dept_raw:
                    base += f" Policy: For staff listings, restrict to the user's department '{dept_raw}' and briefly note the restriction."
                else:
                    base += " Policy: For staff listings, do not reveal cross-department information if the user's department is unknown; ask them to confirm their department."

    return base

# Name token patterns (Titlecase words/initials)
_CAP_NAME_WORD = r"(?:[A-Z]\.|[A-Z][a-z]+(?:[-'][A-Z][a-z]+)*)"
_CAP_NAME_GROUP = rf"(?P<name>{_CAP_NAME_WORD}(?:\s+{_CAP_NAME_WORD}){{1,3}})"

# Lead-in patterns are case-insensitive; captured name validated after
_FLEX_NAME_WORD = r"(?:[A-Za-z]\.|[A-Za-z][a-z]+(?:[-'][A-Za-z][a-z]+)*)"
_FLEX_NAME_GROUP = rf"(?P<name>{_FLEX_NAME_WORD}(?:\s+{_FLEX_NAME_WORD}){{1,3}})"

_NAME_PATTERNS = [
    re.compile(rf"\bmy name is\s+{_FLEX_NAME_GROUP}\b", re.I),
    re.compile(rf"\bi am\s+{_FLEX_NAME_GROUP}\b", re.I),
    re.compile(rf"\bi['’]m\s+{_FLEX_NAME_GROUP}\b", re.I),
    re.compile(rf"\bthis is\s+{_FLEX_NAME_GROUP}\b", re.I),
    re.compile(rf"\bcall me\s+{_FLEX_NAME_GROUP}\b", re.I),
    re.compile(rf"\bfull name\s*(?:is|:)\s*{_FLEX_NAME_GROUP}\b", re.I),
    re.compile(rf"\bname\s*[:\-]\s*{_FLEX_NAME_GROUP}\b", re.I),
]

# Fallbacks that only match clearly name-like tokens (case-sensitive)
_FALLBACK_TWO_TOKENS = re.compile(rf"^{_CAP_NAME_GROUP}$")
_FALLBACK_LEADING_NAME = re.compile(rf"^{_CAP_NAME_GROUP}\s+(?:speaking|here)\b")

def _looks_like_name(name: str) -> bool:
    tokens = name.strip().split()
    has_namey = False
    for t in tokens:
        if re.fullmatch(r"[A-Z]\.", t):
            has_namey = True; continue
        if re.fullmatch(r"[A-Z][a-z]+(?:[-'][A-Z][a-z]+)*", t):
            has_namey = True; continue
    return has_namey

def _normalize_name(s: str) -> str:
    def cap_token(tok: str) -> str:
        if not tok: return tok
        if len(tok) == 2 and tok[1] == "." and tok[0].isalpha():
            return tok[0].upper() + "."
        if "-" in tok: return "-".join(cap_token(x) for x in tok.split("-"))
        if "'" in tok: return "'".join(cap_token(x) for x in tok.split("'"))
        return tok[0].upper() + tok[1:].lower()
    parts = [w.strip(",.;:!?") for w in s.strip().split()]
    return " ".join(cap_token(p) for p in parts if p)

def _extract_full_name(text: str) -> str | None:
    if not text:
        return None
    for pat in _NAME_PATTERNS:
        m = pat.search(text)
        if m:
            candidate = m.group("name").strip()
            if _looks_like_name(candidate):
                return _normalize_name(candidate)
    m = _FALLBACK_TWO_TOKENS.search(text.strip())
    if m:
        return _normalize_name(m.group("name"))
    m = _FALLBACK_LEADING_NAME.search(text.strip())
    if m:
        return _normalize_name(m.group("name"))
    return None

def _mentions_anonymous(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in ["stay anonymous", "skip name", "don't save my name", "be anonymous", "anonymous"])

def _latest_user(payload: Dict[str, Any]) -> str:
    for m in reversed(payload.get("messages", [])):
        if m.get("role") == "user":
            return m.get("content", "")
    return ""

def _stream_text(text: str):
    def gen():
        chunk = json.dumps({'choices': [{'delta': {'content': text}}]}, ensure_ascii=False)
        yield f"data: {chunk}\n\n"
        yield "data: [DONE]\n\n"
    return gen()

# ---------- Tone-aware quick acks ----------

def _ack_for_level(subtype: str, lvl: int | None) -> str:
    """
    Short acknowledgment tuned by seniority:
    lvl ≤2 → formal, 3–4 → neutral, ≥5 → casual, None → neutral.
    """
    if lvl is None:
        base = {
            "GREET": "Hello. How may I assist you?",
            "THANK": "You're welcome.",
            "GOODBYE": "Goodbye.",
            "APOLOGIZE": "Understood.",
        }
        return base.get(subtype, "Understood.")

    if lvl <= 2:
        formal = {
            "GREET": "Good day. How may I assist you?",
            "THANK": "You're welcome.",
            "GOODBYE": "Goodbye.",
            "APOLOGIZE": "Understood.",
        }
        return formal.get(subtype, "Understood.")
    elif 3 <= lvl <= 4:
        neutral = {
            "GREET": "Hello—how can I help?",
            "THANK": "You're welcome.",
            "GOODBYE": "Goodbye.",
            "APOLOGIZE": "No problem.",
        }
        return neutral.get(subtype, "Okay.")
    else:
        casual = {
            "GREET": "Hi! How can I help?",
            "THANK": "You're welcome!",
            "GOODBYE": "Talk soon!",
            "APOLOGIZE": "No worries—let’s continue.",
        }
        return casual.get(subtype, "OK.")

def _quick_ack(subtype: str, up: Dict[str, Any] | None = None) -> str:
    lvl = (up or {}).get("role_level") if up else None
    return _ack_for_level(subtype, lvl)

def _summarize_facts(facts: Any) -> str:
    if not facts:
        return "none"
    lines = []
    for f in facts:
        src = f.get("source", "?")
        cnt = f.get("count", 0)
        when = f.get("when", "")
        lines.append(f"{src}:{cnt}@{when}")
    return "; ".join(lines)

# ---------- KG cache detail lookup ----------

def _try_answer_from_kg_cache(user_text: str, rows: Any) -> str | None:
    """
    Return a single-item 'Results:' block from cached KG rows if the user's text
    asks for details about one of them (by number or label containment).
    """
    if not rows:
        return None
    t = (user_text or "").lower()

    # Try by numeric suffix (e.g., "Restaurant 132")
    m = _NUM_IN_LABEL.search(t)
    target_num = m.group(1) if m else None

    best = None
    for b in rows:
        get = lambda k: (b.get(k) or {}).get("value")
        label = (get("label") or get("name") or get("place") or "").strip()
        lab_low = label.lower()
        if not label:
            continue
        if target_num and target_num in lab_low:
            best = b; break
        # fallback: fuzzy containment of any quoted span
        # e.g., "more on Kolonaki Restaurant 132"
        tokens = [w for w in re.split(r"\W+", t) if w]
        if tokens and all(w in lab_low for w in tokens if w.isalpha() and len(w) > 3):
            best = b

    if not best and _DETAIL_PAT.search(t):
        # weak fallback: choose the top row
        best = rows[0]

    if not best:
        return None

    getb = lambda k: (best.get(k) or {}).get("value")
    label = getb("label") or getb("name") or getb("place")
    addr  = getb("address")
    price = getb("price") or getb("averagePricePerPerson")
    rate  = getb("rating") or getb("avgRating")
    cuis  = getb("cuisine")
    parts = [
        label,
        addr,
        f"€{price}" if price else None,
        f"rating {rate}" if rate else None,
        f"cuisine {cuis}" if cuis else None,
    ]
    parts = [p for p in parts if p]
    line = "• " + " — ".join(parts) if parts else f"• {label or '(item)'}"
    return "Results:\n" + line
