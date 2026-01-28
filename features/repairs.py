# features/repairs.py
from typing import Optional, Dict
import re

# Hedges/disfluency patterns and self-repair cues (for lightweight input cleanup)
HEDGES = [
    r"\buh+\b", r"\bum+\b", r"\berm+\b", r"\bwell[, ]\b", r"\bok(ay)?,?\b",
    r"\bsort of\b", r"\bkinda\b", r"\bperhaps\b", r"\bmaybe\b", r"\bi guess\b"
]
REPAIR_LEADS = [
    r"\bi mean\b", r"\bsorry(,)? i meant\b", r"\bto clarify\b", r"\bactually\b"
]
HEDGES_PAT = re.compile("|".join(HEDGES), re.I)
REPAIR_PAT = re.compile("|".join(REPAIR_LEADS), re.I)
MULTI_SPACE = re.compile(r"\s+")

def apply_self_repair(text: str) -> tuple[str, bool]:
    """Return (cleaned_text, changed): keeps content after a repair lead; strips hedges and extra spaces."""
    if not text:
        return text, False
    t = text
    # Keep only the segment after a repair lead (unless it's a cancel like "never mind")
    m = REPAIR_PAT.search(t)
    if m and "never mind" not in t.lower():
        t = t[m.end():]
    # Remove filler/hedges and collapse whitespace
    t2 = HEDGES_PAT.sub(" ", t)
    t2 = MULTI_SPACE.sub(" ", t2).strip()
    return (t2, t2 != text)

def maybe_clarify(act_major: str, intent: str, slots: Dict, state) -> Optional[str]:
    """Return one short clarification question only when a critical slot is missing for the detected intent."""
    slots = slots or {}

    # Do not clarify if the user cancelled
    if slots.get("cancel"):
        return None

    # KG venue search: prioritize neighborhood/type; infer price from persona
    if intent == "food_search":
        if "neighborhood" not in slots and "near" not in slots and not state.slots.get("neighborhood"):
            return "Any neighborhood preference in Athens (e.g., Kolonaki, Koukaki), or should I search all of Athens?"
        if "type" not in slots:
            return "Are you thinking of a restaurant, cafe, or bar?"
        return None

    # DB queries (read-only): require date or person when applicable
    if intent in {"db_query", "check_tasks", "free_slots"}:
        if "person" in slots and "date" not in slots and "time" not in slots:
            return "For which date or time window should I check?"
        if ("date" in slots or "time" in slots) and "person" not in slots:
            return "Do you want me to filter by a specific person or team?"
        return None

    # Generic directives with zero detail: ask for a single concrete detail
    if act_major == "DIRECTIVE" and not slots:
        return "Could you give me one detail so I can be precise?"

    return None
