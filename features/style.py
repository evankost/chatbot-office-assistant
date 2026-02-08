# features/style.py
from typing import Dict

def for_mood_and_user(mood: str, profile: Dict) -> str:
    """Compact style hint composed from mood + user profile (role, tone/formality, verbosity)."""
    p = profile or {}
    role = (p.get("role") or "employee").lower()
    formality = (p.get("formality") or p.get("tone") or "neutral").lower()
    verbosity = (p.get("verbosity") or "normal").lower()

    # Base tone by mood
    if mood == "negative":
        base = "calm, empathetic, concise"
    elif mood == "positive":
        base = "friendly, encouraging, concise"
    else:
        base = "neutral, polite, clear"

    # Formality inferred from explicit tone or senior role
    is_exec = any(k in role for k in ("ceo", "cfo", "coo", "cto", "director", "head", "manager", "executive"))
    wants_formal = formality in {"formal", "polite"} or is_exec

    if wants_formal:
        base += "; professional tone; avoid slang; no emojis"
    else:
        base += "; conversational but precise; no fluff"

    # Verbosity guardrails
    if verbosity == "brief":
        base += "; keep answers very short (2–4 sentences max)"
    elif verbosity == "detailed":
        base += "; add one short rationale or example if helpful"
    else:
        base += "; keep it succinct"

    # Output format nudge for busy roles
    if is_exec or verbosity == "brief":
        base += "; prefer bullets for lists; highlight key data first"

    return base
