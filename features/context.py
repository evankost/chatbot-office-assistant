# features/context.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from datetime import datetime
from hashlib import blake2b

# Ephemeral slot keys that should not persist across turns
EPHEMERAL_SLOT_KEYS = {"confirm", "cancel"}

@dataclass
class ToolEvent:
    """Structured record of a tool interaction and its result."""
    source: str                     # "db" | "kg" | "llm" | "system"
    subtype: str                    # e.g., "select", "sparql", "verbalization"
    request: Dict[str, Any]         # issued query/params/slots
    response: Dict[str, Any]        # normalized rows/bindings/text
    meta: Dict[str, Any] = field(default_factory=dict)  # timing/counts/errors
    at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

@dataclass
class Turn:
    """One dialogue turn with optional tool events attached."""
    role: str                       # "user" | "assistant" | "system"
    text: str
    act_major: Optional[str] = None
    act_subtype: Optional[str] = None
    intent: Optional[str] = None
    slots: Dict[str, Any] = field(default_factory=dict)
    mood: Optional[str] = None
    tool_events: List[ToolEvent] = field(default_factory=list)
    at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

def _topic_fingerprint(intent: str, slots: Dict[str, Any]) -> str:
    """Compact topic hash from intent and salient slots."""
    keys = ["intent", "type", "neighborhood", "place", "person", "date", "time"]
    sig = "|".join(str((k, slots.get(k))) for k in keys)
    return blake2b(sig.encode("utf-8"), digest_size=8).hexdigest()

@dataclass
class DialogueState:
    # Durable slot memory across turns
    slots: Dict[str, Any] = field(default_factory=dict)

    # User profile and defaults
    user_profile: Dict[str, Any] = field(default_factory=lambda: {
        "tone": "neutral",
        "role": "employee",
        "role_level": None,         # 0=CEO .. higher=lower rank
        "department": None,
        "name": None,
        "staff_id": None,
        "verified": False,
        "privacy_mode": "ask",      # "ask" | "anonymous" | "identified"
        "price_band": "mid",        # "budget" | "mid" | "premium"
        "verbosity": "normal"       # "brief" | "normal" | "detailed"
    })

    history: List[Turn] = field(default_factory=list)
    db_enabled: bool = False

    # Lightweight tracking and policies
    history_intents: List[str] = field(default_factory=list)
    next_expected: Optional[str] = None
    pending_action: Optional[Dict[str, Any]] = None
    last_sentiment: str = "neutral"

    # One-time onboarding gate (EN-only)
    asked_name_once: bool = False

    # KG caches
    last_kg_rows: List[Dict[str, Any]] = field(default_factory=list)                # last list results
    kg_detail_cache: Dict[str, Dict[str, Any]] = field(default_factory=dict)        # per-place detail rows

    # ---------- Update API ----------
    def add_user_turn(self, text: str, act_major: str, act_subtype: str,
                      intent: str, slots: Dict[str, Any], mood: str):
        """Append a user turn and merge durable slots."""
        self.history.append(Turn(role="user", text=text, act_major=act_major,
                                 act_subtype=act_subtype, intent=intent,
                                 slots=slots or {}, mood=mood))
        ephemeral = {"act_subtype", "confirm", "cancel"}
        for k, v in (slots or {}).items():
            if k in ephemeral:
                continue
            if v not in (None, "", False):
                self.slots[k] = v
        self.history_intents = (self.history_intents + [intent])[-6:]

    def add_assistant_turn(self, text: str, slots: Dict[str, Any] | None = None):
        """Append an assistant turn."""
        self.history.append(Turn(role="assistant", text=text, slots=slots or {}))

    def attach_tool_event(self, event: ToolEvent):
        """Attach a tool event to the latest turn."""
        if not self.history:
            self.history.append(Turn(role="system", text="boot"))
        self.history[-1].tool_events.append(event)

    # DB/KG logging hooks
    def log_db_result(self, sql: str, params: Dict[str, Any],
                      rows: List[Dict[str, Any]], elapsed_ms: int,
                      error: str | None = None):
        """Record a DB select result."""
        self.attach_tool_event(ToolEvent(
            source="db", subtype="select",
            request={"sql": sql, "params": params},
            response={"rows": rows, "count": len(rows)},
            meta={"elapsed_ms": elapsed_ms, "error": error or ""}
        ))

    def log_kg_result(self, sparql: str, bindings: List[Dict[str, Any]],
                      elapsed_ms: int, error: str | None = None):
        """Record a SPARQL query result and update the in-memory KG cache."""
        # Update the in-memory cache for follow-up detail questions
        self.last_kg_rows = list(bindings or [])

        # Attach a structured tool event for history/inspection
        self.attach_tool_event(ToolEvent(
            source="kg", subtype="sparql",
            request={"query": sparql},
            response={"bindings": bindings, "count": len(bindings)},
            meta={"elapsed_ms": elapsed_ms, "error": error or ""}
        ))

        # Keep a small rolling facts list for prompting
        try:
            self._recent_facts = getattr(self, "_recent_facts", [])
            self._recent_facts.append({"source": "kg", "count": len(self.last_kg_rows), "when": "now"})
            self._recent_facts = self._recent_facts[-5:]
        except Exception:
            pass

    # ---------- Prompting views ----------
    def recent_facts(self, k: int = 3) -> List[Dict[str, Any]]:
        """Return up to k recent db/kg facts for prompting."""
        facts: List[Dict[str, Any]] = []
        for turn in reversed(self.history):
            for ev in reversed(turn.tool_events):
                if ev.source in {"db", "kg"}:
                    facts.append({
                        "source": ev.source,
                        "summary": ev.response.get("rows") or ev.response.get("bindings"),
                        "count": ev.response.get("count"),
                        "when": ev.at
                    })
                    if len(facts) >= k:
                        return list(reversed(facts))
        return list(reversed(facts))

    def as_short_string(self) -> str:
        """Compact slot view for system hints."""
        keys = ["destination", "date", "time", "person", "place",
                "type", "near", "neighborhood", "cuisine", "sort", "limit"]
        view = {k: self.slots.get(k) for k in keys if self.slots.get(k) not in (None, "", False)}
        return str(view) if view else "no critical slots"

    # ---------- Onboarding ----------
    def needs_onboarding(self) -> bool:
        """True if we should ask for name once (ask-mode, not verified, not asked)."""
        up = self.user_profile
        return (
            up.get("privacy_mode") == "ask"
            and not up.get("verified", False)
            and not self.asked_name_once
        )

    # ---------- User modeling ----------
    def update_user_identity(self, name: Optional[str], staff_id: Optional[int],
                             role: Optional[str], role_level: Optional[int],
                             department: Optional[str], privacy_mode: str):
        """Update identity/profile and derive tone/verbosity/price prefs."""
        up = self.user_profile
        up["name"] = name or up.get("name")
        up["staff_id"] = staff_id
        up["role"] = role or up.get("role")
        up["role_level"] = role_level
        up["department"] = department
        up["verified"] = bool(staff_id)
        up["privacy_mode"] = privacy_mode

        if up["verified"] or privacy_mode == "anonymous":
            self.asked_name_once = True

        lvl = role_level if role_level is not None else 99
        if privacy_mode == "anonymous" or not up["verified"]:
            up["tone"] = "neutral"; up["verbosity"] = "normal"; up["price_band"] = "mid"
        else:
            if lvl <= 2:
                up["tone"] = "formal";  up["verbosity"] = "brief";    up["price_band"] = "premium"
            elif lvl <= 4:
                up["tone"] = "neutral"; up["verbosity"] = "normal";   up["price_band"] = "mid"
            else:
                up["tone"] = "casual";  up["verbosity"] = "detailed"; up["price_band"] = "budget"

    def persona_brief(self) -> str:
        """Short, single-line persona summary."""
        up = self.user_profile
        bits = [
            f"name={up.get('name') or 'unknown'}",
            f"role={up.get('role') or 'unknown'}",
            f"level={up.get('role_level') if up.get('role_level') is not None else 'n/a'}",
            f"dept={up.get('department') or 'n/a'}",
            f"privacy={up.get('privacy_mode')}",
            f"tone={up.get('tone')}",
            f"verbosity={up.get('verbosity')}",
            f"price_band={up.get('price_band')}",
            f"verified={up.get('verified')}",
        ]
        return " | ".join(bits)

    topic_id: Optional[str] = None
    last_entities: Dict[str, Any] = field(default_factory=lambda: {"person": None, "place": None, "venue": None})

    # --- Topic / entity tracking ---
    def update_topics_and_entities(self, intent: str, slots: Dict[str, Any]):
        """Track salient entities and soft-reset domain slots on topic shift."""
        if slots.get("person"): self.last_entities["person"] = slots["person"]
        if slots.get("place"):  self.last_entities["place"]  = slots["place"]
        if slots.get("type") or slots.get("neighborhood"):
            self.last_entities["venue"] = {
                "type": slots.get("type"),
                "neighborhood": slots.get("neighborhood"),
                "cuisine": slots.get("cuisine")
            }

        # Only domain-like turns affect topic/slot clearing
        domain_intents = {"food_search", "db_query", "place_info"}
        domain_keys = {
            "type","neighborhood","cuisine","near",
            "wifi","outdoor","veggie","alcohol",
            "reservations","payment","open_now",
            "price_min","price_max","rating_min","limit","sort",
            "date","time","person","place"
        }
        has_domain = any(k in (slots or {}) for k in domain_keys)
        topical = (intent in domain_intents) or has_domain
        if not topical:
            return

        new_topic = _topic_fingerprint(intent, slots or {})
        shifted = (self.topic_id is not None) and (new_topic != self.topic_id)

        if shifted:
            # Guard: do NOT wipe KG caches on simple detail turns
            # If this is a place_info turn AND the user didn't introduce new conflicting venue slots,
            # keep last_kg_rows / kg_detail_cache so unquoted follow-ups still work.
            introduced_conflict = any(slots.get(k) for k in ("type", "neighborhood", "cuisine"))
            if not (intent == "place_info" and not introduced_conflict):
                # Normal topic shift: clear domain slots and caches
                for k in list(self.slots.keys()):
                    if k in {"type","neighborhood","cuisine","near","wifi","outdoor","veggie","alcohol",
                             "reservations","payment","open_now","price_min","price_max","rating_min","limit","sort",
                             "date","time","person","place"}:
                        self.slots.pop(k, None)
                self.last_kg_rows.clear()
                self.kg_detail_cache.clear()
                self.next_expected = None

        self.topic_id = new_topic

    # --- Minimal reference resolution ---
    def resolve_references(self, text: str, slots: Dict[str, Any]) -> Dict[str, Any]:
        """Borrow prior entities for pronouns/deictics when current slots are empty."""
        t = (text or "").lower()
        if ("him" in t or "her" in t or "them" in t) and not slots.get("person"):
            if self.last_entities.get("person"):
                slots["person"] = self.last_entities["person"]
        if ("there" in t or "that place" in t or "it" in t) and not slots.get("place") and not slots.get("neighborhood"):
            le = self.last_entities.get("venue") or {}
            if le.get("neighborhood"): slots["neighborhood"] = le["neighborhood"]
            if le.get("type") and not slots.get("type"): slots["type"] = le["type"]
        return slots
