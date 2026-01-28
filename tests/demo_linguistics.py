# MIDDLEWARE/tests/demo_linguistics.py
"""
Updated demo harness for linguistic features (package-safe).
- Stubs are applied directly to router's local names (so they stick).
- Onboarding disabled (anonymous) so features are visible.
Run from project root:
  python -m MIDDLEWARE.tests.demo_linguistics
"""

import json, textwrap

# Package imports so core/router.py relative imports resolve
from ..core.router import route_request
from ..features.context import DialogueState, Turn
from ..core import router as _router  

# Offline deterministic stubs for backends
from  ..backends import llm_client as _llm
from  ..backends import kg_client as _kg
from  ..backends import db_client as _db

# -----------------------
# Stub implementations
# -----------------------

def _stub_stream_llm_reply(enriched_payload):
    sys_msgs = [m["content"] for m in enriched_payload.get("messages", []) if m.get("role") == "system"]
    # Last non-system message for display context
    reply_user_snippet = ""
    msgs = enriched_payload.get("messages", [])
    for m in reversed(msgs):
        if m.get("role") != "system":
            reply_user_snippet = m.get("content", "")[:160]
            break
    debug = "\n---[SYSTEM HINTS]---\n" + "\n\n".join(sys_msgs)
    content = debug + "\n\n" + "LLM(reply for): " + reply_user_snippet

    def gen():
        chunk = json.dumps({'choices': [{'delta': {'content': content}}]}, ensure_ascii=False)
        yield f"data: {chunk}\n\n"
        yield "data: [DONE]\n\n"
    return gen()

def _stub_answer_with_kg(payload, user_text, slots, state):
    hood = slots.get("neighborhood") or state.slots.get("neighborhood") or "Athens"
    vtype = slots.get("type") or "bar"
    state.log_kg_result("SPARQL ...", [{"name":"A"}, {"name":"B"}], elapsed_ms=25)
    return textwrap.dedent(f"""\
        - Demo {vtype.title()} A — {hood} Center
        - Demo {vtype.title()} B — {hood} Side Street
    """)

def _stub_answer_with_db(payload, user_text, slots, state):
    rows = [
        {"id": 101, "task": "Prepare Q3 report", "due": "2025-09-15"},
        {"id": 102, "task": "Ops sync",         "due": "2025-09-16"},
    ]
    state.log_db_result("SELECT ...", {}, rows, elapsed_ms=12)
    return "- #101 Prepare Q3 report — 2025-09-15\n- #102 Ops sync — 2025-09-16"

def _stub_lookup_staff_by_name_exact(name):
    table = {
        "Danielle Smith": {"id": 7, "name": "Danielle Smith", "role": "Head of Ops", "role_level": 3, "department": "Operations"},
        "Alex Johnson":   {"id": 12,"name": "Alex Johnson",   "role": "Engineer",    "role_level": 6, "department": "Engineering"},
    }
    return table.get(name)

# Patch backend modules and router’s local bindings
_llm.stream_llm_reply = _stub_stream_llm_reply
_kg.answer_with_kg = _stub_answer_with_kg
_db.answer_with_db = _stub_answer_with_db
_db.lookup_staff_by_name_exact = _stub_lookup_staff_by_name_exact

_router.stream_llm_reply = _stub_stream_llm_reply
_router.answer_with_kg = _stub_answer_with_kg
_router.answer_with_db = _stub_answer_with_db
_router.lookup_staff_by_name_exact = _stub_lookup_staff_by_name_exact

# -----------------------
# Helpers
# -----------------------

def _payload(user_text, prior=None):
    msgs = (prior or []) + [{"role": "user", "content": user_text}]
    return {"model": "demo", "messages": msgs, "stream": True}

def _drain(gen):
    buf = []
    for chunk in gen:
        if not chunk.startswith("data: "):
            continue
        data = chunk[6:].strip()
        if data == "[DONE]":
            break
        try:
            buf.append(json.loads(data)["choices"][0]["delta"]["content"])
        except Exception:
            buf.append(data)
    return "".join(buf)

def _last_user_turn_slots(state: DialogueState):
    # Return slots from the last user Turn
    for t in reversed(state.history):
        if isinstance(t, Turn) and t.role == "user":
            return t.slots or {}
    return {}

def show(title, state, text, out):
    turn_slots = _last_user_turn_slots(state)
    print("="*88)
    print(title)
    print("-"*88)
    print("USER:", text)
    print("TURN SLOTS:", turn_slots)
    print("STATE SLOTS:", state.slots)
    print("SLOT VIEW:", state.as_short_string())
    print("PERSONA:", state.persona_brief())
    print("TOPIC_ID:", state.topic_id)
    print("LAST_ENTITIES:", state.last_entities)
    print("---- STREAMED OUTPUT ----")
    print(out.strip())
    print()

# -----------------------
# Scenarios
# -----------------------

def scenario_disfluency_and_food_search(state):
    text = "uh ok, I mean, cafe near plaka that’s open now"
    out = _drain(route_request(_payload(text), state))
    show("1) Self-repair + KG food_search + open_now", state, text, out)

def scenario_reference_resolution(state):
    text1 = "Find cheap bars in Kolonaki"
    out1 = _drain(route_request(_payload(text1), state))
    show("2a) Initial venue query", state, text1, out1)

    text2 = "show me more there"
    out2 = _drain(route_request(_payload(text2), state))
    show("2b) Anaphora: 'there' resolves to Kolonaki", state, text2, out2)

def scenario_topic_shift_to_db(state):
    text = "check tasks for Danielle tomorrow"
    out = _drain(route_request(_payload(text), state))
    show("3) Topic shift: KG -> DB (slots soft-reset, DB results)", state, text, out)

def scenario_confirm_cancel(state):
    text1 = "book—oh never mind cancel that"
    out1 = _drain(route_request(_payload(text1), state))
    show("4a) Cancel path", state, text1, out1)

    state.pending_action = {"kind": "send_summary"}
    text2 = "yes, go ahead"
    out2 = _drain(route_request(_payload(text2), state))
    show("4b) Confirm path (pending_action fulfilled)", state, text2, out2)

def scenario_sentiment(state):
    for t in [
        "so happy with the result!",
        "not great, a bit disappointed",
        "ok thanks",
    ]:
        out = _drain(route_request(_payload(t), state))
        show("5) Sentiment adaptation", state, t, out)

def scenario_identity_and_etiquette(state):
    text1 = "My name is Danielle Smith"
    out1 = _drain(route_request(_payload(text1), state))
    show("6a) Identity capture + etiquette for level 3–4", state, text1, out1)

    text2 = "hello"
    out2 = _drain(route_request(_payload(text2), state))
    show("6b) Quick-ack tone by seniority", state, text2, out2)

def scenario_plural_and_anaphora(state):
    text1 = "find bars in Psyrri"
    out1 = _drain(route_request(_payload(text1), state))
    show("7a) Plural extraction (type=bar) with neighborhood alias", state, text1, out1)

    text2 = "another one there"
    out2 = _drain(route_request(_payload(text2), state))
    show("7b) Anaphora bias with 'another'", state, text2, out2)

def scenario_anaphora_without_context(state):
    # Fresh state: no prior venue turn
    fresh = DialogueState()
    fresh.update_user_identity(
        name=None, staff_id=None, role=None, role_level=None,
        department=None, privacy_mode="anonymous"
    )
    text1 = "show me more there"
    out1 = _drain(route_request(_payload(text1), fresh))
    show("8) Anaphora with no prior venue (should NOT force food_search)", fresh, text1, out1)

# -----------------------
# Main
# -----------------------

if __name__ == "__main__":
    st = DialogueState()
    st.db_enabled = True

    # Anonymous mode to focus on feature behavior
    st.update_user_identity(
        name=None, staff_id=None, role=None, role_level=None,
        department=None, privacy_mode="anonymous"
    )

    scenario_disfluency_and_food_search(st)
    scenario_reference_resolution(st)
    scenario_topic_shift_to_db(st)
    scenario_confirm_cancel(st)
    scenario_sentiment(st)
    scenario_identity_and_etiquette(st)
    scenario_plural_and_anaphora(st)
    scenario_anaphora_without_context(st)

    print("="*88)
    print("Demo complete. Review each block for slots, topics, entities, and system hints.")
