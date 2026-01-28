# backends/db_client.py
from __future__ import annotations
import time
import re
import requests
from typing import List, Dict, Any, Optional

from ..config import KOBOLDCPP_URL_SQL, DB_DSN, DB_ENABLED, REQUEST_TIMEOUT_S

# Driver guard
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAVE_PG = True
except Exception:
    HAVE_PG = False

# Debug
VERBOSE_SQL = True
LAST_RAW_SQL: Optional[str] = None  # set inside _generate_sql_once()

# Canonical departments + synonyms
DEPARTMENTS = {"R&D", "Operations", "UX", "Finance", "IT", "Sales", "HR"}

_DEPT_SYNONYMS = {
    "R&D": {"r&d", "rnd", "research & development", "research and development"},
    "Operations": {"operations", "ops"},
    "UX": {"ux", "user experience", "design"},
    "Finance": {"finance", "fin"},
    "IT": {"it", "information technology", "it dept", "it department"},
    "Sales": {"sales", "bizdev", "business development"},
    "HR": {"hr", "human resources"},
}

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def canonical_department(s: Optional[str]) -> Optional[str]:
    """Map free text to a canonical department or None."""
    if not s:
        return None
    txt = _norm(s)
    for canon in DEPARTMENTS:
        if txt == _norm(canon):
            return canon
    for canon, toks in _DEPT_SYNONYMS.items():
        if txt in toks:
            return canon
    txt = txt.replace(" department", "").replace(" dept", "").strip()
    for canon in DEPARTMENTS:
        if txt == _norm(canon):
            return canon
    for canon, toks in _DEPT_SYNONYMS.items():
        if txt in toks:
            return canon
    return None

def _has_full_access(user_dept: Optional[str]) -> bool:
    """Full access coordinators: IT and HR."""
    return canonical_department(user_dept) in {"IT", "HR"}

def ping_db(dsn: str, timeout_s: int = 2) -> bool:
    """Light reachability check for /health or boot."""
    if not HAVE_PG:
        return False
    try:
        conn = psycopg2.connect(dsn, connect_timeout=timeout_s)
        conn.close()
        return True
    except Exception:
        return False

# System hint for text-to-SQL (tight schema + read-only)
SCHEMA_HINT = """
You are an expert PostgreSQL SQL generator.
RULES:
- Output ONE valid SQL statement ONLY. No explanations, no markdown, no backticks.
- MUST be READ-ONLY (SELECT / WITH). Never use INSERT/UPDATE/DELETE/DROP/CREATE.
- Use ONLY these tables/columns (public schema):
  staff(id,name,role,role_level,department,email,phone,manager_id)
  tasks(id,title,status,starts_at,assignee)
  appointments(id,subject,person,room,starts_at,ends_at)
Mappings:
- 'today' -> DATE(starts_at)=CURRENT_DATE
- 'tomorrow' -> DATE(starts_at)=CURRENT_DATE + INTERVAL '1 day'
- 'upcoming' -> starts_at >= NOW()
Filters & projections:
- Use ILIKE for ALL text filters (name, department, person, assignee) and include wildcards, e.g. '%IT%'.
- For STAFF-by-department requests, the canonical projection is:
  SELECT id, name, role FROM staff WHERE department ILIKE '%<DEPT>%' ORDER BY name ASC
- Never select a table name as a column. There is NO 'staff' column in the staff table.
- Prefer only the columns relevant to the request.
- Order upcoming by starts_at ASC. Use LIMIT when the user asks for top/next N.
- Never add columns/tables not listed. Never use double quotes for string literals.
IMPORTANT: Obey the LAST user message exactly.
"""

# Few-shots
FEWSHOTS_POOL = {
    # appointments
    "appt:list_upcoming_by_person": (
        "upcoming appointments for Alice Mark",
        "SELECT id, subject, person, room, starts_at "
        "FROM appointments "
        "WHERE person ILIKE '%Alice Mark%' AND starts_at >= NOW() "
        "ORDER BY starts_at ASC"
    ),
    "appt:next_n": (
        "next 5 appointments",
        "SELECT id, subject, person, room, starts_at "
        "FROM appointments "
        "WHERE starts_at >= NOW() "
        "ORDER BY starts_at ASC LIMIT 5"
    ),
    "appt:count_tomorrow": (
        "how many appointments do I have tomorrow",
        "SELECT COUNT(*) AS cnt "
        "FROM appointments "
        "WHERE DATE(starts_at) = CURRENT_DATE + INTERVAL '1 day'"
    ),

    # tasks
    "task:list_upcoming_by_assignee": (
        "upcoming tasks for Alex Trust",
        "SELECT id, title, status, starts_at, assignee "
        "FROM tasks "
        "WHERE assignee ILIKE '%Alex Trust%' AND starts_at >= NOW() "
        "ORDER BY starts_at ASC"
    ),

    # staff
    "staff:list_by_dept_canonical": (
        "list IT staff and their roles",
        "SELECT id, name, role FROM staff "
        "WHERE department ILIKE '%IT%' "
        "ORDER BY name ASC"
    ),
    "staff:nl_variant_can_you_give": (
        "can you give me the staff of Finance department?",
        "SELECT id, name, role FROM staff "
        "WHERE department ILIKE '%Finance%' "
        "ORDER BY name ASC"
    ),
    "staff:nl_variant_in_dept": (
        "staff in HR department",
        "SELECT id, name, role FROM staff "
        "WHERE department ILIKE '%HR%' "
        "ORDER BY name ASC"
    ),
}

def _needs_dept_guard(role_level: Optional[int]) -> bool:
    """Restrict role_level >= 3 (unknown treated as restricted)."""
    return role_level is None or role_level >= 3

def _select_fewshots(req: dict) -> list[tuple[str, str]]:
    """Pick up to 3 aligned examples; ensure a staff-by-dept canonical when relevant."""
    shots = []
    kind = req.get("kind")
    want_count = req.get("count")
    limit = req.get("limit")
    name = req.get("name")
    date = req.get("date")
    department = req.get("department")

    if kind == "appointments":
        if want_count and date == "tomorrow":
            shots.append(FEWSHOTS_POOL["appt:count_tomorrow"])
        if name or date == "upcoming":
            shots.append(FEWSHOTS_POOL["appt:list_upcoming_by_person"])
        if limit:
            shots.append(FEWSHOTS_POOL["appt:next_n"])

    elif kind == "tasks":
        shots.append(FEWSHOTS_POOL["task:list_upcoming_by_assignee"])

    elif kind == "staff":
        shots.append(FEWSHOTS_POOL["staff:list_by_dept_canonical"])
        shots.append(FEWSHOTS_POOL["staff:nl_variant_in_dept"])
        if department:
            shots.append((
                f"can you give me the staff of {department} department?",
                "SELECT id, name, role FROM staff "
                f"WHERE department ILIKE '%{department}%' "
                "ORDER BY name ASC"
            ))

    return shots[:3]

def _infer_request(user_text: str, user_dept: Optional[str] = None) -> dict:
    """Lightweight intent + department extraction; resolves 'my department' → user_dept."""
    text = (user_text or "").strip().lower()
    req = {"kind": None, "name": None, "date": None, "limit": None, "count": False, "department": None}

    if "appointment" in text:
        req["kind"] = "appointments"
    elif "task" in text:
        req["kind"] = "tasks"
    elif "staff" in text or "employee" in text:
        req["kind"] = "staff"

    if "today" in text:
        req["date"] = "today"
    elif "tomorrow" in text:
        req["date"] = "tomorrow"
    elif "upcoming" in text or "next" in text:
        req["date"] = "upcoming"

    if "how many" in text or "count" in text:
        req["count"] = True

    m = re.search(r"\bnext\s+(\d+)\b", text)
    if m:
        req["limit"] = int(m.group(1))

    # crude name capture
    m = re.search(r"\bfor\s+([a-z]+(?:\s+[a-z]+)*)", text)
    if m:
        req["name"] = m.group(1)

    # department capture
    patterns = [
        r"\b(?:staff|employees?)\s+(?:of|from|in)\s+([a-z0-9 &/\-]+?)\s+department\b",
        r"\b([a-z0-9 &/\-]+?)\s+department\s+(?:staff|employees?)\b",
        r"\bdepartment\s+of\s+([a-z0-9 &/\-]+?)\b",
    ]
    for p in patterns:
        md = re.search(p, text, re.I)
        if md:
            req["department"] = md.group(1).strip().title()
            break

    if not req["department"]:
        if user_dept and re.search(r"\bmy\s+department\b", text):
            req["department"] = user_dept

    if not req["department"]:
        md2 = re.search(r"\b([a-z0-9 &/\-]+?)\s+department\b", text, re.I)
        if md2 and md2.group(1).strip().lower() != "my":
            req["department"] = md2.group(1).strip().title()

    return req

def _expand_my_department_in_text(user_text: str, user_dept: Optional[str]) -> str:
    """Inline 'my department' with the concrete department (best-effort)."""
    if not (user_text and user_dept):
        return user_text or ""
    s = user_text
    s = re.sub(r"\b(of|from|in)\s+my\s+department\b", rf"\1 {user_dept} department", s, flags=re.I)
    s = re.sub(r"\bmy\s+department\b", f"{user_dept} department", s, flags=re.I)
    return s

# Turn first-person asks into explicit "for <Name>"
def _personalize_question(user_text: str, state) -> str:
    """Append 'for <Name>' for first-person appt/task asks when name is known."""
    try:
        name = (getattr(state, "user_profile", {}) or {}).get("name")
    except Exception:
        name = None

    if not name:
        return user_text or ""

    if not re.search(r"\b(my|me|i)\b", user_text or "", re.IGNORECASE):
        return user_text or ""

    req = _infer_request(user_text or "")
    kind = req.get("kind")
    has_name = bool(req.get("name"))

    if kind in ("appointments", "tasks") and not has_name:
        return f"{user_text} for {name}"

    return user_text or ""

def _build_messages(user_text: str,
                    user_name: Optional[str] = None,
                    req: Optional[dict] = None,
                    extra_hint: Optional[str] = None) -> List[Dict[str, str]]:
    """Compose messages for the SQL model (dept-aware + dynamic shots)."""
    req = req or _infer_request(user_text)
    few = _select_fewshots(req)

    msgs = [{"role": "system", "content": SCHEMA_HINT}]
    for q, sql in few:
        msgs.append({"role": "user", "content": q})
        msgs.append({"role": "assistant", "content": sql})

    # Dynamic few-shot for first-person
    if user_name and re.search(r"\b(my|me|i)\b", user_text, re.IGNORECASE):
        if req.get("kind") == "appointments":
            msgs.append({"role": "user", "content": "my upcoming appointments"})
            msgs.append({"role": "assistant", "content":
                f"SELECT id, subject, person, room, starts_at "
                f"FROM appointments "
                f"WHERE person ILIKE '%{user_name}%' AND starts_at >= NOW() "
                f"ORDER BY starts_at ASC"
            })
        elif req.get("kind") == "tasks":
            msgs.append({"role": "user", "content": "my upcoming tasks"})
            msgs.append({"role": "assistant", "content":
                f"SELECT id, title, status, starts_at, assignee "
                f"FROM tasks "
                f"WHERE assignee ILIKE '%{user_name}%' AND starts_at >= NOW() "
                f"ORDER BY starts_at ASC"
            })

    if extra_hint:
        msgs.append({"role": "system", "content": extra_hint})

    msgs.append({"role": "user", "content": user_text})
    return msgs

# Output normalization (syntax-only)
FENCE_RE = re.compile(r"^```[a-zA-Z]*\s*|\s*```$", re.MULTILINE)

def _strip_fences(s: str) -> str:
    return FENCE_RE.sub("", s or "").strip()

def _normalize_sql_spacing(raw: str) -> str:
    """Normalize quotes/spacing only; no semantic rewrites."""
    s = _strip_fences(raw)
    s = s.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Schema sanity + optional one-shot regeneration
_ALLOWED_COLS = {
    "staff": {"id","name","role","role_level","department","email","phone","manager_id"},
    "tasks": {"id","title","status","starts_at","assignee"},
    "appointments": {"id","subject","person","room","starts_at","ends_at"},
}

def _parse_projection_and_table(sql: str) -> tuple[list[str], Optional[str]]:
    m_sel = re.search(r"(?is)\bSELECT\s+(.*?)\s+FROM\s+([a-z_][a-z0-9_]*)", sql)
    if not m_sel:
        return [], None
    raw_sel = m_sel.group(1).strip()
    table = m_sel.group(2).lower()
    cols = [c.strip() for c in raw_sel.split(",")]
    return cols, table

def _has_invalid_projection(sql: str) -> bool:
    cols, table = _parse_projection_and_table(sql)
    if not table:
        return True
    if table not in _ALLOWED_COLS:
        return True
    allowed = _ALLOWED_COLS[table]

    for c in cols:
        c_lower = c.lower()
        if c == "*" or re.search(r"\w+\s*\(", c_lower):
            continue
        base = c_lower.split(" as ", 1)[0].strip()
        if base == table:
            return True
        if base not in allowed:
            return True
    return False

def _sql_has_dept_filter(sql: str) -> bool:
    return bool(re.search(r"\bdepartment\s+ilike\b", sql, re.I))

def _sql_uses_my_literal(sql: str) -> bool:
    return bool(re.search(r"department\s+ilike\s*'%\s*my\s*%'", sql, re.I))

def _sql_has_specific_dept(sql: str, dept: str) -> bool:
    pattern = re.escape(dept)
    return bool(re.search(rf"department\s+ilike\s*'%\s*{pattern}\s*%'", sql, re.I))

def _generate_sql_once(question: str,
                       user_name: Optional[str],
                       req: dict,
                       extra_hint: Optional[str] = None) -> str:
    global LAST_RAW_SQL
    payload = {
        "model": "text-to-sql",
        "messages": _build_messages(question, user_name=user_name, req=req, extra_hint=extra_hint),
        "temperature": 0.0,
        "max_tokens": 192,
        "stream": False,
    }
    r = requests.post(KOBOLDCPP_URL_SQL, json=payload, timeout=REQUEST_TIMEOUT_S)
    r.raise_for_status()
    content = r.json().get("choices", [{}])[0].get("message", {}).get("content", "") or ""
    LAST_RAW_SQL = _strip_fences(content)
    sql = _normalize_sql_spacing(LAST_RAW_SQL)
    return sql

def _generate_sql(question: str,
                  user_name: Optional[str] = None,
                  user_dept: Optional[str] = None,
                  role_level: Optional[int] = None) -> str:
    """Dept-aware generation with one retry if projection/guard is wrong."""
    req = _infer_request(question, user_dept=user_dept)
    expanded_q = _expand_my_department_in_text(question, user_dept)

    extra_hint = None
    if req.get("kind") == "staff" and _needs_dept_guard(role_level) and not _has_full_access(user_dept):
        if user_dept:
            extra_hint = (f"For staff listings, restrict results to department '{user_dept}'. "
                          f"Use the canonical projection: SELECT id, name, role FROM staff "
                          f"WHERE department ILIKE '%{user_dept}%' ORDER BY name ASC")
        else:
            extra_hint = ("For staff listings, do not reveal cross-department information when the user's department "
                          "is unknown; ask them to confirm their department in the SQL-friendly filter wording.")

    sql = _generate_sql_once(expanded_q, user_name, req, extra_hint=extra_hint)

    bad_projection = _has_invalid_projection(sql)
    dept_guard_needed = (req.get("kind") == "staff" and _needs_dept_guard(role_level) and not _has_full_access(user_dept))
    needs_retry = False
    retry_hint = None

    if bad_projection:
        needs_retry = True
        if req.get("kind") == "staff":
            tgt = user_dept or "<DEPT>"
            retry_hint = (f"Your previous SQL selected invalid columns or wrong projection. "
                          f"Use exactly: SELECT id, name, role FROM staff "
                          f"WHERE department ILIKE '%{tgt}%' ORDER BY name ASC")
        else:
            retry_hint = "Your previous SQL selected invalid columns. Use only actual columns from the schema."

    if dept_guard_needed:
        if user_dept:
            if not _sql_has_dept_filter(sql) or _sql_uses_my_literal(sql) or not _sql_has_specific_dept(sql, user_dept):
                needs_retry = True
                retry_hint = (f"Ensure the WHERE clause restricts results to department ILIKE '%{user_dept}%'. "
                              f"Projection should be: id, name, role. ORDER BY name ASC.")
        else:
            needs_retry = True
            retry_hint = ("The user's department is unknown. Produce a SQL statement that filters by department, "
                          "but first require the concrete department (e.g., '%Finance%').")

    if needs_retry:
        sql = _generate_sql_once(expanded_q, user_name, req, extra_hint=retry_hint)

    if VERBOSE_SQL:
        print("[DB][Final SQL   ]:", sql)

    first = (sql.strip().split(None, 1)[0] or "").upper()
    if first not in ("SELECT", "WITH"):
        raise ValueError(f"Blocked non-read SQL verb in: {sql[:80]}...")

    return sql

def _run_sql(sql: str) -> List[Dict[str, Any]]:
    with psycopg2.connect(DB_DSN) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        try:
            return cur.fetchall()
        except psycopg2.ProgrammingError:
            return []

def _verbalize_rows(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No results found."
    lines = []
    for r in rows[:10]:
        pairs = "; ".join(f"{k}: {v}" for k, v in r.items())
        lines.append("• " + pairs)
    return "\n".join(lines)

def _wants_sql_echo(user_text: str) -> bool:
    """Echo final SQL when explicitly requested."""
    return bool(re.search(r"\b(show|print)\b.*\b(sql|query)\b", user_text, re.I))

def _guess_user_department_from_slots(slots: Dict[str, Any]) -> Optional[str]:
    """Read the user's own department from slots (best-effort)."""
    for k in ("my_department", "user_department", "own_department", "department_self", "self_dept"):
        v = slots.get(k)
        if v:
            can = canonical_department(v)
            if can:
                return can
    return None

def answer_with_db(payload, user_text: str, slots, state, intent_override: Optional[str] = None) -> Optional[str]:
    """Generate SQL, run it, log in state, and return a short summary."""
    if not (DB_ENABLED and HAVE_PG):
        return None

    up = getattr(state, "user_profile", {}) or {}
    user_dept = canonical_department(up.get("department")) or _guess_user_department_from_slots(slots or {})
    role_level = up.get("role_level")

    effective_q = _personalize_question(user_text or "", state)
    try:
        user_name = (getattr(state, "user_profile", {}) or {}).get("name")
    except Exception:
        user_name = None

    req_probe = _infer_request(effective_q, user_dept=user_dept)

    # Identification required for staff listings
    if req_probe.get("kind") == "staff":
        if (up.get("privacy_mode") == "anonymous") or not user_name:
            return "ACCESS_LIMIT: Identification required to view staff by department. Please share your full name."

    # Unknown department for restricted roles → nudge
    if req_probe.get("kind") == "staff" and _needs_dept_guard(role_level) and not _has_full_access(user_dept) and not user_dept:
        return "ACCESS_LIMIT: Department is unknown. To list staff, please confirm your department."

    t0 = time.time()
    sql, rows, err = None, [], None
    try:
        final_q = effective_q if intent_override is None else f"{effective_q}\n(intent: {intent_override})"
        sql = _generate_sql(final_q, user_name=user_name, user_dept=user_dept, role_level=role_level)
        rows = _run_sql(sql)

        body = _verbalize_rows(rows)
        if _wants_sql_echo(user_text or ""):
            body = f"Final SQL:\n{sql}\n\n{body}"
        return body

    except Exception as e:
        err = str(e)
        return None
    finally:
        elapsed_ms = int((time.time() - t0) * 1000)
        try:
            params = {"raw_sql": LAST_RAW_SQL or "", "final_sql": sql or ""}
            state.log_db_result(sql=sql or "", params=params, rows=rows or [], elapsed_ms=elapsed_ms, error=err)
        except Exception:
            pass

# Deterministic staff lookup
def lookup_staff_by_name_exact(full_name: str) -> Optional[Dict[str, Any]]:
    """Exact match first; fallback to best ILIKE by highest seniority."""
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except Exception:
        return None

    try:
        with psycopg2.connect(DB_DSN) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, name, role, role_level, department "
                "FROM staff WHERE name = %s LIMIT 1;", (full_name,)
            )
            row = cur.fetchone()
            if row:
                return dict(row)

            cur.execute(
                "SELECT id, name, role, role_level, department "
                "FROM staff WHERE name ILIKE %s "
                "ORDER BY role_level ASC, id ASC LIMIT 1;",
                (f"%{full_name}%",)
            )
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception:
        return None
