# seed_data.py — populate PostgreSQL with synthetic org, tasks, appointments
import os
import random
from datetime import datetime, timedelta, time

import psycopg2
from psycopg2.extras import RealDictCursor
from faker import Faker

DB_DSN = os.getenv("DB_DSN", "postgresql://postgres:mavic1245@localhost:5432/companydb")  # DSN from env with default

# knobs
STAFF_COUNT   = int(os.getenv("STAFF_COUNT", "200"))
DAYS_SPAN     = int(os.getenv("DAYS_SPAN", "32"))
TASKS_PER_DAY = int(os.getenv("TASKS_PER_DAY", "60"))
APPTS_PER_DAY = int(os.getenv("APPTS_PER_DAY", "40"))
FAKER_SEED    = int(os.getenv("FAKER_SEED", "13"))

# business-hour ranges
TASK_START_HOUR_MIN, TASK_START_HOUR_MAX = 8, 18
APPT_START_HOUR_MIN, APPT_START_HOUR_MAX = 9, 18
APPT_MIN_LEN_MIN, APPT_MIN_LEN_MAX       = 30, 60

fake = Faker(); Faker.seed(FAKER_SEED); random.seed(FAKER_SEED)  # deterministic seeding

# roles/levels
ROLE_LEVELS = [
    ("CEO", 0),
    ("CTO", 1), ("CFO", 1), ("COO", 1),
    ("Director", 2),
    ("Head of Department", 3),
    ("Manager", 4),
    ("Senior Engineer", 5),
    ("Engineer", 6),
    ("Intern", 7),
]
ROLE_TO_LEVEL = dict(ROLE_LEVELS)

# static pools
DEPARTMENTS = ["R&D", "Operations", "UX", "Finance", "IT", "Sales", "HR"]
ROOMS = ["A1","A2","A3","B1","B2","B3","C1","C2","C3"]
MEETING_SUBJECTS = [
    "Sprint planning", "1:1", "Design review", "Client call",
    "Quarterly results", "Roadmap sync", "Incident review",
    "Hiring panel", "Vendor negotiation"
]
TASK_TITLES = [
    "Prepare weekly report","Fix login bug","Refactor auth module","Update landing page","Plan usability test",
    "Implement caching layer","Write integration tests","Data cleanup","Migrate configuration","Optimize SQL queries",
]
TASK_STATUS_DISTRIBUTION = ["open"] * 6 + ["done"] * 3 + ["blocked"] * 1  # skew open

def connect():
    return psycopg2.connect(DB_DSN)  # open connection

def truncate_all(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE appointments, tasks, staff RESTART IDENTITY CASCADE;")
    conn.commit()

def random_business_dt(day_offset, hour_min, hour_max):
    from datetime import UTC  # local import to avoid global dependency
    base = datetime.combine(datetime.now(UTC).date(), time(0,0))  # midnight UTC
    dt = base + timedelta(days=day_offset)
    hour = random.randint(hour_min, hour_max); minute = random.randint(0,59)
    return dt + timedelta(hours=hour, minutes=minute)

def build_staff(conn):
    """Generate org chart top-down and fill remaining positions."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # CEO
        ceo_name = f"{fake.first_name()} {fake.last_name()}"
        cur.execute(
            "INSERT INTO staff (name, email, phone, role, role_level, department, manager_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id;",
            (ceo_name, fake.unique.email(), fake.phone_number(), "CEO", 0, None, None)
        )
        ceo_id = cur.fetchone()["id"]

        # CxO layer
        cxos = []
        for role in ["CTO", "CFO", "COO"]:
            if random.random() < 0.9:
                name = f"{fake.first_name()} {fake.last_name()}"
                cur.execute(
                    "INSERT INTO staff (name, email, phone, role, role_level, department, manager_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id;",
                    (name, fake.unique.email(), fake.phone_number(), role, 1, None, ceo_id)
                )
                cxos.append(cur.fetchone()["id"])

        # Directors (one per department)
        directors = {}
        for dept in DEPARTMENTS:
            boss = random.choice(cxos) if cxos else ceo_id
            name = f"{fake.first_name()} {fake.last_name()}"
            cur.execute(
                "INSERT INTO staff (name, email, phone, role, role_level, department, manager_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id;",
                (name, fake.unique.email(), fake.phone_number(), "Director", 2, dept, boss)
            )
            directors[dept] = cur.fetchone()["id"]

        # Heads (1–2 per department)
        heads_by_dept = {d: [] for d in DEPARTMENTS}
        for dept in DEPARTMENTS:
            for _ in range(random.randint(1,2)):
                name = f"{fake.first_name()} {fake.last_name()}"
                cur.execute(
                    "INSERT INTO staff (name, email, phone, role, role_level, department, manager_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id;",
                    (name, fake.unique.email(), fake.phone_number(), "Head of Department", 3, dept, directors[dept])
                )
                heads_by_dept[dept].append(cur.fetchone()["id"])

        # Remaining staff
        def staff_count(cur):
            cur.execute("SELECT COUNT(*) AS n FROM staff;")
            return cur.fetchone()["n"]

        remaining = max(0, STAFF_COUNT - staff_count(cur))
        roles_pool = (["Manager"]*1 + ["Senior Engineer"]*2 + ["Engineer"]*5 + ["Intern"]*1)
        for _ in range(remaining):
            dept = random.choice(DEPARTMENTS)
            role = random.choice(roles_pool)
            level = ROLE_TO_LEVEL[role]
            mgr_id = random.choice(heads_by_dept[dept] or [directors[dept]])
            name = f"{fake.first_name()} {fake.last_name()}"
            cur.execute(
                "INSERT INTO staff (name, email, phone, role, role_level, department, manager_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s);",
                (name, fake.unique.email(), fake.phone_number(), role, level, dept, mgr_id)
            )
    conn.commit()

def seed_tasks(conn):
    with conn.cursor() as cur:
        day_offsets = list(range(-(DAYS_SPAN//4), (3*DAYS_SPAN//4)+1))  # spread around today
        for day in day_offsets:
            for _ in range(TASKS_PER_DAY):
                title = random.choice(TASK_TITLES)
                status = random.choice(TASK_STATUS_DISTRIBUTION)
                dt = random_business_dt(day, TASK_START_HOUR_MIN, TASK_START_HOUR_MAX)
                cur.execute("SELECT name FROM staff ORDER BY random() LIMIT 1;")
                assignee = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO tasks (title, status, starts_at, assignee) VALUES (%s,%s,%s,%s);",
                    (title, status, dt, assignee)
                )
    conn.commit()

def seed_appointments(conn):
    with conn.cursor() as cur:
        day_offsets = list(range(-(DAYS_SPAN//4), (3*DAYS_SPAN//4)+1))  # spread around today
        for day in day_offsets:
            for _ in range(APPTS_PER_DAY):
                subj = random.choice(MEETING_SUBJECTS)
                room = random.choice(ROOMS)
                start_dt = random_business_dt(day, APPT_START_HOUR_MIN, APPT_START_HOUR_MAX)
                length_min = random.randint(APPT_MIN_LEN_MIN, APPT_MIN_LEN_MAX)
                end_dt = start_dt + timedelta(minutes=length_min)
                cur.execute("SELECT name FROM staff ORDER BY random() LIMIT 1;")
                person = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO appointments (subject, person, room, starts_at, ends_at) "
                    "VALUES (%s,%s,%s,%s,%s);",
                    (subj, person, room, start_dt, end_dt)
                )
    conn.commit()

def main():
    print(f"[seed_data] Connecting to {DB_DSN}")  # log target
    with connect() as conn:
        print("[seed_data] Truncating existing data...")
        truncate_all(conn)
        print("[seed_data] Building staff with hierarchy...")
        build_staff(conn)
        print("[seed_data] Seeding tasks...")
        seed_tasks(conn)
        print("[seed_data] Seeding appointments...")
        seed_appointments(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM staff;"); staff_n = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM tasks;"); tasks_n = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM appointments;"); appt_n = cur.fetchone()[0]
        print(f"[seed_data] Done. staff={staff_n}, tasks={tasks_n}, appointments={appt_n}")

if __name__ == "__main__":
    main()
