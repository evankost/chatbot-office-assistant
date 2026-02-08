# create_db.py — create/ensure PostgreSQL schema
import os
import psycopg2

from ...config import DB_DSN

# schema DDL
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS staff (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  phone TEXT,
  role TEXT NOT NULL,         -- e.g., CEO, CTO, Director, Head of Department, Manager, Senior Engineer, Engineer, Intern
  role_level INT NOT NULL,    -- hierarchy level: 0=CEO (highest), bigger number = lower rank
  department TEXT,            -- e.g., R&D, Operations, UX, Finance, IT, Sales, HR
  manager_id INT NULL REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('open','done','blocked')),
  starts_at TIMESTAMP NOT NULL,
  assignee TEXT                 -- kept as text to match your current middleware; can be FK later
);

CREATE TABLE IF NOT EXISTS appointments (
  id SERIAL PRIMARY KEY,
  subject TEXT NOT NULL,
  person TEXT NOT NULL,         -- kept as text to match your current middleware; can be FK later
  room TEXT,
  starts_at TIMESTAMP NOT NULL,
  ends_at TIMESTAMP NOT NULL
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_staff_level ON staff(role_level);
CREATE INDEX IF NOT EXISTS idx_staff_manager ON staff(manager_id);
CREATE INDEX IF NOT EXISTS idx_tasks_starts_at ON tasks(starts_at);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee);
CREATE INDEX IF NOT EXISTS idx_appt_person ON appointments(person);
CREATE INDEX IF NOT EXISTS idx_appt_starts_at ON appointments(starts_at);
"""

def main():
    print(f"[create_db] Connecting to {DB_DSN}")  # log target
    with psycopg2.connect(DB_DSN) as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)  # create tables/indexes if missing
        conn.commit()
    print("[create_db] Schema created/ensured successfully.")  # success message

if __name__ == "__main__":
    main()
