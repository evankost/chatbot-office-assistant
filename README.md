# Smart Office Assistant — Local Chatbot

Local chatbot for corporate environments. A **Flask middleware** performs linguistic analysis (speech acts, intent, slots, anaphora, style) and routes requests to a **Knowledge Graph (Blazegraph/SPARQL)** or a **Relational Database (PostgreSQL, read-only)**. Final response generation is handled by **OpenHermes** (via KoboldCPP). Interface: **SillyTavern**.

---

## Structure

```text
chatbot-middleware/
├─ app.py               · Flask middleware + SSE
├─ config.py            · Settings (endpoints/ports/env)
├─ core/                · Intent & routing (router.py)
├─ features/            · speech_acts, sentiment, context, repairs, style
├─ backends/            · llm_client, kg_client, db_client, sparql_mapper
├─ tests/               · demo_linguistics.py, unit tests
├─ setup_extras/        · helpers (create_db.py, seed_data.py, athens_kg.py)
└─ .gitignore           · Environment and cache protection

```

---

## Quick Start

**Requirements:** Python 3.10+, KoboldCPP, Blazegraph, PostgreSQL (read-only role) 

1. Install dependencies

   `pip install -r requirements.txt`

2. Service Setup (Example ports)
   * KoboldCPP: 5001 (OpenHermes), 5002 (SPARQL LLM), 5003 (Text-to-SQL LLM)
   * Blazegraph: local SPARQL endpoint
   * PostgreSQL: **SELECT** only with read-only user
   * Configuration via `chatbot-middleware.config.py` or environment variables

3. Execution (run commands)

   `python -m chatbot-middleware.app`
   
   `python -m chatbot-middleware.tests.demo_linguistics`

---

## Environment (.env examples)

```env
LLM_MAIN_URL=http://localhost:5001
LLM_SPARQL_URL=http://localhost:5002
LLM_SQL_URL=http://localhost:5003
FLASK_HOST=0.0.0.0
FLASK_PORT=5100
BLAZEGRAPH_URL=http://localhost:9999/blazegraph/namespace/kb/sparql
POSTGRES_DSN=postgresql://readonly@localhost:5432/corpdb

```

> The demo (`tests/demo_linguistics.py`) runs **without** requiring Blazegraph/PostgreSQL/LLMs.

---

## Example Queries

* “Italian restaurant in **Plaka**, **open now**.” → KG/SPARQL
* “Show **my tasks** for **today**.” → DB/SQL (read-only)
* “**Another one there**, but **cheaper**.” → reference + re-ranking (cheaper)
* “List **upcoming appointments** for **Patricia Grant** after **15:00**.” → DB/SQL
* “Find **quiet cafés** with **Wi-Fi** near **Syntagma**.” → KG/SPARQL

---

## License

This project is licensed under the MIT License.
