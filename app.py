from flask import Flask, request, Response, stream_with_context
from .core.router import route_request
from .features.context import DialogueState
from . import config
import psycopg2

app = Flask(__name__)                     # Flask app
STATE = DialogueState()                   # shared dialogue state

# DB reachability check at startup
if config.DB_ENABLED:
    try:
        conn = psycopg2.connect(config.DB_DSN)
        conn.close()
        STATE.db_enabled = True
        print("Database reachable, enabling DB queries")
    except Exception as e:
        STATE.db_enabled = False
        print("Database configured but not reachable:", e)

@app.route("/v1/chat/completions", methods=["POST"])
def chat():
    payload = request.get_json(force=True, silent=True) or {}   # parse JSON payload
    gen = route_request(payload, STATE)                         # route and stream reply
    return Response(
        stream_with_context(gen),
        mimetype="text/event-stream",
        headers={
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )

if __name__ == "__main__":
    print("Middleware running at http://localhost:5100/v1/chat/completions")
    app.run(host="0.0.0.0", port=5100, threaded=True, debug=True)  # dev server
