# backends/llm_client.py
import time
import requests
from ..config import KOBOLDCPP_URL_MAIN, REQUEST_TIMEOUT_S, KEEPALIVE_S

def stream_llm_reply(payload):
    """Proxy-stream responses from the main LLM endpoint using SSE framing."""
    def gen():
        last = time.time()  # last time we sent anything (for keep-alives)
        with requests.post(
            KOBOLDCPP_URL_MAIN,
            headers={"Content-Type": "application/json"},
            json=payload,
            stream=True,                  # stream server-sent events
            timeout=REQUEST_TIMEOUT_S,    # end-to-end request timeout
        ) as r:
            r.raise_for_status()          # propagate HTTP errors
            for raw in r.iter_lines(decode_unicode=True):
                now = time.time()
                if (now - last) > KEEPALIVE_S:
                    yield ": keep-alive\n\n"  # comment-style SSE ping to keep proxies open
                    last = now
                if raw and raw.startswith("data: "):
                    yield f"{raw}\n\n"       # forward model chunk verbatim
                    last = now
        yield "data: [DONE]\n\n"             # SSE sentinel to close the client stream
    return gen()
