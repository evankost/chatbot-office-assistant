# features/sentiment.py
# Lightweight lexicon + windowed modifiers sentiment (normalized to [-1, 1])

from typing import Literal

LEX_POS = {"great","awesome","amazing","wonderful","perfect","fantastic","love","happy","glad","cool","nice","excellent"}
LEX_NEG = {"angry","annoyed","frustrated","upset","tired","exhausted","stressed","sad","terrible","horrible","awful","hate","bad","disappointed"}
NEGATORS = {"not","no","never","hardly","barely","scarcely"}
BOOSTERS = {"very","so","really","extremely","super"}
DIMINISH = {"slightly","a bit","somewhat","kinda","sort of"}

def _tokenize(t: str) -> list[str]:
    # Lowercase tokenization with naive '!' separation
    return [w for w in t.replace("!", " ! ").lower().split() if w]

def _score(tokens: list[str]) -> float:
    # Lexicon scoring with negation/boost/diminish in a backward window; normalized to [-1,1]
    s = 0.0
    window = 3
    for i, w in enumerate(tokens):
        val = 0.0
        if w in LEX_POS: val = 1.0
        elif w in LEX_NEG: val = -1.0
        if val == 0.0: continue

        # scan back for modifiers within a small window
        negated = False
        boost = 1.0
        for j in range(max(0, i-window), i):
            ww = tokens[j]
            if ww in NEGATORS: negated = not negated
            elif ww in BOOSTERS: boost += 0.25
            elif ww in DIMINISH: boost -= 0.25

        if negated: val = -val
        s += val * boost

    # punctuation nudge
    if "!" in tokens: s += 0.2

    # clamp and normalize
    if s > 2.5: s = 2.5
    if s < -2.5: s = -2.5
    return s / 2.5

def get_mood(text: str) -> Literal["positive","negative","neutral"]:
    # Discrete mood from normalized score with small deadband
    t = (text or "").strip()
    if not t: return "neutral"
    tokens = _tokenize(t)
    s = _score(tokens)
    if s > 0.15: return "positive"
    if s < -0.15: return "negative"
    return "neutral"

def get_score(text: str) -> float:
    # Return normalized sentiment score in [-1, 1]
    tokens = _tokenize(text or "")
    return _score(tokens)
