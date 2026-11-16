from __future__ import annotations

"""
LLM provider chain.

The simulation uses an LLM in exactly two places: turning a sentence into
a structured ScenarioRequest, and writing the brief. All numbers come from
deterministic handlers that read Neo4j. So an LLM outage must degrade the
experience, never break it: callers catch LLMUnavailable and fall back to
the rule-based parser / template narrative.

Providers are tried in LLM_PROVIDER_ORDER (default: openai,gemini,openrouter).
A provider without a key is skipped. A provider that returns a quota or
rate-limit error is put on a short cooldown (a tiny circuit breaker), so
when the OpenAI quota runs out we stop paying a failed round trip on every
request and go straight to the next provider / rule parser.

OpenAI is called over plain HTTPS with httpx (already a dependency) rather
than the openai SDK, so no new package is needed.
"""

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
DEFAULT_OPENROUTER_MODEL = "openai/gpt-4o-mini"

_TIMEOUT_S = float(os.getenv("LLM_TIMEOUT_SECONDS", "20"))
_COOLDOWN_S = float(os.getenv("LLM_COOLDOWN_SECONDS", "300"))

# provider name -> unix time until which it is skipped
_cooldown_until: dict[str, float] = {}


class LLMUnavailable(RuntimeError):
    """Every configured provider failed or none is configured."""


class _NotConfigured(RuntimeError):
    """Provider has no API key set; skipped silently."""


class _QuotaError(RuntimeError):
    """Provider said: out of quota / rate limited. Triggers cooldown."""


@dataclass
class LLMResult:
    data: dict[str, Any]
    provider: str
    model: str


# =========================================================
# JSON EXTRACTION
# =========================================================

def extract_json(text: str) -> dict[str, Any]:
    """Parse a JSON object out of a model reply (tolerates ``` fences / prose)."""
    clean = text.strip()
    if "```" in clean:
        parts = clean.split("```")
        if len(parts) >= 2:
            clean = parts[1].lstrip()
            if clean.lower().startswith("json"):
                clean = clean[4:]
    clean = clean.strip()
    if not clean.startswith("{"):
        start, end = clean.find("{"), clean.rfind("}")
        if start != -1 and end > start:
            clean = clean[start:end + 1]
    parsed = json.loads(clean)
    if not isinstance(parsed, dict):
        raise ValueError("LLM reply was JSON but not an object")
    return parsed


# =========================================================
# PROVIDERS
# =========================================================

def _raise_for_status(provider: str, resp: httpx.Response) -> None:
    if resp.status_code in (402, 429):
        raise _QuotaError(f"{provider} {resp.status_code}: {resp.text[:300]}")
    if resp.status_code >= 400:
        raise RuntimeError(f"{provider} {resp.status_code}: {resp.text[:300]}")


def _openai_model() -> str:
    return os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL


def _call_openai(system: str, user: str, max_tokens: int) -> tuple[str, str]:
    key = os.getenv("OPENAI_API_KEY", "").strip()
    if not key:
        raise _NotConfigured("OPENAI_API_KEY not set")
    model = _openai_model()
    base = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")

    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_completion_tokens": max_tokens,
        # JSON mode: the model must return one JSON object. Both our system
        # prompts mention "JSON", which OpenAI requires for this mode.
        "response_format": {"type": "json_object"},
    }
    # Reasoning models (o-series, gpt-5*) only accept the default temperature.
    if not model.startswith(("o1", "o3", "o4", "gpt-5")):
        body["temperature"] = 0.1

    resp = httpx.post(
        f"{base}/chat/completions",
        json=body,
        headers={"Authorization": f"Bearer {key}"},
        timeout=_TIMEOUT_S,
    )
    _raise_for_status("openai", resp)
    content = resp.json()["choices"][0]["message"].get("content") or ""
    if not content.strip():
        raise RuntimeError("openai returned empty content")
    return content, model


def _call_gemini(system: str, user: str, max_tokens: int) -> tuple[str, str]:
    key = os.getenv("GEMINI_API_KEY", "").strip()
    if not key:
        raise _NotConfigured("GEMINI_API_KEY not set")
    model = os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip() or DEFAULT_GEMINI_MODEL
    resp = httpx.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
        headers={"x-goog-api-key": key},
        json={
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": [{"text": user}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": max_tokens,
                "responseMimeType": "application/json",
            },
        },
        timeout=_TIMEOUT_S,
    )
    _raise_for_status("gemini", resp)
    data = resp.json()
    parts = (data.get("candidates") or [{}])[0].get("content", {}).get("parts", [])
    text = "".join(p.get("text", "") for p in parts)
    if not text.strip():
        raise RuntimeError("gemini returned empty content")
    return text, model


def _call_openrouter(system: str, user: str, max_tokens: int) -> tuple[str, str]:
    key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not key:
        raise _NotConfigured("OPENROUTER_API_KEY not set")
    model = os.getenv("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL).strip() or DEFAULT_OPENROUTER_MODEL
    resp = httpx.post(
        "https://openrouter.ai/api/v1/chat/completions",
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1,
        },
        headers={"Authorization": f"Bearer {key}", "X-Title": "blue-orbit"},
        timeout=_TIMEOUT_S,
    )
    _raise_for_status("openrouter", resp)
    content = resp.json()["choices"][0]["message"].get("content") or ""
    if not content.strip():
        raise RuntimeError("openrouter returned empty content")
    return content, model


_PROVIDERS: dict[str, Callable[[str, str, int], tuple[str, str]]] = {
    "openai": _call_openai,
    "gemini": _call_gemini,
    "openrouter": _call_openrouter,
}

_KEY_ENV = {"openai": "OPENAI_API_KEY", "gemini": "GEMINI_API_KEY", "openrouter": "OPENROUTER_API_KEY"}


def provider_order() -> list[str]:
    raw = os.getenv("LLM_PROVIDER_ORDER", "openai,gemini,openrouter")
    return [p.strip().lower() for p in raw.split(",") if p.strip().lower() in _PROVIDERS]


def configured_providers() -> list[str]:
    return [p for p in provider_order() if os.getenv(_KEY_ENV[p], "").strip()]


def status() -> dict[str, Any]:
    now = time.time()
    return {
        "order": provider_order(),
        "configured": configured_providers(),
        "cooling_down": {p: round(t - now) for p, t in _cooldown_until.items() if t > now},
        "openai_model": _openai_model(),
    }


# =========================================================
# PUBLIC ENTRY POINT
# =========================================================

def complete_json(system: str, user: str, max_tokens: int = 800) -> LLMResult:
    """Ask the first healthy provider for a JSON object. Raises LLMUnavailable."""
    errors: list[str] = []
    now = time.time()
    for name in provider_order():
        if _cooldown_until.get(name, 0) > now:
            errors.append(f"{name}: cooling down after quota error")
            continue
        try:
            text, model = _PROVIDERS[name](system, user, max_tokens)
            return LLMResult(data=extract_json(text), provider=name, model=model)
        except _NotConfigured as exc:       # no key configured
            errors.append(f"{name}: {exc}")
        except _QuotaError as exc:
            _cooldown_until[name] = time.time() + _COOLDOWN_S
            logger.warning("LLM %s quota/rate limit, cooling down %ss: %s", name, _COOLDOWN_S, exc)
            errors.append(f"{name}: {exc}")
        except Exception as exc:            # network, bad JSON, 5xx
            logger.warning("LLM %s failed: %s", name, exc)
            errors.append(f"{name}: {exc}")
    raise LLMUnavailable("; ".join(errors) or "no LLM providers configured")
