from __future__ import annotations

import json
import logging
import os
import time

import requests
from dotenv import load_dotenv

# Modern 2026 SDK
from google import genai
from google.genai import types

load_dotenv()

from simulation.models import (
    ScenarioRequest,
    ScenarioResult,
    ScenarioType,
    SimulateRequest,
    SimulateResponse,
)
from simulation.prompts import (
    INTENT_SYSTEM_PROMPT,
    NARRATIVE_SYSTEM_PROMPT,
    build_narrative_user_prompt,
)
from simulation.registry import get_handler

logger = logging.getLogger(__name__)

# ── Model config (Updated for 2026) ───────────────────────────
# gemini-3-flash-preview is the current stable workhorse
GEMINI_MODEL = "gemini-3-flash-preview" 
# Use a high-availability fallback on OpenRouter
OPENROUTER_MODEL = "google/gemini-3.1-pro-preview"

# Initialize Client
_GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
client = None
if _GEMINI_API_KEY:
    client = genai.Client(api_key=_GEMINI_API_KEY)


# =========================================================
# LLM CALL HELPERS
# =========================================================

def _call_gemini(system: str, user: str, max_tokens: int = 1000) -> str:
    if not client:
        raise RuntimeError("GEMINI_API_KEY not set")

    # In 2026, we use 'thinking_config' to ensure clean JSON output
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=user,
        config=types.GenerateContentConfig(
            system_instruction=system,
            max_output_tokens=max_tokens,
            temperature=0.1,
            # 'minimal' thinking reduces latency and avoids 'thought' blocks in JSON
            thinking_config=types.ThinkingConfig(
                thinking_level=types.ThinkingLevel.MINIMAL 
            )
        ),
    )

    if not response.text:
        raise RuntimeError("Empty Gemini response")

    return response.text


def _call_openrouter(system: str, user: str, max_tokens: int = 1000) -> str:
    api_key = os.getenv("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "max_tokens": max_tokens,
        "temperature": 0.1,
    }

    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        json=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://simulation-engine.local", # Use a valid URL string
            "X-Title": "simulation-engine",
        },
        timeout=30,
    )

    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]


def _call_llm(system: str, user: str, max_tokens: int = 1000) -> str:
    try:
        return _call_gemini(system, user, max_tokens)
    except Exception as gemini_exc:
        logger.warning(f"Gemini failed: {gemini_exc}, falling back to OpenRouter")
        return _call_openrouter(system, user, max_tokens)


# =========================================================
# INTENT PARSING
# =========================================================

def parse_intent(raw_query: str, year: int = 2024, magnitude: float = 1.0) -> ScenarioRequest:
    try:
        response_text = _call_llm(
            system=INTENT_SYSTEM_PROMPT,
            user=raw_query,
            max_tokens=500,
        )

        # Enhanced 2026 JSON cleaning
        clean = response_text.strip()
        if "```" in clean:
            # Extract content between triple backticks
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]
        
        parsed = json.loads(clean.strip())

        return ScenarioRequest(
            scenario_type=ScenarioType(parsed.get("scenario_type", "unknown")),
            actor=parsed.get("actor"),
            target=parsed.get("target"),
            third_parties=parsed.get("third_parties", []),
            magnitude=float(parsed.get("magnitude", magnitude)),
            year=int(parsed.get("year", year)),
            raw_query=raw_query,
            extra_params=parsed.get("extra_params", {}),
        )

    except Exception as exc:
        logger.warning(f"Intent parsing failed: {exc}")
        return ScenarioRequest(
            scenario_type=ScenarioType.UNKNOWN,
            actor=None,
            target=None,
            raw_query=raw_query,
            year=year,
            magnitude=magnitude,
        )


# =========================================================
# NARRATIVE GENERATION
# =========================================================

def generate_narrative(result: ScenarioResult) -> tuple[str, str]:
    user_prompt = build_narrative_user_prompt(
        scenario_type=result.scenario_type.value,
        actor=result.actor,
        target=result.target,
        raw_query=result.raw_query,
        affected_countries=[c.model_dump() for c in result.affected_countries],
        cascade_effects=[c.model_dump() for c in result.cascade_effects],
        year=result.year,
    )

    try:
        response_text = _call_llm(
            system=NARRATIVE_SYSTEM_PROMPT,
            user=user_prompt,
            max_tokens=600,
        )

        clean = response_text.strip()
        if "```" in clean:
            clean = clean.split("```")[1]
            if clean.startswith("json"):
                clean = clean[4:]

        parsed = json.loads(clean.strip())

        return (
            parsed.get("headline", _default_headline(result)),
            parsed.get("summary", _default_summary(result)),
        )

    except Exception as exc:
        logger.warning(f"Narrative generation failed: {exc}")
        return _default_headline(result), _default_summary(result)


def _default_headline(result: ScenarioResult) -> str:
    actor = result.actor or ""
    target = result.target or ""
    stype = result.scenario_type.value.replace("_", " ")

    if actor and target:
        return f"{stype.title()}: {actor} → {target}"
    if target:
        return f"{stype.title()} in {target}"
    return f"{stype.title()} scenario"


def _default_summary(result: ScenarioResult) -> str:
    n = len(result.affected_countries)
    return (
        f"Simulation complete. {n} countries affected. "
        f"Confidence: {result.confidence:.0%}. "
        f"See affected_countries for projections."
    )


# =========================================================
# UNKNOWN HANDLER
# =========================================================

def _handle_unknown(req: ScenarioRequest) -> ScenarioResult:
    return ScenarioResult(
        scenario_type=req.scenario_type,
        actor=req.actor,
        target=req.target,
        raw_query=req.raw_query,
        year=req.year,
        headline=f"Scenario '{req.scenario_type.value}' not implemented",
        summary="No handler available for this scenario type.",
        confidence=0.0,
        missing_data=["Handler missing"],
    )


# =========================================================
# MAIN ENTRY POINT
# =========================================================

def run_simulation(api_request: SimulateRequest) -> SimulateResponse:
    t0 = time.perf_counter()

    parsed = parse_intent(
        raw_query=api_request.query,
        year=api_request.year,
        magnitude=api_request.magnitude,
    )

    handler = get_handler(parsed.scenario_type)

    if handler:
        try:
            result = handler(parsed)
        except Exception as exc:
            logger.error(f"Handler failed: {exc}", exc_info=True)
            result = _handle_unknown(parsed)
    else:
        result = _handle_unknown(parsed)

    if not result.headline or not result.summary:
        headline, summary = generate_narrative(result)
        result.headline = headline
        result.summary = summary

    result.computation_time_ms = round((time.perf_counter() - t0) * 1000, 1)

    return SimulateResponse(
        request=api_request,
        parsed=parsed,
        result=result,
    )