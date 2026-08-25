"""Quota-safe routing for the independent Morning Briefing Groq pool."""

from __future__ import annotations

import os
from typing import Any, Iterable, Sequence


# These failures identify a key/account problem, so trying the next dedicated
# Morning Briefing key is useful.  Do not rotate for malformed requests.
KEY_ROTATION_STATUSES = frozenset({401, 402, 403, 429})
# A retired or unavailable model is independent of the API key.  Move directly
# to the next model once; retrying it on every key only produces noise.
MODEL_FALLBACK_STATUSES = frozenset({404})
JSON_VALIDATION_RETRIES = 1


def configured_news_keys(environ: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """Return only the dedicated Morning Briefing key slots, without duplicates."""
    environ = os.environ if environ is None else environ
    keys: list[tuple[str, str]] = []
    seen: set[str] = set()
    for env_name in ("GROQ_API_KEY_NEWS", "GROQ_API_KEY_NEWS_2"):
        value = environ.get(env_name, "").strip()
        if value and value not in seen:
            keys.append((env_name, value))
            seen.add(value)
    return keys


def status_code(error: Exception) -> int | None:
    status = getattr(error, "status_code", None)
    if status is None:
        status = getattr(getattr(error, "response", None), "status_code", None)
    return status


def is_json_generation_validation_error(error: Exception) -> bool:
    """Whether Groq rejected an empty/invalid structured-output generation.

    This is distinct from a malformed client request: it can occur after a
    model produces no JSON for an otherwise valid ``json_object`` request.
    """
    if status_code(error) != 400:
        return False
    message = str(error).lower()
    return "json_validate_failed" in message or "failed to validate json" in message


def complete_with_fallback(
    clients: Sequence[tuple[str, Any]],
    models: Iterable[str],
    logger: Any,
    **kwargs: Any,
) -> Any:
    """Complete using dedicated keys, then a model fallback when it is unavailable.

    No health checks are made.  A subsequent key is contacted only after a real
    key-specific failure.  A 404 advances the model chain immediately, avoiding
    repeated calls to a retired model on every key slot.  If all dedicated keys
    exhaust a model pool, the next model is tried before the run is abandoned.
    """
    model_chain = tuple(models)
    if not model_chain:
        raise ValueError("At least one Groq model is required")
    request_kwargs = dict(kwargs)
    purpose = request_kwargs.pop("purpose", "unspecified")

    last_error: Exception | None = None
    for model_index, model in enumerate(model_chain):
        for key_index, (key_env, client) in enumerate(clients):
            for attempt in range(JSON_VALIDATION_RETRIES + 1):
                try:
                    completion_kwargs = dict(request_kwargs)
                    if model.startswith("openai/gpt-oss"):
                        # Keep reasoning tokens bounded for this scheduled batch.
                        completion_kwargs["reasoning_effort"] = "low"
                    response = client.chat.completions.create(model=model, **completion_kwargs)
                    logger.info("🤖 Groq purpose=%s key_slot=%s model=%s", purpose, key_env, model)
                    return response
                except Exception as error:
                    last_error = error
                    if is_json_generation_validation_error(error) and attempt < JSON_VALIDATION_RETRIES:
                        logger.warning(
                            "⚠️ Groq purpose=%s model=%s non ha generato JSON valido, ritento una volta",
                            purpose, model,
                        )
                        continue
                    break

            status = status_code(last_error)

            if status in MODEL_FALLBACK_STATUSES and model_index + 1 < len(model_chain):
                logger.warning(
                    "⚠️ Groq purpose=%s model=%s HTTP %s, provo model fallback=%s",
                    purpose, model, status, model_chain[model_index + 1],
                )
                break

            if is_json_generation_validation_error(last_error) and model_index + 1 < len(model_chain):
                logger.warning(
                    "⚠️ Groq purpose=%s model=%s continua a non generare JSON valido, provo model fallback=%s",
                    purpose, model, model_chain[model_index + 1],
                )
                break

            if status in KEY_ROTATION_STATUSES and key_index + 1 < len(clients):
                logger.warning(
                    "⚠️ Groq purpose=%s key_slot=%s HTTP %s, provo la chiave NEWS successiva",
                    purpose, key_env, status,
                )
                continue

            if status in KEY_ROTATION_STATUSES and model_index + 1 < len(model_chain):
                logger.warning(
                    "⚠️ Groq purpose=%s modello=%s esaurito/non disponibile, provo model fallback=%s",
                    purpose, model, model_chain[model_index + 1],
                )
                break

            raise last_error

    raise last_error or RuntimeError("Nessun modello Groq Morning Briefing disponibile")
