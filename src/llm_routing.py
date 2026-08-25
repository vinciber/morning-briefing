"""Quota-safe routing for Groq NEWS primary, Gemini fallback, and Groq NEWS_2."""

from __future__ import annotations

import os
import json
import urllib.error
import urllib.request
from types import SimpleNamespace
from typing import Any, Iterable, Sequence


# These failures identify a key/account problem, so trying the next dedicated
# Morning Briefing key is useful.  Do not rotate for malformed requests.
KEY_ROTATION_STATUSES = frozenset({401, 402, 403, 429})
# A retired or unavailable model is independent of the API key.  Move directly
# to the next model once; retrying it on every key only produces noise.
MODEL_FALLBACK_STATUSES = frozenset({404})
JSON_VALIDATION_RETRIES = 1


class ProviderHTTPError(RuntimeError):
    """Errore provider con status uniforme per il router di fallback."""

    def __init__(self, status_code: int, message: str = "") -> None:
        self.status_code = status_code
        super().__init__(message)


def configured_gemini_keys(environ: dict[str, str] | None = None) -> list[tuple[str, str]]:
    """Slot Gemini del Morning Briefing, in ordine e senza duplicati."""
    environ = os.environ if environ is None else environ
    keys: list[tuple[str, str]] = []
    seen: set[str] = set()
    for env_name in ("GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3"):
        value = environ.get(env_name, "").strip()
        if value and value not in seen:
            keys.append((env_name, value))
            seen.add(value)
    return keys


class GeminiHTTPClient:
    """Piccolo adattatore Gemini compatibile con il router OpenAI-style.

    Evita una nuova dipendenza nel workflow e converte la risposta nel formato
    ``choices[0].message.content`` già consumato dal summarizer.
    """

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, *, model: str, messages: Sequence[dict[str, Any]],
               temperature: float = 0.2, max_tokens: int | None = None,
               response_format: dict[str, Any] | None = None, **_kwargs: Any) -> Any:
        system_parts = []
        contents = []
        for message in messages:
            text = str(message.get("content") or "")
            role = message.get("role")
            if role == "system":
                system_parts.append(text)
            elif text:
                contents.append({
                    "role": "model" if role == "assistant" else "user",
                    "parts": [{"text": text}],
                })

        body: dict[str, Any] = {
            "contents": contents or [{"role": "user", "parts": [{"text": ""}]}],
            "generationConfig": {"temperature": temperature},
        }
        if system_parts:
            body["systemInstruction"] = {"parts": [{"text": "\n\n".join(system_parts)}]}
        if max_tokens:
            body["generationConfig"]["maxOutputTokens"] = max_tokens
        if (response_format or {}).get("type") == "json_object":
            body["generationConfig"]["responseMimeType"] = "application/json"

        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            # Il body Gemini può contenere dettagli del prompt: non propagarlo
            # nei log del workflow; lo status basta al router per decidere.
            raise ProviderHTTPError(error.code, f"Gemini HTTP {error.code}") from error
        except Exception as error:
            raise ProviderHTTPError(0, f"Gemini request failed: {type(error).__name__}") from error

        parts = (payload.get("candidates") or [{}])[0].get("content", {}).get("parts") or []
        content = "".join(str(part.get("text") or "") for part in parts if isinstance(part, dict))
        if not content:
            raise ProviderHTTPError(500, "Gemini returned no content")
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


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
                    logger.info("🤖 LLM purpose=%s key_slot=%s model=%s", purpose, key_env, model)
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


def complete_with_morning_fallback(
    gemini_clients: Sequence[tuple[str, Any]],
    gemini_model: str,
    groq_clients: Sequence[tuple[str, Any]],
    groq_models: Iterable[str],
    logger: Any,
    **kwargs: Any,
) -> Any:
    """Priorità Morning Briefing: Groq NEWS → Gemini → Groq NEWS_2.

    La NEWS primaria conserva il percorso consolidato. Dopo un suo fallimento
    reale si prova Gemini; NEWS_2 è la terza ed ultima battuta. Nessuna chiave
    chatbot/social viene mai coinvolta.
    """
    purpose = kwargs.get("purpose", "unspecified")
    primary_groq = groq_clients[:1]
    secondary_groq = groq_clients[1:]

    if primary_groq:
        try:
            return complete_with_fallback(primary_groq, groq_models, logger, **kwargs)
        except Exception as error:
            logger.warning(
                "⚠️ Groq NEWS purpose=%s non disponibile, passo a Gemini: %s",
                purpose,
                error,
            )

    if gemini_clients:
        try:
            return complete_with_fallback(gemini_clients, (gemini_model,), logger, **kwargs)
        except Exception as error:
            logger.warning(
                "⚠️ Gemini purpose=%s non disponibile, passo a Groq NEWS_2: %s",
                purpose,
                error,
            )

    if secondary_groq:
        return complete_with_fallback(secondary_groq, groq_models, logger, **kwargs)

    raise RuntimeError("Nessuna chiave Gemini o Groq NEWS configurata per il Morning Briefing")
