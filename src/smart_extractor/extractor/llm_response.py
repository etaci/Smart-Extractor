"""LLM 响应解析与通用格式化工具。"""

from __future__ import annotations

import json
from typing import Any


def _safe_json_loads(raw_text: str) -> dict[str, Any]:
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("LLM 返回内容为空")

    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        candidate = text[start : end + 1]
        result = json.loads(candidate)
        if isinstance(result, dict):
            return result

    raise ValueError("LLM 返回结果不是有效 JSON 对象")


def _extract_chat_message_content(response: Any) -> str:
    if response is None:
        return ""

    if isinstance(response, str):
        return _extract_content_from_text_response(response)

    choices = getattr(response, "choices", None)
    if choices is None and isinstance(response, dict):
        choices = response.get("choices")

    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        message = getattr(first_choice, "message", None)
        if message is None and isinstance(first_choice, dict):
            message = first_choice.get("message")

        content = getattr(message, "content", None)
        if content is None and isinstance(message, dict):
            content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content

        delta = getattr(first_choice, "delta", None)
        if delta is None and isinstance(first_choice, dict):
            delta = first_choice.get("delta")

        delta_content = getattr(delta, "content", None)
        if delta_content is None and isinstance(delta, dict):
            delta_content = delta.get("content")
        if isinstance(delta_content, str) and delta_content.strip():
            return delta_content

        text = getattr(first_choice, "text", None)
        if text is None and isinstance(first_choice, dict):
            text = first_choice.get("text")
        if isinstance(text, str) and text.strip():
            return text

    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    if isinstance(response, dict):
        dict_output_text = response.get("output_text")
        if isinstance(dict_output_text, str) and dict_output_text.strip():
            return dict_output_text

    return ""


def _extract_content_from_text_response(raw_response: str) -> str:
    text = (raw_response or "").strip()
    if not text:
        return ""

    direct = _extract_content_from_json_payload(text)
    if direct:
        return direct

    parts: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or not stripped.startswith("data:"):
            continue
        payload = stripped[5:].strip()
        if not payload or payload == "[DONE]":
            continue
        chunk_content = _extract_content_from_json_payload(payload)
        if chunk_content:
            parts.append(chunk_content)

    if parts:
        return "".join(parts).strip()

    return text


def _extract_content_from_json_payload(payload_text: str) -> str:
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return ""

    if isinstance(payload, dict):
        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            first_choice = choices[0]
            if isinstance(first_choice, dict):
                message = first_choice.get("message")
                if isinstance(message, dict):
                    content = message.get("content")
                    if isinstance(content, str) and content.strip():
                        return content

                delta = first_choice.get("delta")
                if isinstance(delta, dict):
                    delta_content = delta.get("content")
                    if isinstance(delta_content, str) and delta_content.strip():
                        return delta_content

                text = first_choice.get("text")
                if isinstance(text, str) and text.strip():
                    return text

        output_text = payload.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            return output_text

    if isinstance(payload, str):
        return payload.strip()

    return ""


def _normalize_field_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    for item in values:
        value = str(item or "").strip()
        if value and value not in result:
            result.append(value)
    return result


def _normalize_url_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    for item in values:
        value = str(item or "").strip()
        if value.startswith(("http://", "https://")) and value not in result:
            result.append(value)
    return result


def _format_dynamic_text(field_labels: dict[str, str], data: dict[str, Any]) -> str:
    lines: list[str] = []
    for key, value in data.items():
        label = field_labels.get(key) or key
        if isinstance(value, list):
            rendered = "；".join(str(item) for item in value if str(item).strip())
        elif isinstance(value, dict):
            rendered = json.dumps(value, ensure_ascii=False)
        else:
            rendered = str(value or "").strip()

        if not rendered:
            continue
        lines.append(f"“{label}”：“{rendered}”")
    return "\n".join(lines)
