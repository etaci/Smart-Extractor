"""
Learned extraction profile persistence for rule-first dynamic extraction.
"""

from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse


_GLOBAL_PROFILE_LOCK = threading.Lock()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_fields(fields: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for item in fields or []:
        value = str(item or "").strip()
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def _path_prefix(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    segments = [segment for segment in parsed.path.split("/") if segment]
    if not segments:
        return "/"
    return "/" + "/".join(segments[:2])


@dataclass
class LearnedProfile:
    profile_id: str
    domain: str
    path_prefix: str
    page_type: str
    selected_fields: list[str] = field(default_factory=list)
    field_labels: dict[str, str] = field(default_factory=dict)
    sample_url: str = ""
    llm_success_count: int = 0
    rule_success_count: int = 0
    rule_failure_count: int = 0
    last_strategy: str = ""
    last_completeness: float = 0.0
    is_active: bool = True
    disabled_at: str = ""
    disabled_reason: str = ""
    last_matched_url: str = ""
    manual_annotation_count: int = 0
    auto_repair_count: int = 0
    last_annotation_at: str = ""
    last_repair_at: str = ""
    created_at: str = field(default_factory=_now_text)
    updated_at: str = field(default_factory=_now_text)
    last_used_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LearnedProfile":
        return cls(
            profile_id=str(payload.get("profile_id") or ""),
            domain=str(payload.get("domain") or ""),
            path_prefix=str(payload.get("path_prefix") or "/"),
            page_type=str(payload.get("page_type") or "unknown"),
            selected_fields=_normalize_fields(payload.get("selected_fields", [])),
            field_labels={
                str(key).strip(): str(value).strip()
                for key, value in dict(payload.get("field_labels", {})).items()
                if str(key).strip()
            },
            sample_url=str(payload.get("sample_url") or ""),
            llm_success_count=int(payload.get("llm_success_count") or 0),
            rule_success_count=int(payload.get("rule_success_count") or 0),
            rule_failure_count=int(payload.get("rule_failure_count") or 0),
            last_strategy=str(payload.get("last_strategy") or ""),
            last_completeness=float(payload.get("last_completeness") or 0.0),
            is_active=bool(payload.get("is_active", True)),
            disabled_at=str(payload.get("disabled_at") or ""),
            disabled_reason=str(payload.get("disabled_reason") or ""),
            last_matched_url=str(payload.get("last_matched_url") or ""),
            manual_annotation_count=int(payload.get("manual_annotation_count") or 0),
            auto_repair_count=int(payload.get("auto_repair_count") or 0),
            last_annotation_at=str(payload.get("last_annotation_at") or ""),
            last_repair_at=str(payload.get("last_repair_at") or ""),
            created_at=str(payload.get("created_at") or _now_text()),
            updated_at=str(payload.get("updated_at") or _now_text()),
            last_used_at=str(payload.get("last_used_at") or ""),
        )


class LearnedProfileStore:
    """JSON-backed learned profile store."""

    def __init__(self, file_path: str | Path):
        self._file_path = Path(file_path)
        self._file_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = _GLOBAL_PROFILE_LOCK

    def list_profiles(self) -> list[LearnedProfile]:
        return [LearnedProfile.from_dict(item) for item in self._load_items()]

    def get_profile(self, profile_id: str) -> Optional[LearnedProfile]:
        normalized_profile_id = str(profile_id or "").strip()
        if not normalized_profile_id:
            return None

        for item in self.list_profiles():
            if item.profile_id == normalized_profile_id:
                return item
        return None

    def find_best_match(
        self,
        source_url: str,
        selected_fields: list[str] | None = None,
        *,
        include_inactive: bool = False,
    ) -> Optional[LearnedProfile]:
        return self._find_best_match_in_profiles(
            self.list_profiles(),
            source_url,
            selected_fields,
            include_inactive=include_inactive,
        )

    def upsert_from_result(
        self,
        source_url: str,
        *,
        page_type: str,
        selected_fields: list[str],
        field_labels: dict[str, str],
        strategy: str,
        completeness: float,
    ) -> LearnedProfile:
        normalized_fields = _normalize_fields(selected_fields)
        parsed = urlparse(str(source_url or "").strip())
        domain = (parsed.hostname or "").lower()
        current_prefix = _path_prefix(source_url)
        now = _now_text()

        with self._lock:
            profiles = self.list_profiles()
            matched = self._find_best_match_in_profiles(
                profiles,
                source_url,
                normalized_fields,
                include_inactive=True,
            )
            if matched is None:
                matched = LearnedProfile(
                    profile_id=self._next_profile_id(profiles),
                    domain=domain,
                    path_prefix=current_prefix,
                    page_type=str(page_type or "unknown"),
                    selected_fields=normalized_fields,
                    field_labels=field_labels,
                    sample_url=source_url,
                    created_at=now,
                    updated_at=now,
                )
                profiles.append(matched)
            else:
                for index, item in enumerate(profiles):
                    if item.profile_id == matched.profile_id:
                        matched = profiles[index]
                        break

            matched.page_type = str(page_type or matched.page_type or "unknown")
            matched.selected_fields = normalized_fields or matched.selected_fields
            matched.path_prefix = current_prefix or matched.path_prefix
            matched.field_labels = {
                **matched.field_labels,
                **{
                    str(key).strip(): str(value).strip()
                    for key, value in field_labels.items()
                    if str(key).strip()
                },
            }
            matched.sample_url = source_url or matched.sample_url
            matched.last_matched_url = source_url or matched.last_matched_url
            normalized_strategy = str(strategy or "").strip().lower()
            if normalized_strategy.startswith("llm"):
                matched.llm_success_count += 1
            elif normalized_strategy.startswith("rule"):
                matched.rule_success_count += 1
            matched.last_strategy = strategy
            matched.last_completeness = float(completeness or 0.0)
            matched.last_used_at = now
            matched.updated_at = now
            self._save_items([item.to_dict() for item in profiles])
        return matched

    def record_rule_attempt(
        self,
        profile_id: str,
        *,
        success: bool,
        completeness: float,
        source_url: str = "",
    ) -> Optional[LearnedProfile]:
        now = _now_text()
        with self._lock:
            profiles = self.list_profiles()
            target: LearnedProfile | None = None
            for item in profiles:
                if item.profile_id != profile_id:
                    continue
                target = item
                break
            if target is None:
                return None

            if success:
                target.rule_success_count += 1
                target.last_strategy = "rule"
            else:
                target.rule_failure_count += 1
                target.last_strategy = "rule_fallback"
            target.last_completeness = float(completeness or 0.0)
            target.last_matched_url = source_url or target.last_matched_url
            target.last_used_at = now
            target.updated_at = now
            self._save_items([item.to_dict() for item in profiles])
        return target

    def set_profile_active(
        self, profile_id: str, *, is_active: bool, reason: str = ""
    ) -> Optional[LearnedProfile]:
        normalized_profile_id = str(profile_id or "").strip()
        now = _now_text()
        with self._lock:
            profiles = self.list_profiles()
            target: LearnedProfile | None = None
            for item in profiles:
                if item.profile_id != normalized_profile_id:
                    continue
                target = item
                break
            if target is None:
                return None

            target.is_active = bool(is_active)
            if is_active:
                target.disabled_at = ""
                target.disabled_reason = ""
            else:
                target.disabled_at = now
                target.disabled_reason = str(reason or "").strip()
            target.updated_at = now
            self._save_items([item.to_dict() for item in profiles])
        return target

    def reset_profile(self, profile_id: str) -> Optional[LearnedProfile]:
        normalized_profile_id = str(profile_id or "").strip()
        now = _now_text()
        with self._lock:
            profiles = self.list_profiles()
            target: LearnedProfile | None = None
            for item in profiles:
                if item.profile_id != normalized_profile_id:
                    continue
                target = item
                break
            if target is None:
                return None

            target.llm_success_count = 0
            target.rule_success_count = 0
            target.rule_failure_count = 0
            target.last_strategy = ""
            target.last_completeness = 0.0
            target.last_matched_url = ""
            target.last_used_at = ""
            target.updated_at = now
            self._save_items([item.to_dict() for item in profiles])
        return target

    def delete_profile(self, profile_id: str) -> bool:
        normalized_profile_id = str(profile_id or "").strip()
        with self._lock:
            profiles = self.list_profiles()
            remaining = [
                item for item in profiles if item.profile_id != normalized_profile_id
            ]
            if len(remaining) == len(profiles):
                return False
            self._save_items([item.to_dict() for item in remaining])
        return True

    def apply_manual_feedback(
        self,
        profile_id: str,
        *,
        selected_fields: list[str] | None = None,
        field_labels: dict[str, str] | None = None,
        sample_url: str = "",
        repaired: bool = False,
        reactivate: bool = True,
    ) -> Optional[LearnedProfile]:
        normalized_profile_id = str(profile_id or "").strip()
        if not normalized_profile_id:
            return None
        now = _now_text()
        with self._lock:
            profiles = self.list_profiles()
            target: LearnedProfile | None = None
            for item in profiles:
                if item.profile_id == normalized_profile_id:
                    target = item
                    break
            if target is None:
                return None

            normalized_fields = _normalize_fields(selected_fields)
            if normalized_fields:
                target.selected_fields = normalized_fields
            if isinstance(field_labels, dict):
                target.field_labels = {
                    **target.field_labels,
                    **{
                        str(key).strip(): str(value).strip()
                        for key, value in field_labels.items()
                        if str(key).strip()
                    },
                }
            if str(sample_url or "").strip():
                target.sample_url = str(sample_url).strip()
                target.last_matched_url = str(sample_url).strip()
                target.path_prefix = _path_prefix(sample_url)
            target.manual_annotation_count += 1
            target.last_annotation_at = now
            if repaired:
                target.auto_repair_count += 1
                target.last_repair_at = now
                target.rule_failure_count = 0
                target.last_completeness = max(float(target.last_completeness or 0.0), 0.8)
            if reactivate:
                target.is_active = True
                target.disabled_at = ""
                target.disabled_reason = ""
            target.updated_at = now
            self._save_items([item.to_dict() for item in profiles])
        return target

    def stats(self) -> dict[str, int]:
        profiles = self.list_profiles()
        return {
            "total_profiles": len(profiles),
            "active_profiles": sum(1 for item in profiles if item.is_active),
            "disabled_profiles": sum(1 for item in profiles if not item.is_active),
            "rule_success_profiles": sum(
                1 for item in profiles if item.rule_success_count > 0
            ),
            "llm_learned_profiles": sum(
                1 for item in profiles if item.llm_success_count > 0
            ),
        }

    @staticmethod
    def _next_profile_id(profiles: list[LearnedProfile]) -> str:
        max_index = 0
        for item in profiles:
            suffix = str(item.profile_id or "").split("-")[-1]
            if suffix.isdigit():
                max_index = max(max_index, int(suffix))
        return f"lp-{max_index + 1:06d}"

    @staticmethod
    def _find_best_match_in_profiles(
        profiles: list[LearnedProfile],
        source_url: str,
        selected_fields: list[str] | None = None,
        *,
        include_inactive: bool = False,
    ) -> Optional[LearnedProfile]:
        parsed = urlparse(str(source_url or "").strip())
        domain = (parsed.hostname or "").lower()
        if not domain:
            return None

        expected_fields = set(_normalize_fields(selected_fields))
        current_prefix = _path_prefix(source_url)
        best_item: LearnedProfile | None = None
        best_score = -1.0
        for item in profiles:
            if item.domain != domain:
                continue
            if not include_inactive and not item.is_active:
                continue

            profile_fields = set(item.selected_fields)
            field_overlap = 1.0
            if expected_fields:
                overlap = len(expected_fields & profile_fields)
                field_overlap = overlap / max(len(expected_fields), 1)
                if field_overlap < 0.5:
                    continue

            path_score = 1.0 if item.path_prefix == current_prefix else 0.4
            total_score = field_overlap * 2.0 + path_score
            if total_score > best_score:
                best_score = total_score
                best_item = item
        return best_item

    def _load_items(self) -> list[dict[str, Any]]:
        if not self._file_path.exists():
            return []
        try:
            payload = json.loads(self._file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        return payload if isinstance(payload, list) else []

    def _save_items(self, items: list[dict[str, Any]]) -> None:
        self._file_path.write_text(
            json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8"
        )
