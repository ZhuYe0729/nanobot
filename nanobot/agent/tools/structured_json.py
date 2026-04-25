"""Sparse JSON reader tool for schema-first, target-driven object access."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from nanobot.agent.tools.filesystem import _FsTool


_MAX_FILE_BYTES = 64 * 1024 * 1024
_MAX_NODES = 200_000
_MAX_DEPTH = 64
_DEFAULT_SAMPLE_LIMIT = 20
_MAX_SAMPLE_LIMIT = 100
_MAX_VALUE_CHARS = 2_000
_MAX_EVIDENCE_ITEMS = 100
_MAX_SCHEMA_PATHS = 200


@dataclass(slots=True)
class _JsonData:
    path: Path
    encoding: str
    root: Any
    node_count: int
    max_depth: int
    truncated: bool


def _decode_json(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            pass
    return raw.decode("utf-8", errors="replace"), "utf-8-replace"


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _path_join(parent: str, token: str | int) -> str:
    if isinstance(token, int):
        return f"{parent}[{token}]"
    if parent == "$":
        return f"$.{token}"
    return f"{parent}.{token}"


def _normalize_path(path: str | None) -> str:
    if not path:
        return "$"
    value = str(path).strip()
    if not value:
        return "$"
    if value == "$" or value.startswith("$.") or value.startswith("$["):
        return value
    if value.startswith("."):
        return f"${value}"
    return f"$.{value}"


def _parse_path(path: str | None) -> list[str | int]:
    value = _normalize_path(path)
    if value == "$":
        return []
    if not value.startswith("$"):
        raise ValueError(f"unsupported JSON path: {path}")
    tokens: list[str | int] = []
    i = 1
    while i < len(value):
        ch = value[i]
        if ch == ".":
            i += 1
            start = i
            while i < len(value) and value[i] not in ".[":
                i += 1
            key = value[start:i]
            if not key:
                raise ValueError(f"unsupported JSON path: {path}")
            tokens.append(key)
            continue
        if ch == "[":
            end = value.find("]", i)
            if end == -1:
                raise ValueError(f"unsupported JSON path: {path}")
            raw = value[i + 1 : end].strip()
            if raw == "*":
                tokens.append("*")
            else:
                try:
                    tokens.append(int(raw))
                except ValueError as exc:
                    raise ValueError(f"unsupported JSON path: {path}") from exc
            i = end + 1
            continue
        raise ValueError(f"unsupported JSON path: {path}")
    return tokens


def _resolve_one(root: Any, path: str | None) -> tuple[bool, Any]:
    current = root
    for token in _parse_path(path):
        if isinstance(token, str) and token == "*":
            return False, None
        if isinstance(token, str):
            if not isinstance(current, dict) or token not in current:
                return False, None
            current = current[token]
        else:
            if not isinstance(current, list) or token < 0 or token >= len(current):
                return False, None
            current = current[token]
    return True, current


def _resolve_many(root: Any, path: str | None) -> list[tuple[str, Any]]:
    tokens = _parse_path(path)
    results: list[tuple[str, Any]] = []

    def visit(value: Any, rest: list[str | int], current_path: str) -> None:
        if not rest:
            results.append((current_path, value))
            return
        token = rest[0]
        if token == "*":
            if isinstance(value, list):
                for idx, item in enumerate(value):
                    visit(item, rest[1:], _path_join(current_path, idx))
            return
        if isinstance(token, str):
            if isinstance(value, dict) and token in value:
                visit(value[token], rest[1:], _path_join(current_path, token))
            return
        if isinstance(value, list) and 0 <= token < len(value):
            visit(value[token], rest[1:], _path_join(current_path, token))

    visit(root, tokens, "$")
    return results


def _clip(value: Any, max_chars: int = _MAX_VALUE_CHARS) -> dict[str, Any]:
    if isinstance(value, (str, int, float, bool)) or value is None:
        raw = value
    else:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if isinstance(raw, str) and len(raw) > max_chars:
        return {"value": raw[:max_chars], "truncated": True, "chars": len(raw)}
    if isinstance(value, (dict, list)):
        return {"value": value, "truncated": False}
    return {"value": raw, "truncated": False}


def _is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").strip())
        except ValueError:
            return None
    return None


def _scalar_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return "" if value is None else str(value)


class JsonReadTool(_FsTool):
    """Sparse JSON access: scout schema, focus evidence, aggregate, and verify paths."""

    @property
    def name(self) -> str:
        return "json_read"

    @property
    def description(self) -> str:
        return (
            "Sparse-read a JSON file. Use mode='scout' before reading large JSON, "
            "mode='focus' or 'refine' for path/keyword/array evidence and aggregates, "
            "and mode='verify' for exact path checks. Returns compact JSON with anchors."
        )

    @property
    def read_only(self) -> bool:
        return True

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "JSON file path to inspect", "minLength": 1},
                "mode": {
                    "type": "string",
                    "enum": ["scout", "focus", "refine", "verify"],
                    "description": "scout=schema/profile, focus=targeted evidence, refine=shorter follow-up, verify=exact path check",
                },
                "goal": {"type": ["string", "null"], "description": "The reading goal or question"},
                "paths": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "JSONPath-lite paths such as $.users[0].name or settings.retry",
                },
                "needles": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "Keywords to search in keys and scalar values",
                },
                "array_path": {"type": ["string", "null"], "description": "Path to an array for item filtering or aggregation"},
                "filters": {
                    "type": ["array", "null"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "op": {
                                "type": "string",
                                "enum": ["eq", "ne", "contains", "regex", "gt", "gte", "lt", "lte", "not_empty"],
                            },
                            "value": {},
                        },
                        "required": ["path", "op"],
                    },
                    "description": "Filters applied to items under array_path; filter paths are relative to each item",
                },
                "aggregate": {
                    "type": ["object", "null"],
                    "properties": {
                        "op": {"type": "string", "enum": ["count", "sum", "avg", "min", "max", "distinct", "topk"]},
                        "path": {"type": ["string", "null"]},
                        "by": {"type": ["string", "null"]},
                        "limit": {"type": ["integer", "null"], "minimum": 1, "maximum": 100},
                    },
                    "description": "Optional aggregate over array_path items or a wildcard path",
                },
                "item_start": {"type": ["integer", "null"], "minimum": 0, "description": "0-based array item start"},
                "item_limit": {"type": ["integer", "null"], "minimum": 1, "maximum": 1000, "description": "Maximum array items to consider"},
                "sample_limit": {"type": "integer", "minimum": 1, "maximum": _MAX_SAMPLE_LIMIT, "description": "Maximum evidence items to return"},
                "verify": {
                    "type": ["object", "null"],
                    "properties": {
                        "path": {"type": "string"},
                        "expected": {},
                    },
                    "description": "Exact path verification",
                },
            },
            "required": ["path"],
        }

    async def execute(
        self,
        path: str,
        mode: str = "scout",
        goal: str | None = None,
        paths: list[str] | None = None,
        needles: list[str] | None = None,
        array_path: str | None = None,
        filters: list[dict[str, Any]] | None = None,
        aggregate: dict[str, Any] | None = None,
        item_start: int | None = None,
        item_limit: int | None = None,
        sample_limit: int = _DEFAULT_SAMPLE_LIMIT,
        verify: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> str:
        try:
            data = self._load(path)
            sample_limit = min(max(sample_limit or _DEFAULT_SAMPLE_LIMIT, 1), _MAX_SAMPLE_LIMIT)
            mode = mode if mode in {"scout", "focus", "refine", "verify"} else "scout"
            if mode == "verify":
                return self._dump(self._build_verify_pack(data, verify))

            pack = self._base_pack(data, include_schema=mode in {"scout", "focus"})
            pack["mode"] = mode
            pack["hint"] = {
                "goal": goal,
                "paths": paths or [],
                "needles": needles or [],
                "array_path": array_path,
                "filters": filters or [],
            }
            items = self._array_items(data.root, array_path, filters or [], item_start, item_limit)
            if aggregate:
                pack["result"] = self._aggregate(data.root, items, array_path, aggregate)
            else:
                pack["result"] = {"matched_items": len(items), "scanned_nodes": data.node_count}
            if mode == "scout":
                pack["evidence"] = []
            else:
                evidence = []
                evidence.extend(self._path_evidence(data.root, paths or [], sample_limit))
                remaining = max(sample_limit - len(evidence), 0)
                if remaining:
                    evidence.extend(self._needle_evidence(data.root, needles, goal, remaining))
                remaining = max(sample_limit - len(evidence), 0)
                if remaining and array_path:
                    evidence.extend(self._array_evidence(items, array_path, paths or [], remaining))
                pack["evidence"] = evidence[:sample_limit]
            pack["unresolved"] = self._unresolved(data, paths or [], array_path, filters or [], len(items))
            pack["next_actions"] = self._next_actions(path, mode, paths or [], array_path, len(items), sample_limit)
            return self._dump(pack)
        except PermissionError as exc:
            return f"Error: {exc}"
        except Exception as exc:
            return f"Error reading JSON: {exc}"

    def _load(self, path: str) -> _JsonData:
        fp = self._resolve(path)
        if not fp.exists():
            raise FileNotFoundError(f"File not found: {path}")
        if not fp.is_file():
            raise ValueError(f"Not a file: {path}")
        size = fp.stat().st_size
        if size > _MAX_FILE_BYTES:
            raise ValueError(f"JSON file exceeds {(_MAX_FILE_BYTES // (1024 * 1024))}MB sparse reader limit: {path}")
        raw = fp.read_bytes()
        if not raw:
            raise ValueError(f"Empty JSON file: {path}")
        text, encoding = _decode_json(raw)
        root = json.loads(text)
        node_count, max_depth, truncated = self._count_nodes(root)
        return _JsonData(path=fp, encoding=encoding, root=root, node_count=node_count, max_depth=max_depth, truncated=truncated)

    def _count_nodes(self, root: Any) -> tuple[int, int, bool]:
        count = 0
        max_depth = 0
        stack: list[tuple[Any, int]] = [(root, 0)]
        truncated = False
        while stack:
            value, depth = stack.pop()
            count += 1
            max_depth = max(max_depth, depth)
            if count >= _MAX_NODES or depth >= _MAX_DEPTH:
                truncated = bool(stack) or isinstance(value, (dict, list))
                break
            if isinstance(value, dict):
                stack.extend((child, depth + 1) for child in value.values())
            elif isinstance(value, list):
                stack.extend((child, depth + 1) for child in value)
        return count, max_depth, truncated

    def _base_pack(self, data: _JsonData, *, include_schema: bool) -> dict[str, Any]:
        pack: dict[str, Any] = {
            "kind": "JSON EvidencePack",
            "file_card": {
                "path": str(data.path),
                "bytes": data.path.stat().st_size,
                "encoding": data.encoding,
                "root_type": _type_name(data.root),
                "node_count": data.node_count,
                "max_depth": data.max_depth,
                "truncated": data.truncated,
                "sparse_read_recommended": True,
            },
        }
        if include_schema:
            pack["schema_view"] = self._schema_view(data.root)
        return pack

    def _schema_view(self, root: Any) -> dict[str, Any]:
        paths: list[dict[str, Any]] = []
        arrays: list[dict[str, Any]] = []
        stack: list[tuple[str, Any, int]] = [("$", root, 0)]
        while stack and len(paths) < _MAX_SCHEMA_PATHS:
            path, value, depth = stack.pop()
            entry: dict[str, Any] = {"path": path, "type": _type_name(value)}
            if isinstance(value, dict):
                entry["keys"] = list(value.keys())[:20]
                entry["key_count"] = len(value)
                for key, child in reversed(list(value.items())[:50]):
                    stack.append((_path_join(path, key), child, depth + 1))
            elif isinstance(value, list):
                entry["length"] = len(value)
                item_types = Counter(_type_name(item) for item in value[:100])
                entry["item_types_sample"] = dict(item_types)
                arrays.append({"path": path, "length": len(value), "item_types_sample": dict(item_types)})
                for idx, child in reversed(list(enumerate(value[:5]))):
                    stack.append((_path_join(path, idx), child, depth + 1))
            paths.append(entry)
        top_level_keys = list(root.keys()) if isinstance(root, dict) else []
        return {
            "top_level_keys": top_level_keys[:50],
            "top_level_key_count": len(top_level_keys),
            "arrays": arrays[:50],
            "paths": paths,
            "truncated": bool(stack),
        }

    def _array_items(
        self,
        root: Any,
        array_path: str | None,
        filters: list[dict[str, Any]],
        item_start: int | None,
        item_limit: int | None,
    ) -> list[tuple[int, Any]]:
        if not array_path:
            return [(0, root)]
        found, value = _resolve_one(root, array_path)
        if not found or not isinstance(value, list):
            return []
        start = max(item_start or 0, 0)
        end = start + item_limit if item_limit else len(value)
        items = [(idx, value[idx]) for idx in range(start, min(end, len(value)))]
        return [(idx, item) for idx, item in items if self._matches_filters(item, filters)]

    def _matches_filters(self, item: Any, filters: list[dict[str, Any]]) -> bool:
        for spec in filters:
            found, actual = _resolve_one(item, spec.get("path"))
            if not self._matches_filter(actual if found else None, str(spec.get("op", "eq")), spec.get("value")):
                return False
        return True

    def _matches_filter(self, actual: Any, op: str, expected: Any) -> bool:
        actual_s = _scalar_text(actual).strip()
        expected_s = _scalar_text(expected).strip()
        if op == "not_empty":
            return not _is_empty(actual)
        if op == "eq":
            return actual == expected or actual_s == expected_s
        if op == "ne":
            return not (actual == expected or actual_s == expected_s)
        if op == "contains":
            return expected_s.lower() in actual_s.lower()
        if op == "regex":
            try:
                return re.search(expected_s, actual_s) is not None
            except re.error:
                return False
        if op in {"gt", "gte", "lt", "lte"}:
            actual_n = _to_float(actual)
            expected_n = _to_float(expected)
            if actual_n is None or expected_n is None:
                return False
            return {
                "gt": actual_n > expected_n,
                "gte": actual_n >= expected_n,
                "lt": actual_n < expected_n,
                "lte": actual_n <= expected_n,
            }[op]
        return False

    def _path_evidence(self, root: Any, paths: list[str], sample_limit: int) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        for path in paths:
            matches = _resolve_many(root, path)
            if not matches:
                continue
            for resolved_path, value in matches[: max(sample_limit - len(evidence), 0)]:
                clipped = _clip(value)
                evidence.append({
                    "path": resolved_path,
                    "type": _type_name(value),
                    "value": clipped["value"],
                    "truncated": clipped["truncated"],
                })
                if len(evidence) >= sample_limit:
                    return evidence
        return evidence

    def _needle_evidence(self, root: Any, needles: list[str] | None, goal: str | None, sample_limit: int) -> list[dict[str, Any]]:
        normalized = self._normalize_needles(needles, goal)
        if not normalized:
            return []
        evidence: list[dict[str, Any]] = []
        stack: list[tuple[str, Any]] = [("$", root)]
        scanned = 0
        while stack and len(evidence) < sample_limit and scanned < _MAX_NODES:
            path, value = stack.pop()
            scanned += 1
            leaf_text = _scalar_text(value).lower() if not isinstance(value, (dict, list)) else ""
            key_text = path.rsplit(".", 1)[-1].lower()
            if any(needle in key_text or needle in leaf_text for needle in normalized):
                clipped = _clip(value)
                evidence.append({"path": path, "type": _type_name(value), "value": clipped["value"], "truncated": clipped["truncated"]})
            if isinstance(value, dict):
                stack.extend((_path_join(path, key), child) for key, child in reversed(list(value.items())))
            elif isinstance(value, list):
                stack.extend((_path_join(path, idx), child) for idx, child in reversed(list(enumerate(value))))
        return evidence

    def _array_evidence(
        self,
        items: list[tuple[int, Any]],
        array_path: str,
        paths: list[str],
        sample_limit: int,
    ) -> list[dict[str, Any]]:
        evidence: list[dict[str, Any]] = []
        base = _normalize_path(array_path)
        projections = [p for p in paths if p]
        for idx, item in items[:sample_limit]:
            if projections:
                projected: dict[str, Any] = {}
                for rel_path in projections[:20]:
                    found, value = _resolve_one(item, rel_path)
                    if found:
                        projected[rel_path] = value
                value = projected
            else:
                value = item
            clipped = _clip(value)
            evidence.append({
                "path": f"{base}[{idx}]",
                "array_index": idx,
                "type": _type_name(item),
                "value": clipped["value"],
                "truncated": clipped["truncated"],
            })
        return evidence

    def _aggregate(self, root: Any, items: list[tuple[int, Any]], array_path: str | None, spec: dict[str, Any]) -> dict[str, Any]:
        op = str(spec.get("op", "count"))
        limit = min(max(int(spec.get("limit") or 20), 1), 100)
        value_path = spec.get("path")
        by_path = spec.get("by")
        if array_path:
            values = [(_resolve_one(item, value_path)[1] if value_path else item) for _, item in items]
            if op == "count":
                if by_path:
                    counts = Counter(_scalar_text(_resolve_one(item, by_path)[1]) for _, item in items if _resolve_one(item, by_path)[0])
                    return {"op": op, "by": by_path, "groups": dict(counts.most_common(limit)), "matched_items": len(items)}
                return {"op": op, "value": len(items)}
        else:
            matches = _resolve_many(root, value_path or "$")
            values = [value for _, value in matches]
            if op == "count":
                return {"op": op, "value": len(values)}

        if op == "distinct":
            rendered = sorted({_scalar_text(value) for value in values if not _is_empty(value)})
            return {"op": op, "path": value_path, "count": len(rendered), "values": rendered[:limit], "truncated": len(rendered) > limit}
        if op == "topk":
            counts = Counter(_scalar_text(value) for value in values if not _is_empty(value))
            return {"op": op, "path": value_path, "values": dict(counts.most_common(limit))}
        nums = [num for value in values if (num := _to_float(value)) is not None]
        if op == "sum":
            result = round(sum(nums), 10) if nums else None
        elif op == "avg":
            result = round(sum(nums) / len(nums), 10) if nums else None
        elif op == "min":
            result = min(nums) if nums else None
        elif op == "max":
            result = max(nums) if nums else None
        else:
            result = None
        if by_path and array_path:
            groups: dict[str, list[float]] = defaultdict(list)
            for _, item in items:
                found_group, group = _resolve_one(item, by_path)
                found_value, value = _resolve_one(item, value_path)
                num = _to_float(value) if found_value else None
                if found_group and num is not None:
                    groups[_scalar_text(group)].append(num)
            return {"op": op, "path": value_path, "by": by_path, "groups": {key: self._numeric_value(op, vals) for key, vals in groups.items()}}
        return {"op": op, "path": value_path, "value": result, "n": len(nums)}

    def _numeric_value(self, op: str, values: list[float]) -> dict[str, Any]:
        if not values:
            return {"value": None, "n": 0}
        if op == "sum":
            result = round(sum(values), 10)
        elif op == "avg":
            result = round(sum(values) / len(values), 10)
        elif op == "min":
            result = min(values)
        elif op == "max":
            result = max(values)
        else:
            result = None
        return {"value": result, "n": len(values)}

    def _build_verify_pack(self, data: _JsonData, verify: dict[str, Any] | None) -> dict[str, Any]:
        pack = self._base_pack(data, include_schema=False)
        pack["mode"] = "verify"
        if not verify:
            pack["result"] = {"verified": False, "error": "verify object is required for mode='verify'"}
            return pack
        path = verify.get("path")
        found, actual = _resolve_one(data.root, path)
        expected = verify.get("expected")
        matched = found and (expected is None or actual == expected or _scalar_text(actual) == _scalar_text(expected))
        clipped = _clip(actual) if found else {"value": None, "truncated": False}
        pack["result"] = {
            "verified": matched,
            "path": _normalize_path(path),
            "found": found,
            "actual": clipped["value"],
            "actual_type": _type_name(actual) if found else "missing",
            "expected": expected,
            "truncated": clipped["truncated"],
        }
        pack["evidence"] = [{"path": _normalize_path(path), "type": pack["result"]["actual_type"], "value": clipped["value"], "truncated": clipped["truncated"]}] if found else []
        return pack

    def _unresolved(
        self,
        data: _JsonData,
        paths: list[str],
        array_path: str | None,
        filters: list[dict[str, Any]],
        matched_items: int,
    ) -> list[str]:
        unresolved: list[str] = []
        if not array_path:
            for path in paths:
                if not _resolve_many(data.root, path):
                    unresolved.append(f"path not found: {_normalize_path(path)}")
        if array_path:
            found, value = _resolve_one(data.root, array_path)
            if not found:
                unresolved.append(f"array_path not found: {_normalize_path(array_path)}")
            elif not isinstance(value, list):
                unresolved.append(f"array_path is not an array: {_normalize_path(array_path)}")
        if filters and not array_path:
            unresolved.append("filters require array_path")
        if matched_items > _DEFAULT_SAMPLE_LIMIT:
            unresolved.append("matched items exceed returned evidence; refine filters or increase sample_limit")
        if data.truncated:
            unresolved.append(f"file scan stopped after {_MAX_NODES} JSON nodes or depth {_MAX_DEPTH}")
        return unresolved

    def _next_actions(
        self,
        path: str,
        mode: str,
        paths: list[str],
        array_path: str | None,
        matched_items: int,
        sample_limit: int,
    ) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        if mode == "scout":
            actions.append({"tool": "json_read", "path": path, "mode": "focus", "paths": paths[:8], "array_path": array_path})
        if matched_items > sample_limit:
            actions.append({"tool": "json_read", "path": path, "mode": "refine", "array_path": array_path, "filters": "add narrower filters"})
        verify_path = paths[0] if paths else "$"
        actions.append({"tool": "json_read", "path": path, "mode": "verify", "verify": {"path": verify_path}})
        return actions

    def _normalize_needles(self, needles: list[str] | None, goal: str | None) -> list[str]:
        values = [n.strip().lower() for n in (needles or []) if isinstance(n, str) and n.strip()]
        if goal:
            values.extend(token for token in re.findall(r"[A-Za-z0-9_.%-]+", goal.lower()) if len(token) >= 3)
        return values

    @staticmethod
    def _dump(pack: dict[str, Any]) -> str:
        return json.dumps(pack, ensure_ascii=False, indent=2)
