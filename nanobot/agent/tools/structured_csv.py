"""Sparse CSV reader tool for schema-first, target-driven table access."""

from __future__ import annotations

import csv
import io
import json
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from nanobot.agent.tools.filesystem import _FsTool


_MAX_FILE_BYTES = 64 * 1024 * 1024
_MAX_SCAN_ROWS = 200_000
_DEFAULT_SAMPLE_LIMIT = 20
_MAX_SAMPLE_LIMIT = 100
_TYPE_SAMPLE_ROWS = 200


@dataclass(slots=True)
class _CsvData:
    path: Path
    encoding: str
    dialect: str
    headers: list[str]
    original_headers: list[str]
    duplicate_headers: list[str]
    rows: list[dict[str, str]]
    line_numbers: list[int]
    truncated: bool


def _unique_headers(headers: list[str]) -> tuple[list[str], list[str]]:
    seen: dict[str, int] = {}
    unique: list[str] = []
    duplicates: list[str] = []
    for idx, raw in enumerate(headers):
        name = raw.strip() if raw.strip() else f"column_{idx + 1}"
        seen[name] = seen.get(name, 0) + 1
        if seen[name] == 1:
            unique.append(name)
        else:
            unique_name = f"{name}.{seen[name]}"
            unique.append(unique_name)
            duplicates.append(name)
    return unique, duplicates


def _decode_csv(raw: bytes) -> tuple[str, str]:
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError:
            pass
    try:
        import chardet

        detected = chardet.detect(raw)
        encoding = detected.get("encoding") or "utf-8"
        return raw.decode(encoding), encoding
    except Exception:
        return raw.decode("utf-8", errors="replace"), "utf-8-replace"


def _sniff_dialect(sample: str) -> tuple[Any, str]:
    try:
        dialect = csv.Sniffer().sniff(sample)
        return dialect, f"delimiter={dialect.delimiter!r}"
    except csv.Error:
        return csv.excel, "excel"


def _is_empty(value: str) -> bool:
    return value.strip() == ""


def _to_float(value: str) -> float | None:
    if _is_empty(value):
        return None
    cleaned = value.strip().replace(",", "")
    try:
        if cleaned.lower() in {"nan", "inf", "-inf"}:
            return None
        return float(cleaned)
    except ValueError:
        return None


def _looks_date(value: str) -> bool:
    if _is_empty(value):
        return False
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            datetime.strptime(value.strip(), fmt)
            return True
        except ValueError:
            pass
    return False


def _infer_type(values: list[str]) -> str:
    non_empty = [v for v in values if not _is_empty(v)]
    if not non_empty:
        return "empty"
    numeric = sum(1 for v in non_empty if _to_float(v) is not None)
    boolean = sum(1 for v in non_empty if v.strip().lower() in {"true", "false", "0", "1", "yes", "no"})
    dates = sum(1 for v in non_empty if _looks_date(v))
    threshold = max(1, math.ceil(len(non_empty) * 0.8))
    if numeric >= threshold:
        return "number"
    if dates >= threshold:
        return "date"
    if boolean >= threshold:
        return "boolean"
    return "string"


def _normalize_needles(needles: list[str] | None, goal: str | None) -> list[str]:
    values = [n.strip().lower() for n in (needles or []) if isinstance(n, str) and n.strip()]
    if goal:
        values.extend(token for token in re.findall(r"[A-Za-z0-9_.%-]+", goal.lower()) if len(token) >= 3)
    return values


class CsvReadTool(_FsTool):
    """Sparse CSV access: scout schema, focus evidence, aggregate, and verify cells."""

    @property
    def name(self) -> str:
        return "csv_read"

    @property
    def description(self) -> str:
        return (
            "Sparse-read a CSV file. Use mode='scout' before reading large CSVs, "
            "mode='focus' or 'refine' for filtered/projected evidence and aggregates, "
            "and mode='verify' for exact cell checks. Returns compact JSON with anchors."
        )

    @property
    def read_only(self) -> bool:
        return True

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "CSV file path to inspect", "minLength": 1},
                "mode": {
                    "type": "string",
                    "enum": ["scout", "focus", "refine", "verify"],
                    "description": "scout=schema/profile, focus=targeted evidence, refine=shorter follow-up, verify=exact cell check",
                },
                "goal": {"type": ["string", "null"], "description": "The reading goal or question"},
                "needles": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "Keywords or short phrases to search in headers/cells",
                },
                "columns": {
                    "type": ["array", "null"],
                    "items": {"type": "string"},
                    "description": "Columns to project; omitted/empty lets the tool choose compact relevant columns",
                },
                "filters": {
                    "type": ["array", "null"],
                    "items": {
                        "type": "object",
                        "properties": {
                            "column": {"type": "string"},
                            "op": {
                                "type": "string",
                                "enum": ["eq", "ne", "contains", "regex", "gt", "gte", "lt", "lte", "not_empty"],
                            },
                            "value": {"type": ["string", "null"]},
                        },
                        "required": ["column", "op"],
                    },
                    "description": "Row filters applied before evidence/aggregate",
                },
                "aggregate": {
                    "type": ["object", "null"],
                    "properties": {
                        "op": {
                            "type": "string",
                            "enum": ["count", "sum", "avg", "min", "max", "distinct", "topk"],
                        },
                        "column": {"type": ["string", "null"]},
                        "by": {"type": ["string", "null"]},
                        "limit": {"type": ["integer", "null"], "minimum": 1, "maximum": 100},
                    },
                    "description": "Optional aggregate to compute inside the tool",
                },
                "row_start": {
                    "type": ["integer", "null"],
                    "minimum": 1,
                    "description": "1-based data row start, excluding header",
                },
                "row_limit": {
                    "type": ["integer", "null"],
                    "minimum": 1,
                    "maximum": 1000,
                    "description": "Maximum data rows to consider from row_start",
                },
                "sample_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": _MAX_SAMPLE_LIMIT,
                    "description": "Maximum evidence rows to return",
                },
                "verify": {
                    "type": ["object", "null"],
                    "properties": {
                        "row": {"type": "integer", "minimum": 1},
                        "column": {"type": "string"},
                        "expected": {"type": ["string", "null"]},
                    },
                    "description": "Exact cell verification: row is 1-based data row, excluding header",
                },
            },
            "required": ["path"],
        }

    async def execute(
        self,
        path: str,
        mode: str = "scout",
        goal: str | None = None,
        needles: list[str] | None = None,
        columns: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        aggregate: dict[str, Any] | None = None,
        row_start: int | None = None,
        row_limit: int | None = None,
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

            filtered_indexes = self._filter_indexes(data, filters or [], row_start, row_limit)
            selected_columns = self._select_columns(data, columns or [], needles, goal, filtered_indexes)
            pack = self._base_pack(data, include_schema=mode in {"scout", "focus"})
            pack["mode"] = mode
            pack["hint"] = {
                "goal": goal,
                "needles": needles or [],
                "columns": selected_columns,
                "filters": filters or [],
            }
            if aggregate:
                pack["result"] = self._aggregate(data, filtered_indexes, aggregate)
            else:
                pack["result"] = {
                    "matched_rows": len(filtered_indexes),
                    "scanned_rows": len(data.rows),
                }
            if mode == "scout":
                pack["evidence"] = []
            else:
                pack["evidence"] = self._evidence_rows(data, filtered_indexes, selected_columns, needles, sample_limit)
            pack["unresolved"] = self._unresolved(data, filters or [], selected_columns, len(filtered_indexes))
            pack["next_actions"] = self._next_actions(path, mode, selected_columns, len(filtered_indexes), sample_limit)
            return self._dump(pack)
        except PermissionError as exc:
            return f"Error: {exc}"
        except Exception as exc:
            return f"Error reading CSV: {exc}"

    def _load(self, path: str) -> _CsvData:
        fp = self._resolve(path)
        if not fp.exists():
            raise FileNotFoundError(f"File not found: {path}")
        if not fp.is_file():
            raise ValueError(f"Not a file: {path}")
        size = fp.stat().st_size
        if size > _MAX_FILE_BYTES:
            raise ValueError(f"CSV file exceeds {(_MAX_FILE_BYTES // (1024 * 1024))}MB sparse reader limit: {path}")
        raw = fp.read_bytes()
        if not raw:
            raise ValueError(f"Empty CSV file: {path}")
        text, encoding = _decode_csv(raw)
        dialect, dialect_name = _sniff_dialect(text[:8192])
        reader = csv.reader(io.StringIO(text), dialect)
        try:
            original_headers = next(reader)
        except StopIteration as exc:
            raise ValueError(f"CSV has no header row: {path}") from exc
        headers, duplicate_headers = _unique_headers(original_headers)
        rows: list[dict[str, str]] = []
        line_numbers: list[int] = []
        truncated = False
        for raw_row in reader:
            if len(rows) >= _MAX_SCAN_ROWS:
                truncated = True
                break
            values = list(raw_row[: len(headers)])
            if len(values) < len(headers):
                values.extend([""] * (len(headers) - len(values)))
            rows.append(dict(zip(headers, values)))
            line_numbers.append(reader.line_num)
        return _CsvData(
            path=fp,
            encoding=encoding,
            dialect=dialect_name,
            headers=headers,
            original_headers=original_headers,
            duplicate_headers=duplicate_headers,
            rows=rows,
            line_numbers=line_numbers,
            truncated=truncated,
        )

    def _base_pack(self, data: _CsvData, *, include_schema: bool) -> dict[str, Any]:
        pack: dict[str, Any] = {
            "kind": "CSV EvidencePack",
            "file_card": {
                "path": str(data.path),
                "bytes": data.path.stat().st_size,
                "encoding": data.encoding,
                "dialect": data.dialect,
                "data_rows": len(data.rows),
                "columns": len(data.headers),
                "truncated": data.truncated,
                "sparse_read_recommended": True,
            },
        }
        if include_schema:
            pack["schema_view"] = self._schema_view(data)
        return pack

    def _schema_view(self, data: _CsvData) -> dict[str, Any]:
        sample_rows = data.rows[:_TYPE_SAMPLE_ROWS]
        columns = []
        for idx, header in enumerate(data.headers):
            values = [row.get(header, "") for row in sample_rows]
            all_values = [row.get(header, "") for row in data.rows]
            non_empty = [v for v in all_values if not _is_empty(v)]
            counter = Counter(non_empty[:1000])
            columns.append(
                {
                    "index": idx,
                    "name": header,
                    "original_name": data.original_headers[idx] if idx < len(data.original_headers) else header,
                    "type": _infer_type(values),
                    "empty_ratio": round(1 - (len(non_empty) / len(data.rows)), 4) if data.rows else 0,
                    "distinct_sample": [value for value, _ in counter.most_common(5)],
                }
            )
        return {
            "headers": data.headers,
            "duplicate_headers_renamed": data.duplicate_headers,
            "columns": columns,
        }

    def _resolve_column(self, data: _CsvData, name: str | None) -> str | None:
        if not name:
            return None
        exact = {h: h for h in data.headers}
        lowered = {h.lower(): h for h in data.headers}
        if name in exact:
            return exact[name]
        return lowered.get(str(name).lower())

    def _filter_indexes(
        self,
        data: _CsvData,
        filters: list[dict[str, Any]],
        row_start: int | None,
        row_limit: int | None,
    ) -> list[int]:
        indexes = list(range(len(data.rows)))
        if row_start is not None:
            start = max(row_start - 1, 0)
            end = start + row_limit if row_limit else len(indexes)
            indexes = indexes[start:end]
        elif row_limit is not None:
            indexes = indexes[:row_limit]

        resolved_filters: list[tuple[str, str, Any]] = []
        for item in filters:
            col = self._resolve_column(data, item.get("column"))
            if col:
                resolved_filters.append((col, str(item.get("op", "eq")), item.get("value")))
        for col, op, expected in resolved_filters:
            indexes = [idx for idx in indexes if self._matches_filter(data.rows[idx].get(col, ""), op, expected)]
        return indexes

    def _matches_filter(self, actual: str, op: str, expected: Any) -> bool:
        actual_s = actual.strip()
        expected_s = "" if expected is None else str(expected).strip()
        if op == "not_empty":
            return not _is_empty(actual_s)
        if op == "eq":
            return actual_s == expected_s
        if op == "ne":
            return actual_s != expected_s
        if op == "contains":
            return expected_s.lower() in actual_s.lower()
        if op == "regex":
            try:
                return re.search(expected_s, actual_s) is not None
            except re.error:
                return False
        if op in {"gt", "gte", "lt", "lte"}:
            actual_n = _to_float(actual_s)
            expected_n = _to_float(expected_s)
            if actual_n is None or expected_n is None:
                return False
            return {
                "gt": actual_n > expected_n,
                "gte": actual_n >= expected_n,
                "lt": actual_n < expected_n,
                "lte": actual_n <= expected_n,
            }[op]
        return False

    def _select_columns(
        self,
        data: _CsvData,
        columns: list[str],
        needles: list[str] | None,
        goal: str | None,
        indexes: list[int],
    ) -> list[str]:
        explicit = [col for col in (self._resolve_column(data, c) for c in columns) if col]
        if explicit:
            return explicit
        normalized = _normalize_needles(needles, goal)
        if not normalized:
            return data.headers[: min(len(data.headers), 12)]
        selected: list[str] = []
        for header in data.headers:
            if any(n in header.lower() for n in normalized):
                selected.append(header)
        for idx in indexes[:500]:
            row = data.rows[idx]
            for header, value in row.items():
                if header not in selected and any(n in value.lower() for n in normalized):
                    selected.append(header)
        if not selected:
            selected = data.headers[: min(len(data.headers), 12)]
        for header in data.headers[:4]:
            if header not in selected:
                selected.insert(0, header)
        return selected[: min(len(selected), 16)]

    def _evidence_rows(
        self,
        data: _CsvData,
        indexes: list[int],
        columns: list[str],
        needles: list[str] | None,
        sample_limit: int,
    ) -> list[dict[str, Any]]:
        normalized = _normalize_needles(needles, None)
        ranked: list[tuple[int, int]] = []
        for idx in indexes:
            text = " ".join(data.rows[idx].values()).lower()
            score = sum(1 for needle in normalized if needle in text)
            ranked.append((score, idx))
        if normalized:
            ranked.sort(key=lambda item: (-item[0], item[1]))
        selected = [idx for _, idx in ranked[:sample_limit]]
        return [
            {
                "line": data.line_numbers[idx] if idx < len(data.line_numbers) else idx + 2,
                "data_row": idx + 1,
                "columns": {col: data.rows[idx].get(col, "") for col in columns},
            }
            for idx in selected
        ]

    def _aggregate(self, data: _CsvData, indexes: list[int], spec: dict[str, Any]) -> dict[str, Any]:
        op = str(spec.get("op", "count"))
        column = self._resolve_column(data, spec.get("column"))
        by = self._resolve_column(data, spec.get("by"))
        limit = int(spec.get("limit") or 20)
        limit = min(max(limit, 1), 100)
        if op == "count":
            if by:
                counts = Counter(data.rows[idx].get(by, "") for idx in indexes)
                return {"op": op, "by": by, "groups": dict(counts.most_common(limit)), "matched_rows": len(indexes)}
            return {"op": op, "value": len(indexes)}
        if not column:
            return {"op": op, "error": "aggregate column not found", "matched_rows": len(indexes)}
        if op == "distinct":
            values = sorted({data.rows[idx].get(column, "") for idx in indexes if not _is_empty(data.rows[idx].get(column, ""))})
            return {"op": op, "column": column, "count": len(values), "values": values[:limit], "truncated": len(values) > limit}
        if op == "topk":
            counts = Counter(data.rows[idx].get(column, "") for idx in indexes if not _is_empty(data.rows[idx].get(column, "")))
            return {"op": op, "column": column, "values": dict(counts.most_common(limit))}
        if by:
            groups: dict[str, list[float]] = defaultdict(list)
            for idx in indexes:
                value = _to_float(data.rows[idx].get(column, ""))
                if value is not None:
                    groups[data.rows[idx].get(by, "")].append(value)
            return {"op": op, "column": column, "by": by, "groups": self._numeric_groups(op, groups)}
        values = [_to_float(data.rows[idx].get(column, "")) for idx in indexes]
        nums = [v for v in values if v is not None]
        return {"op": op, "column": column, "value": self._numeric_value(op, nums), "n": len(nums)}

    def _numeric_value(self, op: str, nums: list[float]) -> float | None:
        if not nums:
            return None
        if op == "sum":
            return round(sum(nums), 10)
        if op == "avg":
            return round(sum(nums) / len(nums), 10)
        if op == "min":
            return min(nums)
        if op == "max":
            return max(nums)
        return None

    def _numeric_groups(self, op: str, groups: dict[str, list[float]]) -> dict[str, Any]:
        return {key: {"value": self._numeric_value(op, values), "n": len(values)} for key, values in groups.items()}

    def _build_verify_pack(self, data: _CsvData, verify: dict[str, Any] | None) -> dict[str, Any]:
        pack = self._base_pack(data, include_schema=False)
        pack["mode"] = "verify"
        if not verify:
            pack["result"] = {"verified": False, "error": "verify object is required for mode='verify'"}
            return pack
        row_num = int(verify.get("row") or 0)
        column = self._resolve_column(data, verify.get("column"))
        if row_num < 1 or row_num > len(data.rows) or not column:
            pack["result"] = {"verified": False, "error": "row or column not found", "row": row_num, "column": verify.get("column")}
            return pack
        actual = data.rows[row_num - 1].get(column, "")
        expected = verify.get("expected")
        matched = expected is None or actual == str(expected)
        pack["result"] = {
            "verified": matched,
            "row": row_num,
            "line": data.line_numbers[row_num - 1] if row_num - 1 < len(data.line_numbers) else row_num + 1,
            "column": column,
            "actual": actual,
            "expected": expected,
        }
        pack["evidence"] = [{"line": pack["result"]["line"], "data_row": row_num, "columns": {column: actual}}]
        return pack

    def _unresolved(
        self,
        data: _CsvData,
        filters: list[dict[str, Any]],
        selected_columns: list[str],
        matched_rows: int,
    ) -> list[str]:
        unresolved: list[str] = []
        for item in filters:
            if not self._resolve_column(data, item.get("column")):
                unresolved.append(f"filter column not found: {item.get('column')}")
        if not selected_columns:
            unresolved.append("no columns selected")
        if matched_rows > _DEFAULT_SAMPLE_LIMIT:
            unresolved.append("matched rows exceed returned evidence; refine filters or increase sample_limit")
        if data.truncated:
            unresolved.append(f"file scan stopped after {_MAX_SCAN_ROWS} data rows")
        return unresolved

    def _next_actions(
        self,
        path: str,
        mode: str,
        columns: list[str],
        matched_rows: int,
        sample_limit: int,
    ) -> list[dict[str, Any]]:
        actions = []
        if mode == "scout":
            actions.append({"tool": "csv_read", "path": path, "mode": "focus", "columns": columns[:8], "filters": []})
        if matched_rows > sample_limit:
            actions.append({"tool": "csv_read", "path": path, "mode": "refine", "columns": columns[:8], "filters": "add narrower filters"})
        actions.append({"tool": "csv_read", "path": path, "mode": "verify", "verify": {"row": 1, "column": columns[0] if columns else ""}})
        return actions

    @staticmethod
    def _dump(pack: dict[str, Any]) -> str:
        return json.dumps(pack, ensure_ascii=False, indent=2)
