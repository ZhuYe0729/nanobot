from __future__ import annotations

import json
import asyncio
from unittest.mock import MagicMock

import pytest

from nanobot.agent.loop import AgentLoop
from nanobot.agent.tools.registry import ToolRegistry
from nanobot.agent.tools.structured_csv import CsvReadTool
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import CsvToolConfig


def _pack(result: str) -> dict:
    return json.loads(result)


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture()
def sales_csv(tmp_path):
    path = tmp_path / "sales.csv"
    path.write_text(
        "\n".join(
            [
                "id,region,amount,status,notes",
                "A-001,East,10.5,active,first order",
                "A-002,West,20,inactive,\"contains, comma\"",
                "A-003,East,30,active,priority customer",
                "A-004,North,,active,missing amount",
            ]
        ),
        encoding="utf-8",
    )
    return path


class TestCsvReadTool:
    @pytest.fixture()
    def tool(self, tmp_path):
        return CsvReadTool(workspace=tmp_path)

    def test_scout_returns_schema_without_full_table(self, tool, sales_csv):
        result = _run(tool.execute(path=str(sales_csv), mode="scout"))
        pack = _pack(result)

        assert pack["kind"] == "CSV EvidencePack"
        assert pack["file_card"]["data_rows"] == 4
        assert pack["file_card"]["columns"] == 5
        assert pack["schema_view"]["headers"] == ["id", "region", "amount", "status", "notes"]
        assert pack["evidence"] == []

    def test_focus_filters_projects_and_returns_anchors(self, tool, sales_csv):
        result = _run(tool.execute(
            path=str(sales_csv),
            mode="focus",
            columns=["id", "amount"],
            filters=[{"column": "region", "op": "eq", "value": "East"}],
            sample_limit=5,
        ))
        pack = _pack(result)

        assert pack["result"]["matched_rows"] == 2
        assert pack["evidence"][0]["line"] == 2
        assert pack["evidence"][0]["data_row"] == 1
        assert pack["evidence"][0]["columns"] == {"id": "A-001", "amount": "10.5"}
        assert pack["evidence"][1]["columns"] == {"id": "A-003", "amount": "30"}

    def test_numeric_filters_and_sum_aggregate(self, tool, sales_csv):
        result = _run(tool.execute(
            path=str(sales_csv),
            mode="focus",
            columns=["id", "amount"],
            filters=[{"column": "amount", "op": "gte", "value": 20}],
            aggregate={"op": "sum", "column": "amount"},
        ))
        pack = _pack(result)

        assert pack["result"] == {"op": "sum", "column": "amount", "value": 50.0, "n": 2}

    def test_distinct_topk_and_grouped_count(self, tool, sales_csv):
        distinct = _pack(
            _run(tool.execute(
                path=str(sales_csv),
                mode="focus",
                aggregate={"op": "distinct", "column": "region"},
            ))
        )
        topk = _pack(
            _run(tool.execute(
                path=str(sales_csv),
                mode="focus",
                aggregate={"op": "topk", "column": "region"},
            ))
        )
        grouped = _pack(
            _run(tool.execute(
                path=str(sales_csv),
                mode="focus",
                aggregate={"op": "count", "by": "status"},
            ))
        )

        assert distinct["result"]["count"] == 3
        assert distinct["result"]["values"] == ["East", "North", "West"]
        assert topk["result"]["values"]["East"] == 2
        assert grouped["result"]["groups"] == {"active": 3, "inactive": 1}

    def test_verify_exact_cell(self, tool, sales_csv):
        result = _run(tool.execute(
            path=str(sales_csv),
            mode="verify",
            verify={"row": 2, "column": "notes", "expected": "contains, comma"},
        ))
        pack = _pack(result)

        assert pack["result"]["verified"] is True
        assert pack["result"]["line"] == 3
        assert pack["result"]["actual"] == "contains, comma"

    def test_utf8_bom_and_duplicate_headers_are_handled(self, tool, tmp_path):
        path = tmp_path / "dup.csv"
        path.write_bytes("\ufeffname,name,value\nA,B,1\n".encode("utf-8"))

        pack = _pack(_run(tool.execute(path=str(path), mode="scout")))

        assert pack["schema_view"]["headers"] == ["name", "name.2", "value"]
        assert pack["schema_view"]["duplicate_headers_renamed"] == ["name"]

    def test_missing_filter_column_is_reported(self, tool, sales_csv):
        pack = _pack(
            _run(tool.execute(
                path=str(sales_csv),
                mode="focus",
                filters=[{"column": "missing", "op": "eq", "value": "x"}],
            ))
        )

        assert "filter column not found: missing" in pack["unresolved"]

    def test_tool_registry_executes_csv_read(self, tool, sales_csv):
        registry = ToolRegistry()
        registry.register(tool)

        result = _run(registry.execute("csv_read", {"path": str(sales_csv), "mode": "scout"}))

        assert _pack(result)["file_card"]["data_rows"] == 4
        assert tool.read_only is True
        assert tool.concurrency_safe is True


class TestCsvReadToolRegistration:
    def test_csv_read_registered_by_default(self, tmp_path):
        provider = MagicMock()
        provider.get_default_model.return_value = "test-model"

        loop = AgentLoop(bus=MessageBus(), provider=provider, workspace=tmp_path, model="test-model")

        assert loop.tools.has("csv_read")

    def test_csv_read_can_be_disabled_by_config(self, tmp_path):
        provider = MagicMock()
        provider.get_default_model.return_value = "test-model"

        loop = AgentLoop(
            bus=MessageBus(),
            provider=provider,
            workspace=tmp_path,
            model="test-model",
            csv_config=CsvToolConfig(enabled=False),
        )

        assert not loop.tools.has("csv_read")
