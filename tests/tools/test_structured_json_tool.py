from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

import pytest

from nanobot.agent.loop import AgentLoop
from nanobot.agent.tools.registry import ToolRegistry
from nanobot.agent.tools.structured_json import JsonReadTool
from nanobot.bus.queue import MessageBus
from nanobot.config.schema import JsonToolConfig


def _pack(result: str) -> dict:
    return json.loads(result)


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture()
def users_json(tmp_path):
    path = tmp_path / "users.json"
    path.write_text(
        json.dumps(
            {
                "settings": {"enabled": True, "retry": 3},
                "users": [
                    {"id": "u1", "name": "Ada", "role": "admin", "score": 10, "status": "active"},
                    {"id": "u2", "name": "Ben", "role": "viewer", "score": 15, "status": "inactive"},
                    {"id": "u3", "name": "Chen", "role": "admin", "score": 20, "status": "active"},
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


class TestJsonReadTool:
    @pytest.fixture()
    def tool(self, tmp_path):
        return JsonReadTool(workspace=tmp_path)

    def test_scout_returns_schema_without_full_document(self, tool, users_json):
        result = _run(tool.execute(path=str(users_json), mode="scout"))
        pack = _pack(result)

        assert pack["kind"] == "JSON EvidencePack"
        assert pack["file_card"]["root_type"] == "object"
        assert pack["schema_view"]["top_level_keys"] == ["settings", "users"]
        assert {"path": "$.users", "length": 3, "item_types_sample": {"object": 3}} in pack["schema_view"]["arrays"]
        assert pack["evidence"] == []

    def test_focus_reads_paths_and_wildcards(self, tool, users_json):
        result = _run(tool.execute(
            path=str(users_json),
            mode="focus",
            paths=["$.settings.retry", "$.users[*].name"],
            sample_limit=5,
        ))
        pack = _pack(result)

        values = {item["path"]: item["value"] for item in pack["evidence"]}
        assert values["$.settings.retry"] == 3
        assert values["$.users[0].name"] == "Ada"
        assert values["$.users[1].name"] == "Ben"
        assert values["$.users[2].name"] == "Chen"

    def test_focus_searches_needles(self, tool, users_json):
        result = _run(tool.execute(path=str(users_json), mode="focus", needles=["Chen"], sample_limit=3))
        pack = _pack(result)

        assert any(item["path"] == "$.users[2].name" and item["value"] == "Chen" for item in pack["evidence"])

    def test_array_filters_and_projection(self, tool, users_json):
        result = _run(tool.execute(
            path=str(users_json),
            mode="focus",
            array_path="$.users",
            filters=[{"path": "role", "op": "eq", "value": "admin"}],
            paths=["id", "score"],
            sample_limit=5,
        ))
        pack = _pack(result)

        assert pack["result"]["matched_items"] == 2
        assert pack["unresolved"] == []
        assert pack["evidence"][0]["array_index"] == 0
        assert pack["evidence"][0]["value"] == {"id": "u1", "score": 10}
        assert pack["evidence"][1]["value"] == {"id": "u3", "score": 20}

    def test_aggregates_over_array_items(self, tool, users_json):
        summed = _pack(
            _run(tool.execute(
                path=str(users_json),
                mode="focus",
                array_path="$.users",
                aggregate={"op": "sum", "path": "score"},
            ))
        )
        grouped = _pack(
            _run(tool.execute(
                path=str(users_json),
                mode="focus",
                array_path="$.users",
                aggregate={"op": "count", "by": "status"},
            ))
        )
        topk = _pack(
            _run(tool.execute(
                path=str(users_json),
                mode="focus",
                array_path="$.users",
                aggregate={"op": "topk", "path": "role"},
            ))
        )

        assert summed["result"] == {"op": "sum", "path": "score", "value": 45.0, "n": 3}
        assert grouped["result"]["groups"] == {"active": 2, "inactive": 1}
        assert topk["result"]["values"] == {"admin": 2, "viewer": 1}

    def test_verify_exact_path(self, tool, users_json):
        result = _run(tool.execute(
            path=str(users_json),
            mode="verify",
            verify={"path": "$.settings.enabled", "expected": True},
        ))
        pack = _pack(result)

        assert pack["result"]["verified"] is True
        assert pack["result"]["actual"] is True
        assert pack["evidence"][0]["path"] == "$.settings.enabled"

    def test_missing_path_is_reported(self, tool, users_json):
        pack = _pack(_run(tool.execute(path=str(users_json), mode="focus", paths=["$.missing.value"])))

        assert "path not found: $.missing.value" in pack["unresolved"]

    def test_tool_registry_executes_json_read(self, tool, users_json):
        registry = ToolRegistry()
        registry.register(tool)

        result = _run(registry.execute("json_read", {"path": str(users_json), "mode": "scout"}))

        assert _pack(result)["file_card"]["root_type"] == "object"
        assert tool.read_only is True
        assert tool.concurrency_safe is True


class TestJsonReadToolRegistration:
    def test_json_read_registered_by_default(self, tmp_path):
        provider = MagicMock()
        provider.get_default_model.return_value = "test-model"

        loop = AgentLoop(bus=MessageBus(), provider=provider, workspace=tmp_path, model="test-model")

        assert loop.tools.has("json_read")

    def test_json_read_can_be_disabled_by_config(self, tmp_path):
        provider = MagicMock()
        provider.get_default_model.return_value = "test-model"

        loop = AgentLoop(
            bus=MessageBus(),
            provider=provider,
            workspace=tmp_path,
            model="test-model",
            json_config=JsonToolConfig(enabled=False),
        )

        assert not loop.tools.has("json_read")
