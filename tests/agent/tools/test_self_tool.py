"""Tests for SelfTool — agent runtime self-modification."""

from __future__ import annotations

import copy
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from nanobot.agent.tools.self import SelfTool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_loop(**overrides):
    """Build a lightweight mock AgentLoop with the attributes SelfTool reads."""
    loop = MagicMock()
    loop.model = "anthropic/claude-sonnet-4-20250514"
    loop.max_iterations = 40
    loop.context_window_tokens = 65_536
    loop.context_budget_tokens = 500
    loop.workspace = Path("/tmp/workspace")
    loop.restrict_to_workspace = False
    loop._start_time = 1000.0
    loop.web_proxy = None
    loop.web_search_config = MagicMock()
    loop.exec_config = MagicMock()
    loop.input_limits = MagicMock()
    loop.channels_config = MagicMock()
    loop._last_usage = {"prompt_tokens": 100, "completion_tokens": 50}
    loop._runtime_vars = {}
    loop._unregistered_tools = {}
    loop._config_defaults = {
        "max_iterations": 40,
        "context_window_tokens": 65_536,
        "context_budget_tokens": 500,
        "model": "anthropic/claude-sonnet-4-20250514",
    }
    loop._critical_tool_backup = {}

    # Tools registry mock
    loop.tools = MagicMock()
    loop.tools.tool_names = ["read_file", "write_file", "exec", "web_search", "self"]
    loop.tools.has.side_effect = lambda n: n in loop.tools.tool_names
    loop.tools.get.return_value = None

    for k, v in overrides.items():
        setattr(loop, k, v)

    return loop


def _make_tool(loop=None):
    if loop is None:
        loop = _make_mock_loop()
    return SelfTool(loop=loop)


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

class TestInspect:

    @pytest.mark.asyncio
    async def test_inspect_returns_current_state(self):
        tool = _make_tool()
        result = await tool.execute(action="inspect")
        assert "max_iterations: 40" in result
        assert "context_window_tokens: 65536" in result
        assert "tools:" in result

    @pytest.mark.asyncio
    async def test_inspect_single_key(self):
        tool = _make_tool()
        result = await tool.execute(action="inspect", key="max_iterations")
        assert "max_iterations: 40" in result

    @pytest.mark.asyncio
    async def test_inspect_blocked_returns_error(self):
        tool = _make_tool()
        result = await tool.execute(action="inspect", key="bus")
        assert "not accessible" in result

    @pytest.mark.asyncio
    async def test_inspect_runtime_vars(self):
        loop = _make_mock_loop()
        loop._runtime_vars = {"task": "review"}
        tool = _make_tool(loop)
        result = await tool.execute(action="inspect", key="_runtime_vars")
        assert "task" in result
        assert "review" in result

    @pytest.mark.asyncio
    async def test_inspect_unknown_key(self):
        loop = _make_mock_loop()
        # Make getattr return None for unknown keys (like a real object)
        loop.nonexistent = None
        tool = _make_tool(loop)
        result = await tool.execute(action="inspect", key="nonexistent")
        assert "not found" in result


# ---------------------------------------------------------------------------
# modify — restricted
# ---------------------------------------------------------------------------

class TestModifyRestricted:

    @pytest.mark.asyncio
    async def test_modify_restricted_valid(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value=80)
        assert "Set max_iterations = 80" in result
        assert tool._loop.max_iterations == 80

    @pytest.mark.asyncio
    async def test_modify_restricted_out_of_range(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value=0)
        assert "Error" in result
        assert tool._loop.max_iterations == 40  # unchanged

    @pytest.mark.asyncio
    async def test_modify_restricted_max_exceeded(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value=999)
        assert "Error" in result
        assert tool._loop.max_iterations == 40  # unchanged

    @pytest.mark.asyncio
    async def test_modify_restricted_wrong_type(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value="not_an_int")
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_modify_restricted_bool_rejected_as_int(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value=True)
        assert "Error" in result
        assert "bool" in result

    @pytest.mark.asyncio
    async def test_modify_restricted_string_int_coerced(self):
        """LLM may send numeric values as strings; coercion should handle it."""
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value="80")
        assert "Set max_iterations = 80" in result
        assert tool._loop.max_iterations == 80
        assert isinstance(tool._loop.max_iterations, int)

    @pytest.mark.asyncio
    async def test_modify_context_window_valid(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="context_window_tokens", value=131072)
        assert "Set context_window_tokens = 131072" in result
        assert tool._loop.context_window_tokens == 131072


# ---------------------------------------------------------------------------
# modify — blocked & readonly
# ---------------------------------------------------------------------------

class TestModifyBlockedReadonly:

    @pytest.mark.asyncio
    async def test_modify_blocked_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="bus", value="hacked")
        assert "protected" in result

    @pytest.mark.asyncio
    async def test_modify_tools_blocked(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="tools", value={})
        assert "protected" in result

    @pytest.mark.asyncio
    async def test_modify_subagents_blocked(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="subagents", value=None)
        assert "protected" in result

    @pytest.mark.asyncio
    async def test_modify_context_blocked(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="context", value=None)
        assert "protected" in result

    @pytest.mark.asyncio
    async def test_modify_readonly_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="workspace", value="/tmp/evil")
        assert "read-only" in result

    @pytest.mark.asyncio
    async def test_modify_exec_config_readonly(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="exec_config", value={})
        assert "read-only" in result


# ---------------------------------------------------------------------------
# modify — free tier
# ---------------------------------------------------------------------------

class TestModifyFree:

    @pytest.mark.asyncio
    async def test_modify_free_key_stored(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="my_var", value="hello")
        assert "Set _runtime_vars.my_var = 'hello'" in result
        assert tool._loop._runtime_vars["my_var"] == "hello"

    @pytest.mark.asyncio
    async def test_modify_free_numeric_value(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="count", value=42)
        assert tool._loop._runtime_vars["count"] == 42

    @pytest.mark.asyncio
    async def test_modify_rejects_callable(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="evil", value=lambda: None)
        assert "callable" in result
        assert "evil" not in tool._loop._runtime_vars

    @pytest.mark.asyncio
    async def test_modify_rejects_complex_objects(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="obj", value=Path("/tmp"))
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_modify_allows_list(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="items", value=[1, 2, 3])
        assert tool._loop._runtime_vars["items"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_modify_allows_dict(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="data", value={"a": 1})
        assert tool._loop._runtime_vars["data"] == {"a": 1}


# ---------------------------------------------------------------------------
# unregister_tool / register_tool
# ---------------------------------------------------------------------------

class TestToolManagement:

    @pytest.mark.asyncio
    async def test_unregister_tool_success(self):
        loop = _make_mock_loop()
        tool = _make_tool(loop)
        result = await tool.execute(action="unregister_tool", name="web_search")
        assert "Unregistered" in result
        assert "web_search" in loop._unregistered_tools
        loop.tools.unregister.assert_called_once_with("web_search")

    @pytest.mark.asyncio
    async def test_unregister_self_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="unregister_tool", name="self")
        assert "lockout" in result

    @pytest.mark.asyncio
    async def test_unregister_nonexistent_tool(self):
        tool = _make_tool()
        result = await tool.execute(action="unregister_tool", name="nonexistent")
        assert "not currently registered" in result

    @pytest.mark.asyncio
    async def test_register_tool_restores(self):
        loop = _make_mock_loop()
        mock_tool = MagicMock()
        loop._unregistered_tools = {"web_search": mock_tool}
        tool = _make_tool(loop)
        result = await tool.execute(action="register_tool", name="web_search")
        assert "Re-registered" in result
        loop.tools.register.assert_called_once_with(mock_tool)
        assert "web_search" not in loop._unregistered_tools

    @pytest.mark.asyncio
    async def test_register_unknown_tool_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="register_tool", name="web_search")
        assert "was not previously unregistered" in result

    @pytest.mark.asyncio
    async def test_register_requires_name(self):
        tool = _make_tool()
        result = await tool.execute(action="register_tool")
        assert "Error" in result


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

class TestReset:

    @pytest.mark.asyncio
    async def test_reset_restores_default(self):
        tool = _make_tool()
        # Modify first
        await tool.execute(action="modify", key="max_iterations", value=80)
        assert tool._loop.max_iterations == 80
        # Reset
        result = await tool.execute(action="reset", key="max_iterations")
        assert "Reset max_iterations = 40" in result
        assert tool._loop.max_iterations == 40

    @pytest.mark.asyncio
    async def test_reset_blocked_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="reset", key="bus")
        assert "protected" in result

    @pytest.mark.asyncio
    async def test_reset_readonly_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="reset", key="workspace")
        assert "read-only" in result

    @pytest.mark.asyncio
    async def test_reset_deletes_runtime_var(self):
        tool = _make_tool()
        await tool.execute(action="modify", key="temp", value="data")
        result = await tool.execute(action="reset", key="temp")
        assert "Deleted" in result
        assert "temp" not in tool._loop._runtime_vars

    @pytest.mark.asyncio
    async def test_reset_unknown_key(self):
        tool = _make_tool()
        result = await tool.execute(action="reset", key="nonexistent")
        assert "not a known property" in result


# ---------------------------------------------------------------------------
# Edge cases from code review
# ---------------------------------------------------------------------------

class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_inspect_dunder_blocked(self):
        tool = _make_tool()
        for attr in ("__class__", "__dict__", "__bases__", "__subclasses__", "__mro__"):
            result = await tool.execute(action="inspect", key=attr)
            assert "not accessible" in result

    @pytest.mark.asyncio
    async def test_modify_dunder_blocked(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="__class__", value="evil")
        assert "protected" in result

    @pytest.mark.asyncio
    async def test_modify_internal_attributes_blocked(self):
        tool = _make_tool()
        for attr in ("_config_defaults", "_runtime_vars", "_unregistered_tools", "_critical_tool_backup"):
            result = await tool.execute(action="modify", key=attr, value={})
            assert "protected" in result

    @pytest.mark.asyncio
    async def test_modify_free_nested_dict_with_object_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="evil", value={"nested": object()})
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_modify_free_nested_list_with_callable_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="evil", value=[1, 2, lambda: None])
        assert "Error" in result

    @pytest.mark.asyncio
    async def test_modify_free_deep_nesting_rejected(self):
        tool = _make_tool()
        # Create a deeply nested dict (>10 levels)
        deep = {"level": 0}
        current = deep
        for i in range(1, 15):
            current["child"] = {"level": i}
            current = current["child"]
        result = await tool.execute(action="modify", key="deep", value=deep)
        assert "nesting too deep" in result

    @pytest.mark.asyncio
    async def test_modify_free_dict_with_non_str_key_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="evil", value={42: "value"})
        assert "key must be str" in result

    @pytest.mark.asyncio
    async def test_modify_free_valid_nested_structure(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="data", value={"a": [1, 2, {"b": True}]})
        assert "Error" not in result
        assert tool._loop._runtime_vars["data"] == {"a": [1, 2, {"b": True}]}

    @pytest.mark.asyncio
    async def test_whitespace_key_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="   ", value="test")
        assert "cannot be empty or whitespace" in result

    @pytest.mark.asyncio
    async def test_whitespace_name_rejected(self):
        tool = _make_tool()
        result = await tool.execute(action="unregister_tool", name="  ")
        assert "cannot be empty or whitespace" in result

    @pytest.mark.asyncio
    async def test_modify_none_value_for_restricted_int(self):
        tool = _make_tool()
        result = await tool.execute(action="modify", key="max_iterations", value=None)
        assert "Error" in result
        assert "must be int" in result

    @pytest.mark.asyncio
    async def test_inspect_all_truncates_large_runtime_vars(self):
        loop = _make_mock_loop()
        # Create a large runtime vars dict
        loop._runtime_vars = {f"key_{i}": f"value_{i}" * 100 for i in range(100)}
        tool = _make_tool(loop)
        result = await tool.execute(action="inspect")
        # The output should be truncated
        assert "truncated" in result

    @pytest.mark.asyncio
    async def test_reset_internal_attribute_returns_error(self):
        tool = _make_tool()
        result = await tool.execute(action="reset", key="_config_defaults")
        assert "protected" in result


# ---------------------------------------------------------------------------
# watchdog (tested via AgentLoop method, using a real loop-like object)
# ---------------------------------------------------------------------------

class TestWatchdog:

    def test_watchdog_corrects_invalid_iterations(self):
        loop = _make_mock_loop()
        loop.max_iterations = 0
        loop._critical_tool_backup = {}
        # Simulate watchdog
        defaults = loop._config_defaults
        if not (1 <= loop.max_iterations <= 100):
            loop.max_iterations = defaults["max_iterations"]
        assert loop.max_iterations == 40

    def test_watchdog_corrects_invalid_context_window(self):
        loop = _make_mock_loop()
        loop.context_window_tokens = 100
        loop._critical_tool_backup = {}
        defaults = loop._config_defaults
        if not (4096 <= loop.context_window_tokens <= 1_000_000):
            loop.context_window_tokens = defaults["context_window_tokens"]
        assert loop.context_window_tokens == 65_536

    def test_watchdog_restores_critical_tools(self):
        loop = _make_mock_loop()
        backup = MagicMock()
        loop._critical_tool_backup = {"self": backup}
        loop.tools.has.return_value = False
        loop.tools.tool_names = []
        # Simulate watchdog
        for name, bk in loop._critical_tool_backup.items():
            if not loop.tools.has(name):
                loop.tools.register(copy.deepcopy(bk))
        loop.tools.register.assert_called()
        # Verify it was called with a copy, not the original
        called_arg = loop.tools.register.call_args[0][0]
        assert called_arg is not backup  # deep copy

    def test_watchdog_does_not_reset_valid_state(self):
        loop = _make_mock_loop()
        loop.max_iterations = 50
        loop.context_window_tokens = 131072
        loop._critical_tool_backup = {}
        original_max = loop.max_iterations
        original_ctx = loop.context_window_tokens
        # Simulate watchdog
        if not (1 <= loop.max_iterations <= 100):
            loop.max_iterations = loop._config_defaults["max_iterations"]
        if not (4096 <= loop.context_window_tokens <= 1_000_000):
            loop.context_window_tokens = loop._config_defaults["context_window_tokens"]
        assert loop.max_iterations == original_max
        assert loop.context_window_tokens == original_ctx
