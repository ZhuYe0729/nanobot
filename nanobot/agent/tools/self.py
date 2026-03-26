"""Self-modification tool: allows the agent to inspect and modify its own runtime state."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from loguru import logger

from nanobot.agent.tools.base import Tool

if TYPE_CHECKING:
    from nanobot.agent.loop import AgentLoop


class SelfTool(Tool):
    """Inspect and modify your own runtime state."""

    # -- Tier 0: BLOCKED (never accessible, not even for reading) --
    BLOCKED = frozenset({
        # Core infrastructure
        "bus", "provider", "sessions", "session_manager",
        "_running", "_mcp_stack", "_mcp_servers",
        "_mcp_connected", "_mcp_connecting",
        "memory_consolidator", "_concurrency_gate",
        "commands", "tools", "subagents", "context",
        # Internal self-tool state (prevent tampering with defaults/backup)
        "_config_defaults", "_runtime_vars", "_unregistered_tools", "_critical_tool_backup",
    })

    # Attributes that must never be accessed (dunder / introspection)
    _DENIED_ATTRS = frozenset({
        "__class__", "__dict__", "__bases__", "__subclasses__", "__mro__",
        "__init__", "__new__", "__reduce__", "__getstate__", "__setstate__",
        "__del__", "__call__", "__getattr__", "__setattr__", "__delattr__",
        "__code__", "__globals__", "func_globals", "func_code",
    })

    # -- Tier 1: READONLY (inspectable, not modifiable) --
    READONLY = frozenset({
        "workspace", "restrict_to_workspace", "_start_time", "web_proxy",
        "web_search_config", "exec_config", "input_limits",
        "channels_config", "_last_usage",
    })

    # -- Tier 2: RESTRICTED (modifiable with validation) --
    RESTRICTED: dict[str, dict[str, Any]] = {
        "max_iterations":        {"type": int, "min": 1,   "max": 100},
        "context_window_tokens": {"type": int, "min": 4096, "max": 1_000_000},
        "context_budget_tokens": {"type": int, "min": 0,    "max": 1_000_000},
        "model":                 {"type": str, "min_len": 1},
    }

    def __init__(self, loop: AgentLoop) -> None:
        self._loop = loop
        self._channel = ""
        self._chat_id = ""

    def __deepcopy__(self, memo: dict[int, Any]) -> SelfTool:
        """Return a new instance sharing the same loop reference.

        The loop holds unpicklable state (thread locks, asyncio objects), so a
        true deep copy is impossible.  For the watchdog backup use-case we only
        need a fresh wrapper that still points at the live loop.
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        result._loop = self._loop          # shared reference, not copied
        result._channel = self._channel
        result._chat_id = self._chat_id
        return result

    def set_context(self, channel: str, chat_id: str) -> None:
        """Set session context for audit logging."""
        self._channel = channel
        self._chat_id = chat_id

    @property
    def name(self) -> str:
        return "self"

    @property
    def description(self) -> str:
        return (
            "Inspect and modify your own runtime state. "
            "Use 'inspect' to view current configuration, "
            "'modify' to change parameters, "
            "'unregister_tool'/'register_tool' to manage tools, "
            "and 'reset' to restore defaults."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["inspect", "modify", "unregister_tool", "register_tool", "reset"],
                    "description": "Action to perform",
                },
                "key": {"type": "string", "description": "Property key (for inspect/modify/reset)"},
                "value": {"description": "New value (for modify)"},
                "name": {"type": "string", "description": "Tool name (for unregister_tool/register_tool)"},
            },
            "required": ["action"],
        }

    def _audit(self, action: str, detail: str) -> None:
        """Log a self-modification event for auditability."""
        session = f"{self._channel}:{self._chat_id}" if self._channel else "unknown"
        logger.info("self.{} | {} | session:{}", action, detail, session)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    async def execute(
        self,
        action: str,
        key: str | None = None,
        value: Any = None,
        name: str | None = None,
        **_kwargs: Any,
    ) -> str:
        if action == "inspect":
            return self._inspect(key)
        if action == "modify":
            return self._modify(key, value)
        if action == "unregister_tool":
            return self._unregister_tool(name)
        if action == "register_tool":
            return self._register_tool(name)
        if action == "reset":
            return self._reset(key)
        return f"Unknown action: {action}"

    # -- inspect --

    def _inspect(self, key: str | None) -> str:
        if key:
            if err := self._validate_key(key):
                return err
            return self._inspect_single(key)
        return self._inspect_all()

    def _inspect_single(self, key: str) -> str:
        # Allow inspecting these special dicts even though they're in BLOCKED
        if key == "_runtime_vars":
            rv = self._loop._runtime_vars
            rv_repr = repr(rv)
            if len(rv_repr) > 2000:
                rv_repr = rv_repr[:2000] + "... (truncated)"
            return f"_runtime_vars: {rv_repr}"
        if key == "_unregistered_tools":
            return f"_unregistered_tools: {list(self._loop._unregistered_tools.keys())}"
        if key in self.BLOCKED or key.startswith("__") or key in self._DENIED_ATTRS:
            return f"Error: '{key}' is not accessible"
        val = getattr(self._loop, key, None)
        if val is None:
            return "'{key}' not found on agent".format(key=key)
        return f"{key}: {val!r}"

    def _inspect_all(self) -> str:
        loop = self._loop
        parts: list[str] = []
        # Restricted properties
        for k in self.RESTRICTED:
            parts.append(f"{k}: {getattr(loop, k, None)!r}")
        # Readonly properties
        for k in self.READONLY:
            val = getattr(loop, k, None)
            if val is not None:
                parts.append(f"{k}: {val!r}")
        # Tools
        parts.append(f"tools: {loop.tools.tool_names}")
        # Runtime vars (limit output size)
        rv = loop._runtime_vars
        if rv:
            rv_repr = repr(rv)
            if len(rv_repr) > 2000:
                rv_repr = rv_repr[:2000] + "... (truncated)"
            parts.append(f"_runtime_vars: {rv_repr}")
        # Unregistered tools stash
        if loop._unregistered_tools:
            parts.append(f"_unregistered_tools: {list(loop._unregistered_tools.keys())}")
        return "\n".join(parts)

    # -- modify --

    @staticmethod
    def _validate_key(key: str | None, label: str = "key") -> str | None:
        """Validate a key/name parameter. Returns error string or None."""
        if not key or not key.strip():
            return f"Error: '{label}' cannot be empty or whitespace"
        return None

    def _modify(self, key: str | None, value: Any) -> str:
        if err := self._validate_key(key):
            return err
        if key in self.BLOCKED or key.startswith("__"):
            self._audit("modify", f"BLOCKED {key}")
            return f"Error: '{key}' is protected and cannot be modified"
        if key in self.BLOCKED:
            self._audit("modify", f"BLOCKED {key}")
            return f"Error: '{key}' is protected and cannot be modified"
        if key in self.READONLY:
            self._audit("modify", f"READONLY {key}")
            return f"Error: '{key}' is read-only and cannot be modified"
        if key in self.RESTRICTED:
            return self._modify_restricted(key, value)
        # Free tier: store in _runtime_vars
        return self._modify_free(key, value)

    def _modify_restricted(self, key: str, value: Any) -> str:
        spec = self.RESTRICTED[key]
        expected = spec["type"]
        # Reject bool for int fields (bool is subclass of int in Python)
        if expected is int and isinstance(value, bool):
            return f"Error: '{key}' must be {expected.__name__}, got bool"
        # Coerce value to expected type (LLM may send "80" instead of 80)
        if not isinstance(value, expected):
            try:
                value = expected(value)
            except (ValueError, TypeError):
                return f"Error: '{key}' must be {expected.__name__}, got {type(value).__name__}"
        # Range check
        old = getattr(self._loop, key)
        if "min" in spec and value < spec["min"]:
            return f"Error: '{key}' must be >= {spec['min']}"
        if "max" in spec and value > spec["max"]:
            return f"Error: '{key}' must be <= {spec['max']}"
        if "min_len" in spec and len(str(value)) < spec["min_len"]:
            return f"Error: '{key}' must be at least {spec['min_len']} characters"
        # Apply
        setattr(self._loop, key, value)
        self._audit("modify", f"{key}: {old!r} -> {value!r}")
        return f"Set {key} = {value!r} (was {old!r})"

    def _modify_free(self, key: str, value: Any) -> str:
        # Reject callables to prevent code injection
        if callable(value):
            self._audit("modify", f"REJECTED callable {key}")
            return "Error: cannot store callable values in _runtime_vars"
        # Recursively validate that value is JSON-safe (no nested references)
        err = self._validate_json_safe(value)
        if err:
            self._audit("modify", f"REJECTED {key}: {err}")
            return f"Error: {err}"
        old = self._loop._runtime_vars.get(key)
        self._loop._runtime_vars[key] = value
        self._audit("modify", f"_runtime_vars.{key}: {old!r} -> {value!r}")
        return f"Set _runtime_vars.{key} = {value!r}"

    @staticmethod
    def _validate_json_safe(value: Any, depth: int = 0) -> str | None:
        """Validate that value is JSON-safe (no nested references to live objects)."""
        if depth > 10:
            return "value nesting too deep (max 10 levels)"
        if isinstance(value, (str, int, float, bool, type(None))):
            return None
        if isinstance(value, list):
            for i, item in enumerate(value):
                if err := SelfTool._validate_json_safe(item, depth + 1):
                    return f"list[{i}] contains {err}"
            return None
        if isinstance(value, dict):
            for k, v in value.items():
                if not isinstance(k, str):
                    return f"dict key must be str, got {type(k).__name__}"
                if err := SelfTool._validate_json_safe(v, depth + 1):
                    return f"dict key '{k}' contains {err}"
            return None
        return f"unsupported type {type(value).__name__}"

    # -- unregister_tool --

    def _unregister_tool(self, name: str | None) -> str:
        if err := self._validate_key(name, "name"):
            return err
        if name == "self":
            self._audit("unregister_tool", "BLOCKED self")
            return "Error: cannot unregister the 'self' tool (would cause lockout)"
        if not self._loop.tools.has(name):
            return f"Tool '{name}' is not currently registered"
        # Stash the tool instance before removing
        tool = self._loop.tools.get(name)
        self._loop._unregistered_tools[name] = tool
        self._loop.tools.unregister(name)
        self._audit("unregister_tool", name)
        return f"Unregistered tool '{name}'. Use register_tool to restore it."

    # -- register_tool --

    def _register_tool(self, name: str | None) -> str:
        if err := self._validate_key(name, "name"):
            return err
        if name not in self._loop._unregistered_tools:
            return f"Error: '{name}' was not previously unregistered (cannot register arbitrary tools)"
        tool = self._loop._unregistered_tools.pop(name)
        self._loop.tools.register(tool)
        self._audit("register_tool", name)
        return f"Re-registered tool '{name}'"

    # -- reset --

    def _reset(self, key: str | None) -> str:
        if err := self._validate_key(key):
            return err
        if key in self.BLOCKED:
            return f"Error: '{key}' is protected"
        if key in self.READONLY:
            return f"Error: '{key}' is read-only (already at its configured value)"
        if key in self.RESTRICTED:
            default = self._loop._config_defaults.get(key)
            if default is None:
                return f"Error: no config default for '{key}'"
            old = getattr(self._loop, key)
            setattr(self._loop, key, default)
            self._audit("reset", f"{key}: {old!r} -> {default!r}")
            return f"Reset {key} = {default!r} (was {old!r})"
        if key in self._loop._runtime_vars:
            old = self._loop._runtime_vars.pop(key)
            self._audit("reset", f"_runtime_vars.{key}: {old!r} -> deleted")
            return f"Deleted _runtime_vars.{key} (was {old!r})"
        return f"'{key}' is not a known property or runtime variable"
