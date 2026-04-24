"""ContextPrunerHook — AgentHook that prunes tool results after each iteration.

Intercepts context.messages in-place via after_iteration, which fires
after tool messages have been appended to the message list but before
the next model call.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from nanobot.agent.hook import AgentHook, AgentHookContext
from nanobot.context_manager.pruners import prune_exec, prune_grep, prune_read_file
from nanobot.context_manager.tracker import FileReadTracker

if TYPE_CHECKING:
    from nanobot.config.schema import ContextManagerConfig

logger = logging.getLogger(__name__)


class ContextPrunerHook(AgentHook):
    """Prune tool-result content to reduce context window usage.

    Strategies applied per tool:
      read_file — truncate large files; skip repeat reads with a reminder
      exec      — truncate long command output
      grep      — cap excessive match lists
    """

    def __init__(self, config: "ContextManagerConfig") -> None:
        self._config = config
        self._tracker = FileReadTracker(
            preview_lines=config.repeat_read_head_lines
        )

    # ------------------------------------------------------------------
    # Hook entry point
    # ------------------------------------------------------------------

    async def after_iteration(self, context: AgentHookContext) -> None:
        tool_calls = context.tool_calls
        n = len(tool_calls)
        if n == 0:
            return

        # The last N messages should be the tool responses for this iteration.
        # We match by tool_call_id to be safe (parallel calls may reorder).
        id_to_msg: dict[str, dict[str, Any]] = {}
        for msg in reversed(context.messages):
            if msg.get("role") != "tool":
                break
            tc_id = msg.get("tool_call_id", "")
            if tc_id:
                id_to_msg[tc_id] = msg

        for tc in tool_calls:
            msg = id_to_msg.get(tc.id)
            if msg is None:
                continue
            content = msg.get("content")
            if not isinstance(content, str):
                continue  # multimodal (images etc.) — skip

            pruned = self._dispatch(tc.name, tc.arguments, content, context.iteration)
            if pruned is not None:
                msg["content"] = pruned
                logger.debug(
                    "context_manager: pruned %s result (%d→%d chars)",
                    tc.name,
                    len(content),
                    len(pruned),
                )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _dispatch(
        self,
        tool_name: str,
        raw_arguments: str | None,
        content: str,
        iteration: int,
    ) -> str | None:
        try:
            args: dict[str, Any] = json.loads(raw_arguments or "{}")
        except json.JSONDecodeError:
            args = {}

        cfg = self._config

        if tool_name == "read_file":
            return prune_read_file(
                args=args,
                content=content,
                iteration=iteration,
                tracker=self._tracker,
                max_file_lines=cfg.max_file_lines,
                head_lines=cfg.head_lines,
                tail_lines=cfg.tail_lines,
            )

        if tool_name == "exec":
            return prune_exec(
                args=args,
                content=content,
                max_exec_lines=cfg.max_exec_lines,
                head_lines=cfg.head_lines,
                tail_lines=cfg.tail_lines,
            )

        if tool_name == "grep":
            return prune_grep(args=args, content=content)

        return None
