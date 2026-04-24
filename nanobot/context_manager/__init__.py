"""nanobot.context_manager — context window pruning extension.

Provides a low-invasiveness AgentHook that intercepts tool results after
each agent iteration and applies content pruning strategies:

  • Large file reads are truncated to head + tail with navigation hints.
  • Repeat reads of the same file return a short reminder instead of
    the full content, pointing the model back to the earlier turn.
  • Long exec output is truncated, preserving the exit-code footer.
  • Excessive grep matches are capped with a narrowing suggestion.

Usage
-----
Enable via nanobot.yaml / config:

    agent:
      contextManager:
        enabled: true
        maxFileLines: 500
        headLines: 200
        tailLines: 50
        maxExecLines: 200
        repeatReadHeadLines: 50

The hook is wired into AgentLoop._extra_hooks automatically when
enabled (see nanobot/agent/loop.py).
"""

from nanobot.context_manager.hook import ContextPrunerHook

__all__ = ["ContextPrunerHook"]
