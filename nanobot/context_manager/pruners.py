"""Per-tool content pruning functions.

Each function takes raw tool-result content (string) plus tool arguments
and configuration, and returns either a pruned string or None (no change
needed).

All pruned results include a navigation hint so the model knows how to
retrieve the rest.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nanobot.context_manager.tracker import FileReadTracker, ReadRecord


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_lines(text: str) -> int:
    return text.count("\n") + (1 if text and not text.endswith("\n") else 0)


def _head_tail(text: str, head: int, tail: int) -> tuple[str, int, int]:
    """Split *text* into a head section, omitted count, and tail section.

    Returns (assembled_text, total_lines, omitted_lines).
    """
    lines = text.splitlines(keepends=True)
    total = len(lines)
    if total <= head + tail:
        return text, total, 0

    omitted = total - head - tail
    head_part = "".join(lines[:head])
    tail_part = "".join(lines[total - tail :])
    return head_part, total, omitted, tail_part


def _nav_hint(path: str, next_offset: int) -> str:
    return (
        f"\n\n[Context Manager] Content truncated.\n"
        f"  → read_file(\"{path}\", offset={next_offset}, limit=200)  "
        f"to continue reading\n"
        f"  → grep(\"<pattern>\", \"{path}\")  to search specific content"
    )


# ---------------------------------------------------------------------------
# read_file pruner
# ---------------------------------------------------------------------------

def prune_read_file(
    args: dict[str, Any],
    content: str,
    iteration: int,
    tracker: "FileReadTracker",
    max_file_lines: int,
    head_lines: int,
    tail_lines: int,
) -> str | None:
    """Prune a read_file tool result.

    Two cases:
    1. Repeat read of the same path → return a short reminder.
    2. Large file (> max_file_lines) → head + tail with navigation hint.
    """
    path = args.get("path", "")
    # If the call already used offset/limit the model is navigating deliberately
    # — do not interfere with that.
    explicit_pagination = args.get("offset") is not None or args.get("limit") is not None

    prev = tracker.check_and_record(path, iteration, content)

    # ---- Case 1: repeat read ----
    if prev is not None:
        hint = (
            f"[Context Manager] You already read this file at iteration {prev.iteration}. "
            f"The full content is available in your conversation history.\n"
            f"Showing the first lines as a reference:\n\n"
            f"{prev.content_preview}\n\n"
            f"Use read_file(\"{path}\", offset=N, limit=200) to jump to a specific section."
        )
        return hint

    # ---- Case 2: large file ----
    if explicit_pagination:
        return None  # model is already paginating — don't truncate further

    total = _count_lines(content)
    if total <= max_file_lines:
        return None  # small enough, no change

    result = _head_tail(content, head_lines, tail_lines)
    head_part, total_lines, omitted, tail_part = result
    next_offset = head_lines + 1  # 1-based line offset for the model

    pruned = (
        head_part.rstrip("\n")
        + f"\n\n[Context Manager] … {omitted} lines omitted …\n\n"
        + tail_part.lstrip("\n")
        + _nav_hint(path, next_offset)
    )
    return pruned


# ---------------------------------------------------------------------------
# exec pruner
# ---------------------------------------------------------------------------

_EXIT_CODE_RE = re.compile(r"\n\nExit code: \S+\s*$")


def prune_exec(
    args: dict[str, Any],
    content: str,
    max_exec_lines: int,
    head_lines: int,
    tail_lines: int,
) -> str | None:
    """Prune an exec tool result.

    Keeps head + tail around the exit-code footer, which is always preserved.
    """
    # Separate the exit-code suffix if present (nanobot appends it)
    suffix = ""
    m = _EXIT_CODE_RE.search(content)
    body = content if m is None else content[: m.start()]
    if m is not None:
        suffix = content[m.start() :]

    total = _count_lines(body)
    if total <= max_exec_lines:
        return None

    result = _head_tail(body, head_lines, tail_lines)
    head_part, total_lines, omitted, tail_part = result
    pruned = (
        head_part.rstrip("\n")
        + f"\n\n[Context Manager] … {omitted} lines of output omitted …\n\n"
        + tail_part.lstrip("\n")
        + suffix
        + f"\n\n[Context Manager] Output truncated ({total_lines} lines total). "
        f"Run the command again with `| head -n N` or `| tail -n N` to see specific parts."
    )
    return pruned


# ---------------------------------------------------------------------------
# grep pruner
# ---------------------------------------------------------------------------

_GREP_LINE_RE = re.compile(r"^.*$", re.MULTILINE)
_GREP_MAX_MATCHES = 100


def prune_grep(
    args: dict[str, Any],
    content: str,
) -> str | None:
    """Optionally truncate grep results that have too many matches."""
    lines = content.splitlines()
    if len(lines) <= _GREP_MAX_MATCHES:
        return None

    kept = lines[:_GREP_MAX_MATCHES]
    omitted = len(lines) - _GREP_MAX_MATCHES
    pattern = args.get("pattern", "")
    path = args.get("path", "")
    pruned = (
        "\n".join(kept)
        + f"\n\n[Context Manager] … {omitted} more matches omitted. "
        f"Narrow your search with a more specific pattern or path.\n"
        f"  → grep(\"{pattern}\", \"{path}\", output_mode=\"files_with_matches\")  "
        f"to see which files match"
    )
    return pruned
