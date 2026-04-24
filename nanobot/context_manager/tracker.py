"""File-read state tracker for the context manager.

Tracks which files have been seen in the current agent session so the
pruner can detect repeat reads and return a short preview instead of
the full content again.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReadRecord:
    """Metadata for a file that has already been read this session."""

    iteration: int
    path: str
    content_preview: str  # first N lines kept for the "already-seen" reminder


class FileReadTracker:
    """Tracks files read via read_file across agent iterations.

    One instance lives inside ContextPrunerHook and persists for the
    lifetime of a single AgentRunner.run() call.
    """

    def __init__(self, preview_lines: int = 50) -> None:
        self._preview_lines = preview_lines
        self._seen: dict[str, ReadRecord] = {}

    def check_and_record(
        self, path: str, iteration: int, content: str
    ) -> ReadRecord | None:
        """Return the previous ReadRecord if this path was already seen,
        otherwise record it and return None.

        The path is normalised (stripped of leading/trailing whitespace)
        before lookup so minor path variations are deduplicated.
        """
        key = path.strip()
        if key in self._seen:
            return self._seen[key]

        # First time — record it
        preview = self._make_preview(content)
        self._seen[key] = ReadRecord(
            iteration=iteration,
            path=key,
            content_preview=preview,
        )
        return None

    # ------------------------------------------------------------------

    def _make_preview(self, content: str) -> str:
        lines = content.splitlines()
        kept = lines[: self._preview_lines]
        preview = "\n".join(kept)
        if len(lines) > self._preview_lines:
            preview += f"\n[… {len(lines) - self._preview_lines} more lines not shown in preview]"
        return preview
