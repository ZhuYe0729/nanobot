---
name: context-manager
description: Context compression rules — how to navigate truncated file content and command output efficiently.
metadata: {"nanobot": {"always": true}}
---

# Context Manager

The runtime **truncates large tool results** to save context window space.
Truncated content is always marked with a `[Context Manager]` notice, which
also tells you exactly how to retrieve the rest.

---

## File reads (`read_file`)

### Large files

When a file exceeds the truncation threshold you will see:

```
[Context Manager] … N lines omitted …
...
[Context Manager] Content truncated.
  → read_file("path/to/file", offset=201, limit=200)  to continue reading
  → grep("<pattern>", "path/to/file")  to search specific content
```

Use the `offset` and `limit` parameters to page through the file:

```
read_file("path/to/file", offset=201, limit=200)   # lines 201-400
read_file("path/to/file", offset=401, limit=200)   # lines 401-600
```

### Repeat reads

If you request a file you have already read in this session, you will
receive a short reminder preview instead of the full content:

```
[Context Manager] You already read this file at iteration N.
The full content is available in your conversation history.
```

**Before re-reading a file**, scroll back through your context — the
earlier result is there. Use `grep` if you need to locate a specific
piece of information quickly.

---

## Command output (`exec`)

Long command output is trimmed to head + tail lines.  The exit code is
always preserved.  To see a specific part of long output:

```bash
some_command | head -n 50     # first 50 lines
some_command | tail -n 50     # last 50 lines
some_command | grep "pattern" # filter by keyword
```

---

## Search results (`grep`)

If a grep returns more matches than the display limit, you will see:

```
[Context Manager] … N more matches omitted.
  → grep("<pattern>", "path", output_mode="files_with_matches")
```

Narrow your pattern or use `output_mode="files_with_matches"` to get a
file list, then read individual files.

---

## General navigation strategy

1. **`list_dir`** — discover what files exist
2. **`grep`** — find which file contains what you need
3. **`read_file` with `offset`/`limit`** — read targeted sections
4. **Avoid re-reading** — the content is already in your context
