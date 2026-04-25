# Tool Usage Notes

Tool signatures are provided automatically via function calling.
This file documents non-obvious constraints and usage patterns.

## exec — Safety Limits

- Commands have a configurable timeout (default 60s)
- Dangerous commands are blocked (rm -rf, format, dd, shutdown, etc.)
- Output is truncated at 10,000 characters
- `restrictToWorkspace` config can limit file access to the workspace

## glob — File Discovery

- Use `glob` to find files by pattern before falling back to shell commands
- Simple patterns like `*.py` match recursively by filename
- Use `entry_type="dirs"` when you need matching directories instead of files
- Use `head_limit` and `offset` to page through large result sets
- Prefer this over `exec` when you only need file paths

## grep — Content Search

- Use `grep` to search file contents inside the workspace
- Default behavior returns only matching file paths (`output_mode="files_with_matches"`)
- Supports optional `glob` filtering plus `context_before` / `context_after`
- Supports `type="py"`, `type="ts"`, `type="md"` and similar shorthand filters
- Use `fixed_strings=true` for literal keywords containing regex characters
- Use `output_mode="files_with_matches"` to get only matching file paths
- Use `output_mode="count"` to size a search before reading full matches
- Use `head_limit` and `offset` to page across results
- Prefer this over `exec` for code and history searches
- Binary or oversized files may be skipped to keep results readable

## csv_read — Sparse CSV Reading

- Use `csv_read` before `read_file` for CSV analysis, especially when the task asks for counts, sums, filters, row lookup, or exact cell values
- `csv_read` may be disabled by configuration (`tools.csv.enabled=false`); if unavailable, use `read_file`, `grep`, and executable scripts as usual
- Start with `mode="scout"` to inspect schema, row/column counts, inferred column types, and compact samples
- Use `mode="focus"` with `columns`, `filters`, `needles`, or `aggregate` to retrieve only task-relevant evidence instead of the whole table
- Use `mode="verify"` with `verify={row, column, expected}` to confirm exact values before citing critical numbers
- Prefer `aggregate` operations (`count`, `sum`, `avg`, `min`, `max`, `distinct`, `topk`) over manually counting rows from a full CSV read
- Fall back to `read_file` only when you need literal CSV formatting that `csv_read` cannot provide

## json_read — Sparse JSON Reading

- Use `json_read` before `read_file` for JSON analysis, especially for large config files, nested records, arrays of objects, or exact path checks
- `json_read` may be disabled by configuration (`tools.json.enabled=false`); if unavailable, use `read_file`, `grep`, and executable scripts as usual
- Start with `mode="scout"` to inspect root type, top-level keys, arrays, and compact path/type summaries without reading the full JSON
- Use `mode="focus"` with `paths`, `needles`, `array_path`, `filters`, or `aggregate` to retrieve only task-relevant evidence
- Path syntax is JSONPath-lite: `$`, `$.foo.bar`, `foo.bar`, and `$.items[0].name`; complex JSONPath predicates are not supported
- Use `array_path` plus relative filter paths for arrays of objects, for example `array_path="$.users"` and `filters=[{"path":"role","op":"eq","value":"admin"}]`
- Use `mode="verify"` with `verify={path, expected}` before citing critical values
- Fall back to `read_file` only when you need literal formatting or unsupported path syntax

## cron — Scheduled Reminders

- Please refer to cron skill for usage.
