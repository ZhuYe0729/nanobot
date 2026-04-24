---
name: github
description: "Clone repos, read code, and interact with GitHub. Works with or without the `gh` CLI — falls back to git + REST API. Supports public and private repos via token auth."
metadata: {"nanobot":{"emoji":"🐙","requires":{"bins":["gh"]},"optional":true,"install":[{"id":"brew","kind":"brew","formula":"gh","bins":["gh"],"label":"Install GitHub CLI (brew)"},{"id":"apt","kind":"apt","package":"gh","bins":["gh"],"label":"Install GitHub CLI (apt)"}]}}
---

# GitHub Skill

Full GitHub operations: clone repos, read files, manage issues/PRs/CI. Works with or without the `gh` CLI installed.

## Authentication

Set a token for private repos and higher rate limits (5 000/hr vs 60/hr unauthenticated):

```bash
export GITHUB_TOKEN="ghp_your_token_here"
```

The token is used automatically by `gh` (if installed), `git`, and all `curl` API calls below.

---

## Cloning Repositories

### With `gh` CLI (preferred when available)

```bash
gh repo clone owner/repo              # current directory
gh repo clone owner/repo ./my-project # specific directory
gh repo clone owner/private-repo      # uses GITHUB_TOKEN automatically
```

### Without `gh` — using `git` directly

**Public repos:**
```bash
git clone https://github.com/owner/repo.git
```

**Private repos (token in URL):**
```bash
git clone https://${GITHUB_TOKEN}@github.com/owner/private-repo.git
```

**Shallow clone (faster, less disk):**
```bash
git clone --depth 1 https://github.com/owner/repo.git
```

---

## Reading Code Without Cloning

Fetch individual file contents via the GitHub REST API — no clone needed.

### Get a file (base64-encoded content)

```bash
curl -sL \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  -H "Accept: application/vnd.github.v3+json" \
  "https://api.github.com/repos/owner/repo/contents/path/to/file.py"
```

### Decode and read a file in one shot

```bash
curl -sL \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  "https://api.github.com/repos/owner/repo/contents/README.md" \
  | jq -r '.content' | base64 -d
```

### Download raw file directly (no base64 decoding)

```bash
curl -sL \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  "https://raw.githubusercontent.com/owner/repo/main/path/to/file.py"
```

### Get a file at a specific ref (branch/tag/commit)

```bash
curl -sL \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  "https://api.github.com/repos/owner/repo/contents/package.json?ref=v2.0.0" \
  | jq -r '.content' | base64 -d
```

---

## Issues

### With `gh` CLI

```bash
gh issue list --repo owner/repo
gh issue create --repo owner/repo --title "Bug" --body "Description"
gh issue view <number> --repo owner/repo
```

### Without `gh` — REST API

```bash
curl -sL "https://api.github.com/repos/owner/repo/issues?state=open&per_page=10" \
  | jq '.[] | .number, .title, .state'
```

---

## Pull Requests

### With `gh` CLI

```bash
gh pr list --repo owner/repo
gh pr view <number> --repo owner/repo
gh pr checks <number> --repo owner/repo
```

### Without `gh` — REST API

```bash
curl -sL "https://api.github.com/repos/owner/repo/pulls?state=open" \
  | jq '.[] | .number, .title, .head.ref'
```

---

## CI / Actions

### With `gh` CLI

```bash
gh run list --repo owner/repo --limit 10
gh run view <run-id> --repo owner/repo
gh run view <run-id> --repo owner/repo --log-failed
```

### Without `gh` — REST API

```bash
curl -sL "https://api.github.com/repos/owner/repo/actions/runs?per_page=5" \
  | jq '.workflow_runs[] | .id, .name, .conclusion'
```

---

## Notes

- **Always prefer `GITHUB_TOKEN` env var** — it works across `gh`, `git`, and `curl`.
- **Public repos** don't require a token, but rate limits are 60/hr unauthenticated vs 5 000/hr with a token.
- **Private repos** always require authentication.
- **Shallow clones** (`--depth 1`) are much faster when you only need the latest code.
- **Raw URLs** (`raw.githubusercontent.com`) are the simplest way to fetch file contents — no JSON or base64 involved.
- The `gh` CLI is optional. All operations have a `curl`/`git` fallback.
