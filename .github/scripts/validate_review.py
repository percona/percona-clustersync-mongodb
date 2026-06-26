#!/usr/bin/env python3
"""Validate and curate an opencode review JSON payload before posting.

Reads the agent's review JSON, the PR files manifest, and (optionally) the
ANTHROPIC_API_KEY from the environment. Writes a curated GitHub PR review
payload to the output path or exits non-zero if the input is unrecoverable.

The validator is lenient: invalid inline comments are dropped and a note is
appended to the review body. Catastrophic problems (malformed JSON, missing
required keys, secret-bearing summary) exit with code 2.
"""

from __future__ import annotations

import json
import os
import re
import sys

# 5 max inline comments per review. Strict for cost and noise control.
MAX_COMMENTS = 5
# 8 KiB per inline comment body, ~60 KiB total payload.
MAX_BODY_BYTES = 8 * 1024
MAX_PAYLOAD_BYTES = 60 * 1024
# Max span (line - start_line) for multi-line comments. Larger spans render
# a giant highlighted block in the GitHub UI that buries the finding, so we
# silently collapse them to single-line at `line`.
MAX_MULTILINE_SPAN = 15

VERDICTS = {
    "Approve",
    "Approve with Suggestions",
    "Needs Work",
    "Request Changes",
}
SIDES = {"LEFT", "RIGHT"}

# Concrete provider tokens and private-key markers only. Generic hex/base64
# patterns false-positive on legitimate review content (commit hashes, base64
# fixtures, etc.).
SECRET_RE = re.compile(
    r"github_pat_[A-Za-z0-9_]{80,}"
    r"|gh[pousr]_[A-Za-z0-9_]{30,}"
    r"|sk-ant-[A-Za-z0-9_-]{20,}"
    r"|-----BEGIN [A-Z ]*PRIVATE KEY-----"
)

HUNK_HEADER_RE = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def fatal(msg: str) -> None:
    print(f"::error::{msg}", file=sys.stderr)
    sys.exit(2)


def contains_secret(text: str, anthropic_key: str | None) -> bool:
    if anthropic_key and anthropic_key in text:
        return True
    return bool(SECRET_RE.search(text))


def commentable_lines(patch: str) -> tuple[set[int], set[int]]:
    """Return (right_side_lines, left_side_lines) commentable per the patch.

    GitHub allows comments on context and added lines for RIGHT side, on
    context and removed lines for LEFT side. Hunk headers reset the line
    counters; intermediate lines without a recognized prefix are ignored.
    """
    right_lines: set[int] = set()
    left_lines: set[int] = set()
    cur_left = cur_right = 0
    for line in patch.splitlines():
        m = HUNK_HEADER_RE.match(line)
        if m:
            cur_left = int(m.group(1))
            cur_right = int(m.group(2))
            continue
        if not line:
            continue
        c = line[0]
        if c == "+" and not line.startswith("+++"):
            right_lines.add(cur_right)
            cur_right += 1
        elif c == "-" and not line.startswith("---"):
            left_lines.add(cur_left)
            cur_left += 1
        elif c == " ":
            right_lines.add(cur_right)
            left_lines.add(cur_left)
            cur_right += 1
            cur_left += 1
    return right_lines, left_lines


def load_pr_files(pr_files_path: str) -> dict[str, tuple[set[int], set[int]]]:
    with open(pr_files_path, encoding="utf-8") as f:
        files = json.load(f)
    by_path: dict[str, tuple[set[int], set[int]]] = {}
    for entry in files:
        path = entry.get("filename")
        patch = entry.get("patch")
        if not path or not patch:
            continue
        by_path[path] = commentable_lines(patch)
    return by_path


def validate_comment(
    c: object,
    files_lines: dict[str, tuple[set[int], set[int]]],
    anthropic_key: str | None,
) -> tuple[dict | None, str | None]:
    """Return (curated_comment_dict, error_string). One of them is None."""
    if not isinstance(c, dict):
        return None, "comment is not an object"
    path = c.get("path")
    body = c.get("body")
    side = c.get("side", "RIGHT")
    line = c.get("line")
    start_line = c.get("start_line")
    start_side = c.get("start_side", side)

    if not isinstance(path, str) or not path:
        return None, "missing path"
    if not isinstance(body, str) or not body.strip():
        return None, "missing body"
    if side not in SIDES:
        return None, f"invalid side {side!r}"
    if not isinstance(line, int):
        return None, "missing or non-integer line"
    if path not in files_lines:
        return None, f"path {path!r} not in PR diff"
    if len(body.encode("utf-8")) > MAX_BODY_BYTES:
        return None, "body exceeds size limit"
    if contains_secret(body, anthropic_key):
        return None, "body matches secret pattern"

    right_lines, left_lines = files_lines[path]
    target = right_lines if side == "RIGHT" else left_lines
    if line not in target:
        return None, f"line {line} not in patch on {side}"

    out: dict = {"path": path, "side": side, "line": line, "body": body}

    if start_line is not None:
        if not isinstance(start_line, int):
            return None, "start_line must be int"
        if start_side not in SIDES:
            return None, f"invalid start_side {start_side!r}"
        if start_line >= line:
            return None, "start_line must be less than line"
        start_target = right_lines if start_side == "RIGHT" else left_lines
        if start_line not in start_target:
            return None, f"start_line {start_line} not in patch on {start_side}"
        # Collapse over-large spans to single-line at `line` to avoid the
        # giant-highlight UX issue. The finding is preserved; only the range
        # is narrowed.
        if line - start_line <= MAX_MULTILINE_SPAN:
            out["start_line"] = start_line
            out["start_side"] = start_side

    return out, None


def main(review_json_path: str, pr_files_path: str, output_path: str) -> int:
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY") or None

    try:
        with open(review_json_path, encoding="utf-8") as f:
            raw = f.read()
    except FileNotFoundError:
        fatal(f"Review JSON not found at {review_json_path}.")

    # Defensive: agents sometimes wrap JSON output in ```json fences despite
    # the prompt asking for raw JSON. Strip a leading/trailing fence if
    # present so the validator does not fail on harmless wrapping.
    stripped = raw.strip()
    if stripped.startswith("```"):
        first_nl = stripped.find("\n")
        if first_nl > 0:
            stripped = stripped[first_nl + 1 :]
        if stripped.rstrip().endswith("```"):
            stripped = stripped.rstrip()[:-3].rstrip()

    try:
        review = json.loads(stripped)
    except json.JSONDecodeError as e:
        fatal(f"Review JSON is malformed: {e}.")

    if not isinstance(review, dict):
        fatal("Review JSON top level must be an object.")

    verdict = review.get("verdict", "Approve")
    if verdict not in VERDICTS:
        fatal(f"Invalid verdict {verdict!r}.")

    effort = review.get("effort", 1)
    if not isinstance(effort, int) or not 1 <= effort <= 5:
        fatal(f"Invalid effort {effort!r}; must be int 1-5.")

    summary = review.get("summary", "")
    if not isinstance(summary, str):
        fatal("summary must be a string.")
    if contains_secret(summary, anthropic_key):
        fatal("Summary matches secret pattern; refusing to post.")

    comments_in = review.get("comments", [])
    if not isinstance(comments_in, list):
        fatal("comments must be an array.")
    # Per user spec: take first 5, agent is told to prioritize.
    comments_in = comments_in[:MAX_COMMENTS]

    files_lines = load_pr_files(pr_files_path)

    curated: list[dict] = []
    dropped: list[str] = []
    for c in comments_in:
        ok, err = validate_comment(c, files_lines, anthropic_key)
        if ok is not None:
            curated.append(ok)
        else:
            dropped.append(err or "unknown reason")

    # Build the final review body.
    body_parts: list[str] = []
    if summary.strip():
        body_parts.append(summary.rstrip())
    elif not curated:
        # Inline-only reviews are allowed, but if BOTH are empty, synthesize a
        # minimal summary so the posted review carries the verdict.
        body_parts.append(f"## Go Review Summary\n\n**Verdict**: {verdict}")

    if dropped:
        body_parts.append(
            "\n_Note: "
            f"{len(dropped)} inline comment(s) were dropped during validation: "
            + "; ".join(dropped[:5])
            + (f" (+{len(dropped) - 5} more)" if len(dropped) > 5 else "")
            + "._"
        )

    body_parts.append(f"\n<!-- review-effort: {effort}/5 -->")
    body = "\n\n".join(body_parts).strip()

    payload: dict = {"event": "COMMENT", "body": body}
    if curated:
        payload["comments"] = curated

    rendered = json.dumps(payload)
    if len(rendered.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        fatal(f"Final review payload exceeds {MAX_PAYLOAD_BYTES} bytes.")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered)

    # Surface a structured summary line for the workflow log.
    print(
        f"validate_review: verdict={verdict} effort={effort}/5 "
        f"comments={len(curated)} dropped={len(dropped)}"
    )
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            "usage: validate_review.py REVIEW_JSON PR_FILES OUTPUT_PAYLOAD",
            file=sys.stderr,
        )
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2], sys.argv[3]))
