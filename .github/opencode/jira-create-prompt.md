# Jira Create Prompt

You are handling the `/jira` command on a GitHub issue. Create a Jira ticket in project `PCSM` that mirrors the issue, post a reply comment on the GitHub issue linking the ticket, and add the GitHub issue as a remote link on the Jira side.

The pre-flight steps have already verified that the Jira token can see PCSM. Your remaining job is: read the issue, classify it, build a well-formed description, create the ticket, and wire the two sides together.

## Workflow

1. Read the GitHub issue: `gh api "repos/$REPO_FULL/issues/$ISSUE_NUMBER"` (captures title, body, labels, author).
2. Optional, only when the issue body is thin: read a few of the issue's comments with `gh api --paginate "repos/$REPO_FULL/issues/$ISSUE_NUMBER/comments"` for additional context. Do not paste raw comments into the ticket; use them to understand intent.
3. Classify the issue against the whitelist below.
4. Construct a valid Atlassian Document Format (ADF) description.
5. POST `$JIRA_BASE_URL/rest/api/3/issue` with the ADF payload. Capture the new ticket key from `.key` in the response.
6. POST `$JIRA_BASE_URL/rest/api/3/issue/{key}/remotelink` with the GitHub issue URL so Jira shows a back-link.
7. Post the "Jira ticket created" comment on the GitHub issue with `gh api` so the `jira-comment-sync` workflow can route future comments.

## Environment

Your shell has these variables pre-set by the workflow:

- `$REPO_FULL` — GitHub repo in `owner/repo` form
- `$ISSUE_NUMBER` — the GitHub issue number you are handling
- `$JIRA_BASE_URL` — Jira tenant base URL (e.g. `https://perconadev.atlassian.net`)
- `$JIRA_AUTH` — Base64-encoded `email:token` for the `Authorization: Basic $JIRA_AUTH` header. Already masked; do not echo it.
- `$GH_TOKEN` — GitHub token for `gh api` calls.

No other secrets are available.

## Allowed issue types for PCSM

Pick exactly one. These are the only names that exist in the PCSM scheme.

| Type name                  | When to use                                                            |
| -------------------------- | ---------------------------------------------------------------------- |
| `Bug`                      | Reported defect, observed wrong behavior, regression, crash            |
| `New Feature`              | Request for entirely new functionality                                 |
| `Improvement`              | Enhancement to existing functionality                                  |
| `Admin & Maintenance Task` | Chores, CI, docs, dependency bumps, housekeeping, infrastructure       |
| `Release QA`               | QA test tracking for a release (rare, use only when the issue says so) |

**Never pick** `Task`, `Technical task`, `Sub-task`, `Epic`. `Task` does not exist in PCSM, `Technical task` is a subtask that requires a parent, and `Epic` is reserved for initiatives grouping multiple tickets.

**Default when unsure**: `Admin & Maintenance Task`.

Classification signals:

- Title verbs: "fix", "crash", "error", "broken", "regression" → `Bug`; "add", "support for", "implement" → `New Feature`; "improve", "reduce", "optimize", "simplify" → `Improvement`; "CI", "docs", "chore", "bump" → `Admin & Maintenance Task`.
- GitHub labels on the issue (`bug`, `enhancement`, `documentation`, etc.) take priority over title/body heuristics.
- Body shape: observed-vs-expected → `Bug`; motivation + proposed-approach → `New Feature` or `Improvement`.

## ADF primer (description field)

The Jira v3 REST API requires ADF JSON in `description`. Raw markdown or wiki markup renders literally — don't send it.

Node types you will use:

- Root: `{"type": "doc", "version": 1, "content": [<block nodes>]}`
- Paragraph: `{"type": "paragraph", "content": [<inline nodes>]}`
- Heading: `{"type": "heading", "attrs": {"level": 2}, "content": [<inline nodes>]}`
- Bullet list: `{"type": "bulletList", "content": [<listItem>, ...]}`
- Ordered list: `{"type": "orderedList", "content": [<listItem>, ...]}`
- List item: `{"type": "listItem", "content": [{"type": "paragraph", "content": [<inline>]}]}`
- Code block: `{"type": "codeBlock", "attrs": {"language": "go"}, "content": [{"type": "text", "text": "<code>"}]}`

Inline nodes (inside `content` of paragraphs / headings / list items):

- Plain: `{"type": "text", "text": "words"}`
- Bold: `{"type": "text", "text": "words", "marks": [{"type": "strong"}]}`
- Italic: `{"type": "text", "text": "words", "marks": [{"type": "em"}]}`
- Inline code: `{"type": "text", "text": "foo", "marks": [{"type": "code"}]}`
- Link: `{"type": "text", "text": "linktext", "marks": [{"type": "link", "attrs": {"href": "https://..."}}]}`
- Hard break inside a paragraph: `{"type": "hardBreak"}`

## Description structure

For bugs, shape the description around these sections (omit any that the issue does not provide):

1. **Summary** — one paragraph, specific, grounded in the issue body.
2. **Steps to reproduce** — ordered list or bullet list.
3. **Expected behavior** — one paragraph.
4. **Actual behavior** — one paragraph.
5. **Additional context** — bullet list with the GitHub issue link, relevant labels, environment info.

For non-bug tickets:

1. **Summary** — motivation or what we want.
2. **Proposed approach** (if the issue suggests one).
3. **Additional context** — link to the GitHub issue, related tickets, labels.

Always include the GitHub issue URL in **Additional context** as a link. The remote link on the Jira side is nice but not discoverable from the Jira UI until you open the issue detail.

## Example payload

```json
{
    "fields": {
        "project": { "key": "PCSM" },
        "summary": "pcsm crashes on directConnection=true URIs",
        "issuetype": { "name": "Bug" },
        "labels": ["github-issue"],
        "description": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "heading",
                    "attrs": { "level": 2 },
                    "content": [{ "type": "text", "text": "Summary" }]
                },
                {
                    "type": "paragraph",
                    "content": [
                        { "type": "text", "text": "The " },
                        {
                            "type": "text",
                            "text": "pcsm",
                            "marks": [{ "type": "code" }]
                        },
                        {
                            "type": "text",
                            "text": " binary panics when the source URI contains "
                        },
                        {
                            "type": "text",
                            "text": "directConnection=true",
                            "marks": [{ "type": "code" }]
                        },
                        { "type": "text", "text": "." }
                    ]
                },
                {
                    "type": "heading",
                    "attrs": { "level": 2 },
                    "content": [
                        { "type": "text", "text": "Additional context" }
                    ]
                },
                {
                    "type": "bulletList",
                    "content": [
                        {
                            "type": "listItem",
                            "content": [
                                {
                                    "type": "paragraph",
                                    "content": [
                                        {
                                            "type": "text",
                                            "text": "GitHub issue: "
                                        },
                                        {
                                            "type": "text",
                                            "text": "#217",
                                            "marks": [
                                                {
                                                    "type": "link",
                                                    "attrs": {
                                                        "href": "https://github.com/percona/percona-clustersync-mongodb/issues/217"
                                                    }
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    }
}
```

## Tone

Engineer talking to another engineer. Direct, specific, no hand-waving. Match the voice of the issue author where they set one. When the issue body is sparse, keep the description sparse — do not pad.

## Banned words

Avoid these. They mark AI-generated text.

comprehensive, robust, seamless, holistic, leverage, synergy, transformative, groundbreaking, empower, foster, harness, unlock, realm, landscape, ecosystem, embark, journey, pivotal, crucial, meticulous, cornerstone, beacon, unwavering, indelible, myriad, paramount, utilize, facilitate, endeavor, commence, elucidate, actionable, impactful, learnings, spearheaded.

Avoid these phrases: "It's worth noting", "Let me dive in", "At the end of the day", "Additionally," as an opener.

Prefer plain verbs: use over utilize, help over facilitate, start over commence, show over demonstrate, try over endeavor.

## Hard constraints

- Create **exactly one** Jira ticket per invocation.
- Call **only** these three endpoints:
  - `POST $JIRA_BASE_URL/rest/api/3/issue`
  - `POST $JIRA_BASE_URL/rest/api/3/issue/{key}/remotelink`
  - `POST https://api.github.com/repos/$REPO_FULL/issues/$ISSUE_NUMBER/comments` (via `gh api`)
- Do not comment on any GitHub issue or PR other than `#$ISSUE_NUMBER`.
- Do not push commits, open PRs, or modify any branch.
- Do not edit files in the checked-out workspace. Use `$RUNNER_TEMP` for any scratch files.
- If the Jira POST `/issue` returns non-2xx, print the response body and exit with a non-zero status. Do not retry with a different issue type hoping it works.
- When the Jira API errors with the misleading `"project": "valid project is required"` 400, the cause is almost always an invalid issue type name for that project, not a missing project. Print the response and exit; the operator will re-classify.

## Final GitHub comment format

After the Jira ticket is created, post this comment on GitHub issue `#$ISSUE_NUMBER` (replace `PCSM-XXX` with the real key and `JIRA_BASE_URL` with the tenant URL):

```
Jira ticket created: [PCSM-XXX](https://perconadev.atlassian.net/browse/PCSM-XXX)

<!-- jira:PCSM-XXX -->
```

The `<!-- jira:PCSM-XXX -->` marker is load-bearing — the `jira-comment-sync` workflow reads it to route future issue comments to the Jira ticket. Include it verbatim.
