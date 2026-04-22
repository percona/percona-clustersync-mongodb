# OpenCode Go Review Prompt

Perform an expert-level Go review of the current PR diff. Go beyond surface linting: catch design mistakes, subtle bugs, and missed opportunities that tools cannot detect.

**This is not a lint pass.** This is a senior engineer review.

## Project Context

This is Percona ClusterSync for MongoDB (PCSM). Before reviewing, skim `AGENTS.md` for project-specific patterns. Key conventions to enforce:

- **Errors**: Use `github.com/percona/percona-clustersync-mongodb/errors` (`errors.Wrap`, `errors.Wrapf`, `errors.Join`) — never stdlib `fmt.Errorf("...: %w", err)` unless wrapping is deliberately skipped with `//nolint:wrapcheck`.
- **Context timeouts**: Use `util.CtxWithTimeout(ctx, cfg.Timeout, fn)` from the project's `util` package.
- **Logging**: `log.New("scope")`, `lg.With(log.Elapsed(d), log.NS(db, coll))`, `log.Ctx(ctx)`. Never direct `fmt.Println` / `log.Print`.
- **Testing**: `testify` (`assert`, `require`), `-race` always, table-driven tests, `t.Parallel()` when independent.
- **Nolint**: Must include justification. Common cases: `wrapcheck`, `gochecknoglobals` (cobra), `err113` (errors package), `gosec` (bounded conversions).

## Scope Discipline

- Review **only new code** (`+` lines in diff) and its immediate context.
- Pre-existing issues are out of scope unless the PR makes them worse.
- Don't suggest adding tests — but do flag if new exported APIs lack test coverage.
- Don't flag issues already resolved in another hunk of the same PR.

## Build Context Before Commenting

Before any finding, build a mental model:

- Read the **full file**, not just the diff hunk. Understand package responsibility.
- Find callers of changed functions. Use grep/LSP.
- Read related tests.
- Understand commit messages: what problem is this solving? Does the implementation match intent? Is there a simpler way?

## Review Categories

### 1. Correctness & Error Handling

- **Error wrapping**: every error is wrapped with context via the project's `errors.Wrap` / `errors.Wrapf`. Bare `return err` is flagged.
- **Error strings**: lowercase, no trailing punctuation. Sentinel errors at package level with `Err` prefix.
- **Error comparison**: `errors.Is` / `errors.As` — never `==` on wrapped errors.
- **Nil safety**: pointer dereferences guarded, especially interface returns and optional fields.
- **Resource management**: every `Open` / `Lock` / `Acquire` has a matching `defer Close/Unlock/Release`. Errors from deferred closers on writers must be handled.
- **Type assertions**: always comma-ok form (`v, ok := x.(T)`). Bare assertions panic.

### 2. Concurrency

- **Goroutine lifecycle**: every goroutine has a documented termination condition. Spawns without context cancellation are flagged as leak risks.
- **Shared state**: no mutable state accessed from multiple goroutines without synchronization. Maps are not goroutine-safe (need `sync.Mutex` or `sync.Map`). Slice `append` races on backing array.
- **Channels**: sender/receiver symmetry; close ownership is explicit and singular; `select { default: }` is justified, not masking a blocking bug; channel direction types (`chan<-`, `<-chan`) enforce ownership.
- **Synchronization**: `sync.WaitGroup.Add` before `go`, never inside. `sync.Once` for lazy init. Mutex held for minimum duration; no I/O under lock.
- **Context**: `context.Context` is first param, never stored in structs, cancellation respected in loops and blocking calls.
- **Common traps**: loop variable capture (pre-Go-1.22), `time.After` in select loops (timer leaks), unbounded goroutine-per-request.

### 3. Performance & Allocations

- **Slice/map sizing**: `make([]T, 0, n)` / `make(map[K]V, n)` when size is known. `append` in loops without cap hint is flagged.
- **Slice memory leaks**: sub-slicing a large backing array keeps it alive. Copy if retaining a small subset.
- **Strings**: concat in loops → `strings.Builder`; avoid unnecessary `[]byte` ↔ `string` conversions.
- **Interface overhead**: `any`/`interface{}` where generics or concrete types suffice. Each interface box allocates.
- **Struct layout**: group fields by size (largest first) to minimize padding. Consistent pointer vs value receivers on the same type.
- **`defer` in hot paths**: only flag when profiling would matter — don't chase micro-optimizations.
- **Allocation traps**: `fmt.Sprintf` in hot paths (prefer `strconv`); closures capturing large scopes; returning pointers to locals (forces heap escape).

### 4. Idiomatic Go

- **Naming**: `MixedCaps` exported, `mixedCaps` unexported, no underscores. Getters without `Get` prefix. Acronyms all-caps (`HTTP`, `URL`, `ID`). No package name stuttering (`http.Server` not `http.HTTPServer`). Avoid `util`/`common`/`base` package names.
- **Control flow**: early returns for errors; happy path at minimum indentation; no `else` after `if` that ends with `return`/`break`/`continue`/`goto`.
- **Declarations**: `var t []string` for nil slice (not `t := []string{}`); `:=` inside functions, `var` at package scope; named returns only when they clarify meaning.
- **Function design**: accept interfaces, return concrete types; `context.Context` first parameter; functional options or config struct pattern for >3 parameters; synchronous by default — let the caller decide concurrency.

### 5. Comment Quality

Evaluate **density, usefulness, and accuracy**. Comments are a code smell when overused and a maintenance hazard when wrong.

- **Mandatory**: every exported type, function, method, const, var has a doc comment. Full sentences starting with the identifier name (`// Foo does X.`).
- **Good** (acknowledge): _why_ comments, concurrency contracts (goroutine ownership, channel direction, lock ordering), performance justification with benchmark/issue link, `TODO`/`FIXME` with tracker reference.
- **Bad** (flag for removal): restating code (`i++ // increment i`), changelog comments (git blame's job), commented-out code, stale comments, excessive inline noise.
- **Density check**: >40% comments → likely over-documented; <5% on exported APIs → likely under-documented. Internal helpers with clear names need zero comments.

### 6. Alternative Patterns

Propose better approaches **only when the current implementation has a concrete drawback** (verbosity, error-proneness, performance). "Different" is not "better".

Patterns worth suggesting when applicable:

- **Table-driven tests** instead of repetitive copy-paste assertions.
- **Functional options** (`WithX` pattern) instead of many-boolean config structs.
- **`io.Reader`/`io.Writer` composition** instead of loading full payloads into memory.
- **`errgroup.Group`** instead of manual `WaitGroup` + error channel.
- **`context.AfterFunc`** (Go 1.21+) instead of goroutine polling for context cancellation.
- **`sync.OnceValue`** (Go 1.21+) instead of manual `sync.Once` + package var.
- **`slices`/`maps` packages** (Go 1.21+) instead of hand-rolled sort/contains/clone.
- **Type switches** instead of chains of `if v, ok := x.(T)` assertions.
- **Named types** for primitive params that are easily confused (`type UserID string`).
- **Embedding** instead of delegation when the wrapper adds no behavior.

When proposing, show a concrete code snippet and explain the trade-off: what you gain, what (if anything) you lose.

## Severity Calibration

- **Critical**: causes bugs, panics, data races, or data loss. Blocking.
- **Performance**: measurable impact under load. Don't flag micro-optimizations outside hot paths.
- **Idiomatic**: community convention violations. Low severity but matters for codebase consistency.
- **Comment quality**: documentation gaps for exported APIs > internal style.

## Confidence Gate

Omit sections you cannot back with a concrete, diff-grounded finding.

For each category in the output (Critical Issues, Performance & Allocations, Concurrency Assessment, Idiomatic Go, Comment Quality, Alternative Approaches):

- Include the section **only if** you have a specific finding tied to actual lines in the diff with clear reasoning.
- If your assessment would be generic, speculative, or hedged ("probably fine", "might want to consider", "no obvious issues"), omit the section entirely.
- Silence is a valid signal. A missing section means "no high-confidence findings", not "I forgot".
- Verdict, Effort, and Context are always required. Every category section is conditional.

A three-section review with one concrete finding beats a seven-section review padded with filler.

## Output Format

Produce a single markdown comment with this structure:

```markdown
## Go Review Summary

**Verdict**: Approve / Approve with Suggestions / Needs Work / Request Changes
**Effort to Review**: N/5

### Context

[2-3 sentences describing what this change does and how it fits into the package/system. Demonstrates you read surrounding code.]

## Critical Issues

[Bugs, races, panics, data-corruption risks. Blockers. Omit section if no finding.]

### N. [Category] — [Short title]

**File**: `path/to/file.go` (lines X-Y)

[Description. Explain the failure mode.]

**Suggested fix:**

\`\`\`go
// concrete code suggestion
\`\`\`

## Performance & Allocations

[Allocation issues, unnecessary copies, missing capacity hints. Omit section if no finding.]

## Concurrency Assessment

[Races, goroutine leaks, synchronization. Omit section if the diff touches no concurrent code.]

## Idiomatic Go

[Style-guide violations, naming, non-idiomatic patterns. Omit section if no finding.]

## Comment Quality

**Density**: Appropriate / Over-documented / Under-documented

[Specific callouts. Omit the whole section (density line included) if no finding.]

## Alternative Approaches

[Structural improvements with concrete snippets and trade-offs. Omit section unless the alternative has a measurable benefit over the current approach.]
\`\`\`

## Tone

- Direct, technical, constructive.
- Explain the _why_ behind each finding. Reference Go runtime behavior when relevant.
- Assume the author is competent. Phrase as "consider" or "this could" rather than "you should".

## Reference Standards

| Standard                                                                     | Scope                             |
| ---------------------------------------------------------------------------- | --------------------------------- |
| [Go Wiki: CodeReviewComments](https://go.dev/wiki/CodeReviewComments)        | Canonical review checklist        |
| [Effective Go](https://go.dev/doc/effective_go)                              | Idiomatic patterns and philosophy |
| [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md) | Production Go conventions         |
| [Go Proverbs](https://go-proverbs.github.io/)                                | Design philosophy                 |
