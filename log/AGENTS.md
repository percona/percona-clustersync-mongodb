# LOGGING ADAPTER KNOWLEDGE

## OVERVIEW

Score: 8; distinct structured-log boundary. Owns zerolog initialization, stable attributes, context propagation, and MongoDB driver log adaptation.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Configure output | `log.go:InitGlobals` | Writer, level, JSON/console selection, global context logger |
| Change timestamps | `log.go:TimeFieldFormat` | UTC with millisecond precision |
| Add structured fields | `log.go:AttrFn` | Shared operation, namespace, size, count, and optime attributes |
| Change scope handling | `log.go:New` | Replaces the `s` field rather than adding duplicate JSON keys |
| Propagate request logging | `log.go:Ctx`, `WithContext` | Context-backed logger transport |
| Adapt driver logging | `mongo.go` | `options.LogSink` implementation |

## CONVENTIONS

- `InitGlobals` receives its output writer: server output is stdout, client-command output is stderr.
- JSON bypasses the console writer; console output alone uses the color switch.
- Timestamps use UTC; duration fields use fractional seconds rather than integer units.
- `Elapsed` rounds to milliseconds and writes `elapsed_secs`.
- Namespace attributes omit the dot when collection is empty.
- Operation timestamps use a two-element unsigned integer array under `op_ts`.
- `With` derives a logger; `New` replaces scope through `UpdateContext`.
- Driver `Info` messages deliberately map to debug and retain their numeric `level` field.
- Driver errors remain error-level events with the original error attached.
- `MongoLogger(ctx)` captures the context's zerolog logger; keep driver fields on that sink instead of creating a separate global output path.

## ANTI-PATTERNS

- Do not add scope with another `With().Str("s", ...)`; duplicate scope keys motivated the replacement path.
- Do not promote all driver informational events to application info level.
- Do not hardcode stdout in this adapter; client command output must stay separable from logs.
- Do not change attribute units independently of their `_secs` and `_bytes` names.
