# CONFIGURATION KNOWLEDGE

## OVERVIEW

Score: 8; distinct configuration boundary. Owns defaults, env/flag binding, human-size parsing, compressor filtering, and validation.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add runtime config | `config.go` | Struct fields, Viper loading, env bindings, decode hooks |
| Change defaults/limits | `const.go` | Ports, timeouts, queue sizes, clone bounds, HA timing |
| Change validation | `validate.go` | Source/target, listen host, port, and clone-size ranges |
| Test config loading | `config_test.go` | Flags, env vars, decode behavior |
| Test size parsing | `validate_test.go` | Boundary and auto-value cases |
| Test compressors | `compressors_test.go` | Allowlist, trimming, and deduplication |

## CONVENTIONS

- `Load` merges Cobra persistent/local flags with `PCSM_*` environment variables through Viper.
- Hyphenated flag names normally map to underscore-separated env names; `source` and `target` explicitly bind `PCSM_SOURCE_URI` and `PCSM_TARGET_URI`.
- Decode hooks parse durations and comma-separated slices.
- Every runtime flag requiring env support is bound explicitly in `bindEnvVars`.
- Deprecated env names remain accepted only where `WarnDeprecatedEnvVars` names the replacement.
- Compressor values are trimmed, allowlisted, and deduplicated.
- Clone segment/read batch sizes accept human-readable units and zero as auto.
- `Validate` checks zero port as the default without mutating config; size validators accept zero as auto.

## ANTI-PATTERNS

- Do not add a CLI config field without matching mapstructure name, env binding, default, and tests.
- Do not accept `listen-host` values that include a port.
- Do not allow identical source and target URIs.
- Do not bypass clone size bounds after human-readable parsing.
- Do not preserve unknown compressor names.
- Do not introduce another env prefix or undocumented deprecated alias.
