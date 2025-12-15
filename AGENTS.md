# AGENTS.md - Percona ClusterSync for MongoDB

## Build/Lint/Test Commands
- **Build**: `make build` (production) or `make test-build` (with race detection)
- **Lint**: `make lint` (uses golangci-lint with gofmt, gofumpt, goimports)
- **Test Go**: `go test -race ./...` or `make test`
- **Test Python**: `poetry run pytest` or `make pytest`
- **Single Go Test**: `go test -race -run TestName ./package`
- **Single Python Test**: `poetry run pytest tests/test_file.py::test_name`
- **Clean**: `make clean`

## Code Style Guidelines
- **Imports**: Group stdlib, third-party, local (separated by blank lines). Use `goimports` for formatting.
- **Error Handling**: Use custom `errors` package (`errors.Wrap`, `errors.New`, `errors.Join`). Always wrap errors with context. Return `nil` for nil errors in Wrap.
- **Types**: Prefer explicit types. Use generics where appropriate. Document exported types with comments.
- **Naming**: Use camelCase for unexported, PascalCase for exported. Acronyms stay uppercase (e.g., `URI`, `HTTP`).
- **Comments**: Document all exported functions/types with godoc format. Start with the function/type name.
- **Linting**: Follow golangci-lint v2 with most linters enabled (see `.golangci.yml`). Use `//nolint:lintername` sparingly with justification.
- **Context**: Pass `context.Context` as first parameter. Use `util.CtxWithTimeout` for timeout operations.
- **Formatting**: Use `gofmt` and `gofumpt`. Max line length flexible but keep readable. Use tabs for indentation.
- **Testing**: Use `testify` for assertions. Add `-race` flag. Python tests use `pytest` with `pymongo`.
