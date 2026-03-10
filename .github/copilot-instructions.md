# Copilot Instructions for codecrafters-redis-go

## Build and run
- Local run (same compile/run flow as CodeCrafters scripts): `./your_program.sh`
- Build like CodeCrafters does: `go build -o /tmp/codecrafters-build-redis-go app/*.go`
- General module build check: `go build ./...`

## Tests
- Run all tests: `go test ./...`
- Run a single test (when tests exist): `go test ./redis -run '^TestName$'`

## High-level architecture
- `app/main.go` is the entrypoint: it listens on `0.0.0.0:6379`, creates `redis.NewServer(listener)`, and starts the server loop.
- `redis/server.go` owns connection lifecycle:
  - Accept loop on the listener.
  - One goroutine per client (`go s.handleClient(conn)`).
  - Per-request parsing and command dispatch.
- Request parsing/encoding and command handlers live in `redis/resp.go`:
  - RESP encoders (`EncodeSimpleString`, `EncodeBulkString`, `EncodeError`, null bulk string).
  - RESP array parser (`ReadArray` + internal bulk-string reader).
  - Command handlers (`HandleEcho`, `HandleSet`, `HandleGet`) returning `([]byte, error)`.
- State is in `redis/store.go`: in-memory `map[string]storeEntry` protected by `sync.RWMutex`, with optional expirations.

## Key repository conventions
- RESP-first input handling: `handleClient` peeks the first byte and prefers RESP arrays (`*`) because the tester sends array-form commands.
- Keep command handlers pure-ish: handlers build RESP payloads and return errors; `server.go` converts handler errors to RESP errors and writes responses.
- Normalize command/options using `strings.ToUpper` before dispatch (`PING`, `ECHO`, `SET`, `GET`; `EX`/`PX` for TTL).
- `SET` argument contract is strict:
  - `SET key value`
  - `SET key value EX seconds`
  - `SET key value PX milliseconds`
  - TTL must parse as positive integer.
- Expiration is lazy on read: `Store.Get` checks expiry and deletes expired keys under write lock (no background sweeper).
- Keep local and remote execution behavior aligned by updating both `your_program.sh` and `.codecrafters/{compile.sh,run.sh}` when changing build/run flow.
