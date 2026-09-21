# Contributing

Thanks for helping. quartzx is a zero-dependency Go library; keep it that way.

## Setup

```sh
git clone https://github.com/JiaBao-do/quartzx
cd quartzx
git config core.hooksPath .githooks   # enables the pre-push hook
```

The pre-push hook checks the exact commit being pushed from a clean checkout: `gofmt`, `go mod tidy`, `go vet`,
`go test -race -shuffle=on`, `go build`, and `golangci-lint` if installed. Never bypass it with `--no-verify`.

## Rules

- Requires Go 1.24+. Do not use APIs newer than 1.24 (for example `sync.WaitGroup.Go`, `errors.AsType`).
- Standard library only. No new dependencies.
- Every change ships with tests. Time-dependent behaviour is tested with `FakeClock`, never `time.Sleep`.
- Exported identifiers need doc comments; user-facing behaviour needs an `Example*` test.
- Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`). Update `CHANGELOG.md`.
