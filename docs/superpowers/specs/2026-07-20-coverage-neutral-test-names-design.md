# Coverage-neutral test names

## Goal

Remove code-coverage terminology from test filenames and internal test helpers. Names should describe the exercised behavior or the role of a fixture, without changing test behavior or user-visible `Test...` names.

## Scope

Rename these files:

- `client_coverage_test.go` to `client_edge_cases_test.go`
- `connpool/coverage_extra_test.go` to `connpool/pool_edge_cases_test.go`

Rename these package-private test symbols:

- `newCoverageClient` to `newClientWithInertPool`
- `coverageObservedContext` to `doneCallProbeContext`
- `newCoveragePool` to `newInertPool`
- `mustGetCoverageConn` to `mustGetInertConn`
- `waitForCoverageCondition` to `requireEventually`
- `newClosedCoverageAMQPConnection` to `newClosedAMQPConnection`

The replacement symbols have been checked for package-level collisions.

## Out of scope

- Do not rename any `Test...` or `t.Run` names; they are already behavior-oriented.
- Do not change test logic, assertions, timing, fixtures, or package structure.
- Keep the `.gitignore` coverage-output comment because it describes real Go coverage artifacts.
- Do not split or merge test files.

## Verification

- Search tracked content and paths to confirm no unintended `coverage` terminology remains outside `.gitignore`.
- Run `gofmt` on the renamed test files.
- Run `go test ./...` and `go test -race ./...`.
