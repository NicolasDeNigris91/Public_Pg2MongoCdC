# Contributing to pg2mongo-cdc

Thanks for considering a contribution. This document covers what you need to
know to get a change merged - environment, conventions, and the bar a PR has
to clear.

If you are reporting a security issue, do **not** open a public PR or issue.
Follow [SECURITY.md](./SECURITY.md) instead.

## Ground rules

- **Stay close to the invariants.** The pipeline has hard guarantees
  (partition-by-PK ordering, LSN-gated upserts, commit-after-side-effect).
  See [`docs/invariants.md`](./docs/invariants.md). A change that breaks one
  of those needs an ADR in `docs/decisions/`, not just a code diff.
- **Tests in the same PR as the code.** Unit tests at minimum; integration
  if you touched the data path; a chaos scenario if you touched failure
  recovery.
- **One topic per PR.** Refactors mixed with feature work get split or sent
  back. Drive-by formatting changes go in their own commit.
- **Conventional commit messages.** `type(scope): summary`, lower-case,
  imperative mood. Existing log is the reference; match its style.

## Local environment

Prerequisites:

- Go 1.26+
- Docker + Docker Compose v2
- `bash` (the chaos scripts assume POSIX bash; on Windows use WSL or Git Bash)
- `helm` 3.16+ if you touch the chart
- `golangci-lint` v2 if you want to run lint locally (CI runs it either way)

Bring up the dev stack:

```bash
cp .env.example .env
docker compose -f docker-compose.yml -f docker-compose.chaos.yml up -d --build --wait
bash scripts/register-connectors.sh
bash scripts/seed.sh
```

Tear down:

```bash
docker compose down -v
```

## Test bar

| Change touches             | Required                                                                |
| -------------------------- | ----------------------------------------------------------------------- |
| Pure refactor / docs       | `make test`                                                             |
| Sink writer / Mongo I/O    | `make test` + `make test-mongo`                                         |
| Cross-service data path    | `make test` + `make test-stack`                                         |
| Failure recovery / retries | `make test-stack` + at least one matching scenario in `chaos/scenarios/` |
| Helm chart                 | `helm lint deploy/helm/pg2mongo-cdc` + `helm template ... \| yq .`      |

CI runs the same pipeline on every PR (`.github/workflows/ci.yml`). Lint,
gosec, trivy, unit, integration-mongo and integration-stack must all be
green before merge.

## Style

- Go: standard `gofmt` + `goimports`, no `interface{}` (`any` instead),
  errors wrap with `%w`. The full linter set is in `.golangci.yml` - run
  `golangci-lint run` from each service directory.
- YAML / JSON: 2-space indent. Sorted keys where order is not significant.
- Bash: `set -euo pipefail` at the top of every script. Every chaos
  scenario must carry a `# PASS:` comment so the CI gate can find it.
- Docs: prose in present tense, code blocks copy-paste runnable. No
  emojis in docs or comments. ASCII hyphens, not em-dashes.

## Commit messages

Follow Conventional Commits with these scopes:

```
feat(sink|transformer|loadgen|chart): ...
fix(sink|transformer|loadgen|chart|chaos): ...
docs: ...
chore: ...
ci: ...
release: vX.Y.Z - <one-liner>
```

The body should explain the *why*. The diff already shows the *what*.

## Pull request workflow

1. Fork and branch from `main`. Branch name is irrelevant; the squashed
   commit on `main` is what survives.
2. Open the PR using the template. Fill the Verification section with
   actual commands you ran, not aspirational ones.
3. CI must pass. If a flake repeats, file an issue rather than re-running.
4. Maintainer reviews and merges (squash). Releases are tagged from `main`
   with `vX.Y.Z`; see `CHANGELOG.md` for the format.

## License

By submitting a PR, you agree that your contribution is licensed under the
[MIT License](./LICENSE) of this project.
