# PIP-478 implementation log

Working log of the PIP-478 implementation ("Asynchronous v5 client authentication plugin
interfaces and TLS material provider plugin interface"). Maintained by the orchestrating
agent (Fable 5) with implementation work delegated to Opus 4.8 agents in parallel
worktrees/branches. Not part of the PIP itself — this file documents *how* the design was
implemented, the decisions taken along the way, and where every experiment lives.

## Operating rules (standing instructions, 2026-07-03)

- Implement the design in `pip/pip-478.md`. When implementation reality contradicts the
  document, **revisit the document independently** and record the change here and in the PIP.
- Design taste: follow the style of Lari's PIPs/changes and merlimat's code; do **not**
  inherit patterns from the wider codebase uncritically ("garbage and baggage").
- Keep scope limited to PIP-478 goals; touch other things only when necessary, and update
  the design when doing so.
- Keep all experiment branches and commits.
- **Push policy**: NEVER push to `origin` (apache/pulsar) or open PRs there. DO push
  experiment branches to `forked` (lhotari/pulsar) and open PRs *within the fork* to run
  Personal CI (see CONTRIBUTING.md; precedent: lhotari/pulsar#229). Ignore the fork's PR
  title checks; rerun flaky jobs rather than chasing unrelated failures.
- Orchestrator (Fable 5) plans/reviews/decides; Opus 4.8 agents implement. Parallelize via
  worktrees where useful.
- **Cross-model review**: `codex` CLI (v0.141.0, GPT-5.5, user's Max plan) is available for
  independent review — `codex exec review` against the working diff, or `codex exec "<prompt>"`
  for targeted questions. Use at design-sensitive checkpoints (new public API surface after
  stage 1, concurrency-heavy code like the reload/watch machinery, SPI contract questions),
  not routinely. Codex needs its own network access (runs outside the Bash sandbox).

## Branch registry

| Branch | Purpose | Status |
|---|---|---|
| `lh-pip-478-v5-client-async-auth-and-tls-material-provider` | The PIP document (design) | active design branch; doc-only |
| `lh-pip-478-impl` | **Pre-existing** implementation from an earlier design revision (old master base `1fa9e3532b4`, 9 commits, 385 files, +11.6k/−4.6k). Commit message wording ("TLS material provider") suggests the *superseded material-model* SPI, pre-rename capability interfaces | kept as reference / mining source; superseded |
| `lh-pip-478-impl-prerebase-backup` | Backup of the above from an earlier rebase attempt | kept frozen |
| `lh-pip-478-impl-v2` | **Current implementation branch**, created 2026-07-03 from the design branch tip (`c1966d8f274`) | active |

## Design snapshot implemented

`pip/pip-478.md` @ `c1966d8f274` — after the 2026-07-03 revision pass: capability-factory
discovery (never `instanceof`), uniform capability names (`BinaryAuthDataProvider` /
`HttpAuthHeadersProvider` / `BinaryAuthChallengeHandler` / `HttpAuthChallengeHandler`),
instance-factory TLS SPI (`PulsarTlsFactory`, `TlsHandle`, `TlsPurpose` + `TlsEndpoint`
hint, flat `TlsPolicy`), no HTTP backend pluggability, new neutral `pulsar-common-api`
module, PIP-337 removal with full disposition inventory, 4-stage phasing (SPI types →
default factory + server side → client side + bridges → PIP-337 removal last).

## Decision log

- **D1 (2026-07-03): assess before reusing the old impl branch.** `lh-pip-478-impl` is
  large and touches every integration point we need, but was written against a design the
  current PIP explicitly rejects (material-model SPI) and predates the TlsPurpose/TlsPolicy
  simplification, the capability renames, and the HTTP-SPI cuts. Decision: fresh
  implementation on `lh-pip-478-impl-v2` following the PIP's stage 1→4 phasing; the old
  branch is mined for (a) integration call sites it already found, (b) server-side config
  plumbing, (c) test migrations — via an explicit gap assessment, not wholesale rebase.
  Rationale: rebasing 385 files across a changed SPI shape fights the old design at every
  step; the integration *knowledge* is the reusable part, not the diffs.

## Step log

### 2026-07-03

1. Design revision pass completed on the design branch (5 commits, `8535972951..c1966d8f274`):
   factual fixes, TlsPurpose redesign, complexity cuts, PIP-337 blast-radius specification,
   open issues resolved into decision records. See the PIP's git history for detail.
2. Discovered pre-existing `lh-pip-478-impl` (+ backup branch); recorded in branch registry;
   took decision D1.
3. Created `lh-pip-478-impl-v2`; started this log.
4. Launched two Opus 4.8 assessment agents (read-only):
   - **impl-gap-assessor** — classify the old impl branch diff against the current design:
     matches-current / implements-superseded / reusable-integration-scaffolding; deliver a
     reuse map per module.
   - **v5-module-mapper** — map current `pulsar-client-api-v5` / `pulsar-client-v5` contents,
     Gradle new-module mechanics (settings registration, convention plugins, version
     catalog), and the v4 client auth/TLS integration points for stage 1 scaffolding.

## Watch items (implementation risks flagged during design)

- `DefaultBrokerTlsFactory` is specced "configured from ServiceConfiguration" but lives in
  the dependency-light `pulsar-common-api` — the ServiceConfiguration→policies mapping must
  happen broker-side (constructor takes the composed map), or the class moves to
  `pulsar-broker-common`. Resolve during stage 2 and update the PIP's Public API listing.
- `TlsFactoryInitContext.openTelemetry()` forces an `opentelemetry-api` dependency on
  `pulsar-common-api` — check how `pulsar-client-api-v5` handles the same (compileOnly?) and
  keep the module dependency-light.
- Old impl branch may contain fixes for CI regressions (its last commit says so) — check
  what broke there before repeating it.
