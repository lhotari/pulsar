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

- **D2 — RESOLVED 2026-07-03: verdict (b), fixable — the async path for v4 built-ins ships.**
  Root-cause investigation (d2-investigator) proved both retreat drivers were not fundamental:
  (1) the OAuth2 "refresh-without-reconnect" failure was the old branch's *duplicated* async
  state machine dropping the `ClientCnx.authenticationDataProvider` field write — the wire
  refresh worked; the observable contract (a pre-existing master test,
  `TokenOauth2AuthenticatedProducerConsumerTest.testOAuth2TokenRefreshedWithoutReconnect`)
  broke. (2) the `ClientCnxRequestTimeoutQueueTest` NPE was an unstubbed mock
  `ctx.executor()` reached only because `AuthenticationDisabled` was made async needlessly.
  Stage-3 plan: async for the credential-fetching built-ins (Token/Basic/OAuth2/Athenz/SASL),
  sync-verbatim for Disabled/Tls/KeyStoreTls; ONE continuation-based state machine in
  ClientCnx (inline when the credential future is already done, executor hop only when
  pending; single field-assignment site; unwrap CompletionException to v4 exception types).
  PIP updated with the scope clarification + acceptance invariants + the single-state-machine
  requirement. Full plan: scratchpad d2-resolution.md.

- **D2-original (superseded by the resolution above): v4 async path vs. old-branch CI evidence.**
  The PIP says the migrated built-in v4 plugins give v4 `PulsarClient` users the async path
  automatically (this is Motivation #1's payoff for v4 users). The old branch tried exactly
  that and had to retreat (`d1e7038de0a`): async-enabled built-ins broke OAuth2
  token-refresh-without-reconnect and a mock-executor test; its resolution was "built-in v4
  shims keep the verbatim sync path". That retreat guts Motivation #1 for v4 API users, so
  v2 will first attempt to make the async path *work* (the old branch already found one root
  cause: async connect paths must assign `authenticationDataProvider` exactly like the sync
  path, including on the REFRESH sentinel re-resolve). Fall back to sync-verbatim + update
  the PIP only if the breakage proves fundamental rather than implementation bugs/test
  artifacts. Evidence gate: OAuth2 refresh tests + `ClientCnxRequestTimeoutQueueTest`.

- **D3 (2026-07-03): stage-2 hard requirement from old-branch CI.** The built server-side
  TLS context must honor insecure mode: with `tlsAllowInsecureConnection=true` the factory
  installs a permissive trust manager so handshakes complete and the client cert remains
  available for TLS auth (old branch broke `BrokerServiceTest.testTlsAuthAllowInsecure` by
  omitting this). Also: run spotless/checkstyle before any push.

## Reuse map (from impl-gap-assessor, 2026-07-03)

Old branch `lh-pip-478-impl` (`a8fe04814fe` + CI fixes) is a complete, CI-green build of the
*previous* design revision. Mine-vs-fresh summary:

- **Mine with mechanical renames**: the whole v5 auth capability layer (core, contexts,
  value types, exceptions), `AsyncAuthenticationDriver` (exact match), the bridge family
  (`LegacyV4AuthenticationAdapter` + 3 inner adapters, `V5ToV4AuthenticationAdapter`,
  `TlsAuthentication`), the ~110-LOC `ClientCnx` async carve-out, `PulsarHttpClient` +
  `Factory`/`Config`/`HttpRequest`/`HttpResponse` shapes, v5-native plugin bodies
  (Token/Basic/Athenz/SASL/OAuth2), every server/client integration call site, the
  `JettySslContextFactory` reload rework, file load/watch/cache internals
  (`FileBasedTlsMaterialLoader`/`Source`), and the stage-4 PIP-337 deletion sweep (385-file
  migration off the old SPI).
- **Rewrite fresh**: the TLS SPI core (old = rejected material model exposing
  PrivateKey/cert chains; new = `PulsarTlsFactory` instance factory), the HTTP provider
  layer (old = ServiceLoader-pluggable; new = framework-owned, drop
  `PulsarHttpClientProvider`/`FactoryConfig`/`ClientHttpTlsPurpose`), `FileBasedTlsFactory`
  / `DefaultBrokerTlsFactory`, the Jetty synthesis path.
- **Never existed — build new**: the `pulsar-common-api` Gradle module; the new server
  config keys (`tlsFactoryClassName`/`tlsFactoryConfig` + `brokerClient*`); the
  retained-deprecated-fail-loud v4 builder dispositions and retained-ignored `ClusterData`
  fields (old branch removed config without graceful degradation).
- Deletions to apply during mining: `ChallengeResponseAuthentication`, `StatefulCallContext`,
  `BinaryProtocolAuthData`+`Default…` → `BinaryAuthData(byte[])` record; fix `capability()`
  javadoc (old text said instanceof-based discovery — forbidden by the current design).
- Caveat: old-branch broker/functions/websocket diffstats are inflated by an unrelated
  jakarta/OTel migration riding the branch — don't mine those parts blindly.
- **Correction (stage2-scout, 2026-07-03): the old branch has NO Jetty `reload(...)` rework.**
  Its `JettySslContextFactory` keeps the condemned `getSslContext()` override, merely swapping
  the PIP-337 factory for a `Supplier<SSLContext>`; server wiring is pull-model throughout and
  the listener SPI is dead code on server paths. Liftable from it: only the `Server` ctor
  config block (ciphers/protocols, provider string, client-auth flags, `setSniRequired(false)`,
  Conscrypt loader). The subscribe+`reload()` push path is fresh stage-2 work. The scout's full
  dossier (transplant split, D3 exact fix, config-key insertion map, per-component wiring
  old→new) is preserved in scratchpad stage2-dossier.md and drives the stage-2 brief.

- **D4 (2026-07-03): stage-1 scope includes the bridges and minimal v5-module consumer fixes.**
  The PIP-466 sync `Authentication` stub already has in-module consumers
  (`PulsarClientBuilderV5`, `AuthenticationFactory`, `AuthenticationAdapter`), so "SPI types
  with no consumers" cannot be literally additive: replacing the stub forces those to move.
  Stage 1 therefore ships: the `pulsar-common-api` module + TLS SPI types (fresh), the v5
  auth SPI (mined + renamed), the HTTP SPI types (mined, provider layer deleted), the bridge
  family + `TlsAuthentication` + `AsyncAuthenticationDriver` (mined/copied — rename-only per
  the reuse map), and the minimal builder/factory adjustments to keep the v5 modules
  compiling and their tests meaningful. No ClientCnx wiring, no server changes (stages 2–3).
  v5-experimental tests may be reshaped freely; the "pass unmodified" criterion protects v4
  tests only.
- **D5 (2026-07-03): `pulsar-common-api` is a PUBLISHED module** using
  `pulsar.public-java-library-conventions` + a `pulsar-bom` constraint. Forced by the build
  guard: published modules (pulsar-common, broker-common, proxy…) may only depend on
  published modules, and stage 2 makes them depend on the TLS SPI. Netty/Jetty stay
  `compileOnly`; `opentelemetry-api` `compileOnly` only because `TlsFactoryInitContext`
  exposes it. Watch item: where `FileBasedTlsFactory` (needs netty at runtime) really lands
  in stage 2 without making the API module heavyweight — may need a PIP Public-API-listing
  correction (split package risk if it moves module but keeps the package).

- **D6 (2026-07-03): default-implementation placement (resolves the D5 watch item).**
  `FileBasedTlsFactory` + `TlsContexts` + internal `TlsMaterialSource` → `pulsar-common`,
  package `org.apache.pulsar.common.tls.impl` (netty-handler + tcnative already there; JDK
  *and* OpenSSL contexts natively; `.impl` avoids splitting the SPI package).
  `DefaultBrokerTlsFactory` → `pulsar-broker-common` (Jetty + ServiceConfiguration knowledge),
  constructor takes the composed purpose→policy map. `FileBasedTlsFactory` returns `empty()`
  for the Jetty class (framework synthesizes — the recommended default). PIP Public API
  section corrected accordingly.

- **D7 (2026-07-03, rule-based, execute in stage 2): split-package remediation.**
  stage1-builder discovered `org.apache.pulsar.common.tls` already exists in `pulsar-common`
  (8 hostname-verification helpers: TlsHostnameVerifier, PublicSuffixMatcher, …), so the SPI
  in `pulsar-common-api` currently splits the package (tolerated by the build; precedent:
  `org.apache.pulsar.common.api`). Rule: in stage 2, if the helpers are dependency-light
  (JDK-only), relocate them into `pulsar-common-api` (FQCNs unchanged; `pulsar-common` gains
  the `pulsar-common-api` dependency anyway) — else move the SPI package before anything is
  released. Recorded in the PIP's Resolved decision 5.

## Step log

### 2026-07-03

1. Design revision pass completed on the design branch (5 commits, `8535972951..c1966d8f274`):
   factual fixes, TlsPurpose redesign, complexity cuts, PIP-337 blast-radius specification,
   open issues resolved into decision records. See the PIP's git history for detail.
2. Discovered pre-existing `lh-pip-478-impl` (+ backup branch); recorded in branch registry;
   took decision D1.
3. Created `lh-pip-478-impl-v2`; started this log.
4. **impl-gap-assessor delivered** → reuse map recorded above; decisions D2 (v4 async path)
   and D3 (server insecure-trust requirement) opened from its CI-fix forensics.
5. **codex (GPT-5.5) design review** of the revised PIP: 2 blockers, 6 majors, 2 minors —
   all contract-precision, none conceptual. Applied to `pip/pip-478.md`:
   F1 endpoint-vs-rotation scope note (HTTP clients select TLS by purpose only; endpoint
   hint is the one-shot binary path; SslEngineFactory leaves room for future extension);
   F2 normative binary challenge routing (connect → `BinaryAuthDataProvider`; refresh
   sentinel → same provider, never the challenge handler; other challenges →
   `BinaryAuthChallengeHandler`, absent → `AuthenticationException`);
   F3 `empty()` guardrails (resolved-but-unbuildable fails exceptionally; fallback contract
   binding for custom factories; CLIENT_OAUTH2 example);
   F4 capability lookup contract (stable post-init, cacheable, parent-owned, multi-capability
   objects, concurrent-safe);
   F5 bounded HTTP challenge exchange (max rounds, original timeout budget spans exchange,
   no driver retry of plugin failures, GET re-issue sidesteps body replay);
   F6 factory-owned instance snapshots (consumers never release; Netty refcount rule);
   F7 rotation vs pooled connections (idle eviction on reload + connection-TTL bound);
   F9 blanket never-throw-synchronously rule in the Error model;
   F10 state-slot null-removes + framework-serialized rounds + private-key-class guidance.
   **F8 partially adopted**: added the explicit automation-breakage acknowledgment and the
   rationale; REJECTED codex's keep-PIP-337-adapter-for-one-release suggestion — it would
   preserve exactly the maintenance burden and surface the removal decision exists to end.
6. **Stage 1 complete** (stage1-builder, Opus): commits `849867689f7` (pulsar-common-api
   module: published, bom entry, TLS SPI fresh + HTTP SPI) and `80e5d9df9bc` (v5 auth SPI +
   value types + contexts + exceptions; `AsyncAuthenticationDriver` in pulsar-client-api;
   bridges TlsAuthentication / LegacyV4AuthenticationAdapter(+3 inners) /
   V5ToV4AuthenticationAdapter; deleted the PIP-466 sync stub, v5 `AuthenticationData`,
   experimental `config.TlsPolicy`, old `AuthenticationAdapter`). Verified green:
   `:pulsar-common-api:build :pulsar-client-api-v5:build :pulsar-client-v5:build` (incl.
   checkstyle+spotless) + `:pulsar-client-original:compileJava
   :pulsar-client-admin-original:compileJava` (v4 untouched); new v5 adapter tests 8/8.
   Notable deviations (all PIP-faithful): PulsarHttpClientConfig carries TlsPurpose only (no
   insecure/hostname flags — those are per-purpose factory concerns); blockingExecutor wired
   through adapter + init contexts; BROKER_CLIENT fallback chosen empty. TODOs tagged
   `TODO PIP-478 stage 3`: real executors/HTTP factory in PulsarClientBuilderV5, tlsPolicy()
   mapping, extractTlsMaterial() folding, ClientCnx carve-out. Discovered the split-package
   conflict → D7.
7. **D2 investigation complete** (d2-investigator, Opus): verdict fixable; see D2 above. PIP
   updated (async-scope clarification, acceptance invariants, single-state-machine carve-out,
   CompletionException unwrapping).
8. Applied D6 corrections + D7 note to the PIP (module placements, softened native-support
   claim, Public API section split).
9. Launched two Opus 4.8 assessment agents (read-only):
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
