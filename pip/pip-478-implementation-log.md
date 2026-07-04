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
| `lh-pip-478-impl-v2` | **Current implementation branch**, created 2026-07-03 from the design branch tip (`c1966d8f274`) | active; Personal CI = lhotari/pulsar#231 |
| `lh-pip-478-tls-default-flip` | v2 + one commit (`98ac5804f9a`): server TLS default flipped to the new SPI path (D8 experiment). Locally green incl. legacy TLS suites through the new path; watch item: AuthenticationTls proxy→broker cert override (stage-3 deliverable) | experiment; Personal CI = lhotari/pulsar#232 |

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

- **D8 (2026-07-04): new server TLS path ships OPT-IN; the default flips on CI evidence.**
  Stage 2b's selection rule keeps default config on the legacy PIP-337 path (custom
  `sslFactoryPlugin` → legacy + deprecation WARN; `tlsFactoryClassName` set → new SPI path;
  else legacy default). Accepted: it keeps trunk releasable and legacy suites undisturbed.
  But the "existing v4 TLS tests pass unmodified" bar is only *proven* when the default
  flips — and bundling that flip with stage-4 PIP-337 removal would entangle two risks. So:
  a dedicated experiment branch (`lh-pip-478-tls-default-flip`) flips the per-component
  defaults and gets its own fork Personal CI run; the flip merges into the main branch once
  green (stage 4 at the latest). Deviations also accepted from 2b: DirectProxyHandler uses a
  ProxyService-owned subscription + volatile SslContext instead of the PIP's one-shot-per-
  connection (one-shot's future would block the Netty event loop in a sync channel
  initializer; endpoint hint moot for the file-based factory) — PIP note when the pattern
  recurs; websocket/worker WEB factories stay JDK-engine (no netty-handler on their compile
  classpath; Jetty consumes JDK SSLContext anyway); TLS reload metrics (`pulsar.tls.reload`)
  not yet emitted by the stage-2a factory — tracked as a follow-up task.

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
9. **Stage 2a complete** (stage2a-builder, Opus): commits `0dc0b0a` (D7 executed — 7
   hostname-verification helpers relocated to pulsar-common-api, FQCNs unchanged,
   pulsar-common now depends on pulsar-common-api), `4e86c1c` (FileBasedTlsFactory core in
   `pulsar-common/.tls.impl`: resolution→fallback→role rule, one-shot + subscribing handles,
   keep-last-good poll fan-out with value-diff suppression, OpenSSL refcount discipline;
   TlsMaterialSource with snapshot→load→commit-on-success mtime handling — deliberately NOT
   FileModifiedTimeUpdater, whose auto-commit-on-check is the retry bug; TlsContexts with
   build-time hostname verification and the D3 insecure rule, client-auth never below
   OPTIONAL; TlsFactoryProbe), `06b96db` (DefaultBrokerTlsFactory + fromServiceConfiguration
   mapping in pulsar-broker-common; JettyTlsFactory — vanilla SslContextFactory.Server,
   setSslContext pre-start, reload() on rotation; plain synthesized SslContextFactory.Client),
   `86a3ea0` (synthesis coverage). 24 new tests green incl. a live-handshake D3 test and a
   live-Jetty rotation test; all pre-existing PIP-337 tests untouched and green. Deviations
   accepted: FileBasedTlsFactorySettings bundles engine/client-auth/refresh (server
   client-auth is configuration-level per the PIP); engine mapping from v4 `sslProvider` is a
   stage-2b TODO.
10. **Personal CI recipe fixed** (PR lhotari/pulsar#231): stale fork master → CONFLICTING →
   no workflows; retargeted onto base branch `pulsar-pip478-ci-base` at the true merge-base
   (name must match pulsar-ci.yaml's `pulsar-*` filter). Recorded in memory.
11. **codex stage-1 API review**: 3 High (bridge layer) + 3 Medium. Triaged in scratchpad
   stage1-fixup-queue.md; headline: AsyncAuthenticationDriver must become exchange-scoped
   (state slot must survive challenge rounds), unsupported binary capability must fail loudly
   (not silently "none"), LegacyV4CredentialAdapter must expose only probed capabilities via
   a capability() override — the capability-factory model working as designed. F5 decided as
   documented no-copy byte[] ownership. Fixup agent queued.
12. **Stage 2b complete** (stage2b-builder, Opus, 7 commits `352c9e78bbc..4af158f15c6`):
   tlsFactory* config keys across ServiceConfiguration/ProxyConfiguration/
   WebSocketProxyConfiguration/WorkerConfig + conf files; shared `TlsFactorySupport`
   selection rule (8/8 unit tests); broker binary (subscribe BROKER, swap-on-rotation) + web
   (JettyTlsFactory reloading Server); proxy ×4 sites (PROXY/WEB/BROKER_CLIENT incl.
   AdminProxyHandler one-shot synthesized Jetty Client, DirectProxyHandler shared rotating
   context); websocket + functions-worker first-time pluggability. New integration tests
   green (broker mTLS produce/consume + HTTPS admin 2/2, proxy 1/1) AND legacy suites
   undisturbed (TlsProducerConsumerTest 10/10, ProxyTlsTest 2/2,
   AdminProxyHandlerKeystoreTLSTest 1/1). Opt-in default → decision D8. Honest gap:
   websocket/worker have no dedicated e2e TLS test (same JettyTlsFactory driver validated
   via broker/proxy + stage-2a live-rotation test).
13. *(entry written by stage1-fixer — see above in file)*
14. **Stage-3 dossier delivered** (stage3-scout): sub-splits 3a–3d with gates; banked in
   scratchpad stage3-dossier-index.md. Orchestrator ruling on its R6: the
   v4-tests-unmodified bar outranks eager probing — if the fail-fast probe breaks lazy v4
   tests, probing scopes to the v5 builder path and the PIP gets a note.
15. **Personal CI #231 GREEN through stage 2b**: 42 checks, 41 pass, 1 skip (CodeQL,
   fork-expected). Flip experiment CI #232 running separately.
16. **Stage 3a complete** (stage3a-builder, Opus, 7 commits `0f4cfdf..75ab6d1`): ClientCnx
   single-continuation carve-out (one state machine; inline-when-done; single
   provider-assignment site on connect+REFRESH; CompletionException unwrapped) +
   async-enabled Token/Basic/OAuth2/Athenz/SASL via one shared `V5BinaryAuthenticationDriver`
   pattern (v4 shims keep verbatim sync surface + heavy logic; v5 bodies minimal).
   **All gates green, including `testOAuth2TokenRefreshedWithoutReconnect` UNMODIFIED on the
   async OAuth2 path — the exact historical failure, now passing (D2 vindicated).**
   ClientCnxRequestTimeoutQueueTest 7/7 (defensive executor stub); new ClientCnxAsyncAuthTest
   (REFRESH ×3 rounds, no reconnect, fresh getCommandData; failure-path close);
   SaslAuthenticateTest 4/4 (multi-round binary through the async path); TLS shims proven
   untouched. REFRESH routing: new exchange per sentinel (mirrors sync re-resolve; PIP-
   consistent). Reviewed deviations, accepted: (a) `newConnectCommand` → overridable
   `buildConnectCommand(AuthData)` hook, 4 overriders migrated incl. ProxyClientCnx
   (necessary for single-continuation + proxy forwarded-auth; touches pulsar-proxy main —
   in-scope collateral); (b) `pulsar-client` gains `api(pulsar-client-api-v5)` (v4 shims
   delegate to v5 bodies; folds v5 auth API into the shaded v4 jars — PIP note queued for the
   next doc batch); (c) sync-throw funneling on eager credential suppliers (+ test).
   3b handoff: late-bind real client services into v5 bodies' init (true off-event-loop
   offload for OAuth2/Athenz lands there); consolidate V5BinaryAuthenticationDriver /
   V4Exchange duplication in 3b/3c; SASL HTTP loop untouched for 3d.
17. **Flip CI #232 classified** (flip-ci-investigator): 3 failures = 2× ONE root cause (the
   known AuthenticationTls→BROKER_CLIENT fold gap: AuthedAdminProxyHandlerTest 401 +
   integration ClientTlsTest.testAdmin[4] 502 wrong-identity) + 1 TLS-independent flake
   (TopicPoliciesTest 422). **No unknown new-path defects.** Flip viable once the fold lands
   (3c scope, banked in stage3-dossier-index with call sites + old-branch mine source
   `AuthenticationDataTlsMaterialSource`). PIP updated: BROKER_CLIENT fold documented.
18. **codex review of the 3a carve-out**: 2×P1 (connect-failure loses mapped v4 exception
   type; empty-challenge misrouted as sentinel — would reset SASL conversations) + P2
   (generation/close guards for stale exchange completions) + P3 (concurrency test gaps).
   isDone-race cleared. All queued in scratchpad stage3a-fixup-queue.md; fixer next in the
   writer queue.
19. **Stage 3b complete** (stage3b-builder, Opus, commits `a4949330749`, `f01d4edeb30`):
   R2 late-binding solved via v5.internal ClientAuthenticationServices(+Aware) —
   PulsarClientImpl binds real scheduler/bounded-blocking-executor/clientInstanceId (+3c stub
   HTTP factory) before auth start; OAuth2/Athenz credential I/O now provably off the event
   loop (new CredentialOffloadTest 3/3). Client TLS onto the new SPI, opt-in per D8
   (ClientTlsFactorySupport: v5 tlsPolicy/tlsFactory or system property → new path; custom
   sslFactoryPlugin → legacy; default → legacy until flip). v5 builder tlsPolicy/tlsFactory
   + built-in AuthenticationTls/KeyStoreTls fold + probe-on-v5-path (R6 ruling). Design
   outcome folded into the PIP: binary transport = async one-shot with TlsEndpoint (initTls
   is already async); synchronous integration points (AHC SslEngineFactory; proxy data path)
   = subscribing form. New V5TlsProducerConsumerTest 2/2 (incl. the fold); legacy TLS suites
   unmodified green; all 3a gates intact. Remaining for 3c: real AHC HTTP factory (stub
   seam), generic third-party hasDataForTls() probe, server-side BROKER_CLIENT fold.
20. **Stage-3a fixup complete** (stage3a-fixer, Opus, commits `e4ed1f02c80`, `bef9f3b4cdd`):
   P1a connect failures now fail connectionFuture with the mapped v4 type before close (all
   three branches); P1b sentinel = REFRESH only (audit: the live bug was the INIT/null
   over-match — empty bytes never matched the old predicate; fix covers all); P2
   generation+liveness guards (event-loop-only counters; REFRESH supersedes in-flight round,
   non-refresh challenge during a pending round dropped at DEBUG — sound because the broker
   never pipelines challenges and SASL rounds can't overlap the initial handshake); P3
   concurrency tests (ClientCnxAsyncAuthTest 2→7: thread affinity, stale-completion,
   close-before-completion, write failure, mapped-type surfacing). **Consolidation done**:
   shared `BinaryAuthenticationExchange` in pulsar-client-original owns the normative
   routing + unwrap + v4 mapping; both drivers delegate (−270 lines of duplication; the
   rules now have one home). All gates green incl. the OAuth2 refresh gate unmodified and
   SaslAuthenticateTest 4/4.
21. **CI #231 GREEN through the 3a fixup** (41 pass / 1 CodeQL-skip @ `619a1d12cd8`) — the
   hardened async-auth carve-out survived the full matrix.
22. **Stage 3c complete** (stage3c-builder, Opus, commits `4bc35123305`, `0cd5a7b6042`,
   `45ad0a7d035`): framework AHC-backed PulsarHttpClient on shared client resources
   (subscribing SslContext per purpose + 5-min connectionTtl; honest caveat: AHC has no
   public idle-pool flush, so rotation is TTL/idle-timeout-bounded — in-code doc); OAuth2 off
   its private clients (Motivation #3 closed for the framework path; TokenClient/
   MetadataResolver now take PulsarHttpClient); BOTH TLS folds — server-side BROKER_CLIENT
   AuthProvidedMaterialSource (auth-cert-wins, rotation-aware; wired proxy-side incl.
   AdminProxyHandler/DirectProxyHandler; broker-side capability+test only) and the generic
   v4 hasDataForTls() builder probe. All gates green incl. AuthedAdminProxyHandlerTest on
   BOTH paths (original file gained only a no-op protected hook — behavior-preserving;
   accepted over duplicating the class) and the full oauth2 suite + TokenOauth2 gate
   unmodified. **Flip blocker fixed and proven locally.**
   Two carried items: (a) STAGE-4 PREREQUISITE — v4 OAuth2 plugin-carried IdP TLS params
   must fold into a CLIENT_OAUTH2/minted purpose policy before the deprecated legacy
   private-client fallback can be removed (today plugin-carried material → legacy fallback
   by design; v5-path CLIENT_OAUTH2 policy rides the framework client); (b) the broker's own
   outbound BROKER_CLIENT client (replication) still needs new-SPI wiring so the broker-side
   fold gains a real consumer.
23. Launched two Opus 4.8 assessment agents (read-only):
   - **impl-gap-assessor** — classify the old impl branch diff against the current design:
     matches-current / implements-superseded / reusable-integration-scaffolding; deliver a
     reuse map per module.
   - **v5-module-mapper** — map current `pulsar-client-api-v5` / `pulsar-client-v5` contents,
     Gradle new-module mechanics (settings registration, convention plugins, version
     catalog), and the v4 client auth/TLS integration points for stage 1 scaffolding.
13. **Stage-1 fixup applied** (all six codex findings from stage1-fixup-queue.md):
   - **F1 (+ PIP edit)** — `AsyncAuthenticationDriver` reshaped to exchange-scoped:
     `newAuthenticationExchange(String)` → `AuthenticationExchange {getAuthDataAsync(),
     authenticateAsync(AuthData challengeOrRefresh)}`. `V5ToV4AuthenticationAdapter` binds ONE
     `SimpleAuthCallContext` per exchange (initial data + REFRESH + all challenge rounds), routing
     per the normative binary rules (REFRESH/INIT sentinel → BinaryAuthDataProvider; other
     challenge → BinaryAuthChallengeHandler, absent → AuthenticationException). Its synthesized v4
     provider overrides `authenticate(AuthData)` to ride the same exchange (INIT reuses the cached
     initial credential to avoid re-seeding CR conversations). PIP carve-out listing + prose updated.
   - **F2** — missing `BinaryAuthDataProvider` now fails loudly: `V5ToV4.start()` throws v4-mapped
     `UnsupportedAuthenticationException`; the exchange's `getAuthDataAsync()` also fails (no more
     `"none"`/empty synth).
   - **F3** — `LegacyV4CredentialAdapter` probes `hasDataFromCommand()`/`hasDataForHttp()` post-start
     on the blocking executor (cached) and overrides `capability(Class)` → empty for unsupported
     kinds; claimed-but-null v4 data → `UnsupportedAuthenticationException`.
   - **F4** — `wrap()` is side-effect-free (method-name heuristic only; no `getAuthData(null)` probe —
     third-party `hasDataForTls()` merge deferred to builder-time stage 3); `configure()` stores
     params; configure/start/probe run in `initializeAsync()` on the blocking executor; the
     `Runnable::run` inline fallback is gone (pre-init capability calls fail with IllegalStateException).
   - **F5** — no-copy byte[] ownership javadoc on BinaryAuthData/AuthChallenge/ChallengeResponse +
     HttpRequest.Bytes/HttpResponse (array fields ⇒ record equals/hashCode is identity-based; no
     value comparisons exist in-tree, so documented, not overridden).
   - **F6** — `PulsarTlsFactory` default endpoint overload wrapped in try/catch → `failedFuture`;
     never-throws-synchronously sentence added to the auth capability interfaces + `PulsarHttpClient`.
   Tests: +3 in `V5ToV4AuthenticationAdapterTest` (exchange state across rounds, REFRESH→provider,
   start-fails-on-missing-binary) and +3 in `LegacyV4AuthenticationAdapterTest` (wrap/configure do no
   v4 calls, probed-capabilities-only, pre-init capability → failed future). Verified: spotlessApply +
   `:pulsar-client-api-v5:build :pulsar-client-v5:build :pulsar-common-api:build
   :pulsar-client-original:compileJava` green; 14 v5 auth tests pass.

24. **Stage 3d complete** (stage3d-builder, Opus): the framework HTTP SASL auth driver —
   the last stage-3 piece. New in `pulsar-client-original` `.impl.auth.v5`:
   `HttpAuthenticationDriver` (the single shared 401→resubmit→200 state machine; bounded
   ≤10 rounds via `MAX_CHALLENGE_ROUNDS`, the original request's timeout budget spans the
   whole exchange, plugin failure fails the request with no driver retry), the thin
   `HttpChallengeTransport` adapter SPI (bodiless GET to the original URI + status/headers),
   `HttpAuthCallContextImpl` (exchange-scoped state slot), and the `AsyncHttpAuthenticationProvider`
   bridge marker (the HTTP mirror of the binary `AsyncAuthenticationDriver`; callers `instanceof`
   the bridge, then discover handling via `capability(HttpAuthChallengeHandler.class)`, never
   instanceof-on-capability). `SaslAuthenticationV5` gained `HttpAuthChallengeHandler` +
   `HttpAuthHeadersProvider`, porting `AuthenticationSasl.newRequestHeader`/`getHeaders`
   (Init/ING/Done/ServerCheckToken, SASL-Token/State/SASL-Server-ID, role-token capture) with the
   PulsarSaslClient conversation + role token held in the call-context slot (exchange-scoped, per
   the PIP — NOT cached across requests; each request re-runs the negotiation, so the client-side
   ServerCheckToken/role-token-expired branches are ported-but-unreached).
   - **Driver dispatch is a side-channel warmup**: the driver runs the GET-loop to obtain the role
     token, then `getHttpHeadersAsync` produces the real request's headers (role token + Done) — the
     faithful shape of what v4 `authenticationStage` + `newRequestHeader` do today.
   - **Admin-adapter shape chosen**: pre-computed exchange (like today) but **driver-owned**, over the
     built-in SASL plugin's **own JAX-RS client** as the default transport (exposed via the bridge) —
     exactly what `authenticationStage` uses. Rejected: (a) plumbing the admin's shared AsyncHttpClient
     into ~40 `BaseResource` subclasses (too invasive; admin also does NOT bind `ClientAuthenticationServices`,
     so no framework HTTP client is available there); (b) a Jersey `ClientRequestFilter` (moves the loop
     into the real-request path, risking body replay for admin PUT/POST — the SASL GET-to-original-URI
     side-channel sidesteps replay). The lookup path uses its **shared AsyncHttpClient** as the transport
     (reuses the client's event-loop/DNS).
   - **Callers wired via the capability**: `HttpClient.executeGet` (lookup, AHC transport),
     `BaseResource.requestAsync` + `ComponentResource.getAuthHeaders` (admin, plugin JAX-RS transport;
     shared `computeAuthHeaders` helper). `ComponentResource`'s blocking `.get()` runs on the admin/app
     thread (never the event loop) and the SASL warmup completes on the plugin's JAX-RS pool → no deadlock.
     The v4 `authenticationStage`/`newRequestHeader` hooks stay **verbatim** as the fallback for third-party
     v4 plugins and single-pass built-ins (Token/Basic/OAuth2 continue on the v4 path unchanged).
   - **Tests**: `HttpAuthenticationDriverTest` (8: multi-round→200, single-round, max-rounds abort,
     timeout-budget abort, plugin-failure-no-retry, non-200/401 fail, default-transport, missing-capability),
     `SaslAuthenticationV5HttpTest` (2: real body + real driver + fake server transport — 2-round + 1-round,
     asserting role-token replay, one-conversation-per-exchange state-slot persistence, Init/ING sequencing).
     Added `testImplementation(project(":pulsar-client-original"))` to auth-sasl for the latter.
   - **Gates green**: `SaslAuthenticateTest` 4/4 (proves HTTP-lookup SASL via `HttpClient` **and** admin
     HTTP SASL via Jersey `BaseResource` **and** binary connect, all e2e over MiniKdc; `isTcpLookup=false`);
     `ProxySaslAuthenticationTest` 1/1 (binary-via-SASL-proxy + admin HTTP SASL setup); `HttpClientTest`,
     `HttpLookupServiceTest`, `SameAuthParamsLookupAutoClusterFailoverTest` 4 (non-SASL lookup regression);
     3a/3b/3c spot gates `ClientCnxAsyncAuthTest` 7/7, `CredentialOffloadTest` 3/3, `TokenOauth2…Test` 2/2;
     `:pulsar-client-original:build` FULL + `:pulsar-client-admin-original:build` + `:pulsar-client-v5:build`
     + `:pulsar-client-auth-sasl:build` all green; spotless + checkstyle clean.
   - **Stage-4 handoff**: the built-in SASL plugin's private JAX-RS client is still the admin-warmup
     transport (and the v4 `authenticationStage`/`newRequestHeader` HTTP hooks remain) — both to be removed
     in stage 4 once the admin path binds framework services / a framework `PulsarHttpClient` (or the driver's
     warmup for admin is moved onto a shared client). The exchange-scoped role token means no cross-request
     role-token cache (extra KDC round-trips vs v4); revisit only if a perf gate needs it.

### 2026-07-04

25. **Stage 4a complete** (stage4a-builder, Opus, 3 commits) — the three prerequisites that must land
   before the PIP-337 removal (stage 4c). Each is its own commit; all locally green.
   - **Item 3 — TLS reload metrics (task #13), commit `3b786973c91` `[feat][misc]`.** `FileBasedTlsFactory`
     now emits the PIP Metrics-section instruments via `TlsFactoryInitContext.openTelemetry()`: new
     `TlsReloadMetrics` builds `pulsar.tls.reload` (counter `{purpose, result=success|failure}`) and
     `pulsar.tls.last_reload_success` (gauge, unix seconds, `{purpose}`) off the injected OpenTelemetry
     root (scope `org.apache.pulsar.tls`); a noop root yields no-op instruments so recording is always
     safe. Semantics chosen deliberately: an attempt is counted per **actual** (re)load — the initial
     load of a purpose, and each rotation where the on-disk material changed and was rebuilt; a steady
     poll that finds no change is **not** counted, so the counter reflects real (re)load events and the
     gauge stops advancing exactly when rotation silently fails **or** silently stops happening (the
     Monitoring-section signal). **Metrics wiring coverage per component**: broker (binary via
     `PulsarChannelInitializer`, web via `WebService`) and the client (via `ClientTlsFactorySupport`) already
     pass a **real** OpenTelemetry into the factory init context, so the instruments are live there with no
     new plumbing; proxy / websocket / functions-worker still pass `OpenTelemetry.noop()` (no OTel-root
     accessor reachable without new plumbing — `PulsarProxyOpenTelemetry`/`PulsarWorkerOpenTelemetry` expose
     only a `Meter`) — documented, left for a follow-up. `opentelemetry-api` added `compileOnly` to
     `pulsar-common` (matching `pulsar-common-api`; the real root is always supplied at runtime by the owning
     component). Unit test with an in-memory OTel SDK reader (4/4): initial success + gauge, initial failure
     + no gauge, rotation reload, failed rotation. `FileBasedTlsFactoryTest` and the other TLS-impl tests
     unmodified green.
   - **Item 1 — broker outbound BROKER_CLIENT onto the new SPI, commit `f56764903ad` `[feat][broker]`.**
     `PulsarService.createClientImpl` (the single funnel for the broker's own outbound `PulsarClient`
     instances — local + per-cluster replication) attaches a per-client `PulsarTlsFactory` to the
     `ClientConfigurationData` via the 3b `tlsFactory` seam when `brokerClientTlsFactoryClassName` is set
     (D8 opt-in; off by default → legacy PIP-337 path unchanged) and the outbound connection is TLS. New
     `ClientTlsFactorySupport.brokerClientTlsFactory` composes `CLIENT_DEFAULT` (the purpose the outbound
     transport requests) from the config's `tls*` fields and folds the broker-client `Authentication` TLS
     material via the **3c `authMaterialSupplier`** (auth-cert-wins) — giving that capability its first real
     consumer, so an `AuthenticationTls` broker-client identity reaches the transport on the new path.
     **Per-cluster wiring outcome: done, without minting `broker-client.<cluster>` purposes** — each outbound
     client owns a factory built from its own `conf`, whose `tls*` fields already carry the per-cluster
     `ClusterData.brokerClientTls*` material (mapped by `BrokerService.configTlsSettings`); the minted-purpose
     shape would only matter for a shared factory, which we deliberately don't use (per-client ownership keeps
     lifecycle/close correct). A non-default class name is instantiated reflectively (self-sources material;
     `brokerClientTlsFactoryConfig` params not yet plumbed to the client init context — noted). Gate:
     `ReplicatorTlsFactoryTest` — replication clients ride the new SPI (`conf.getTlsFactory() != null`) **and**
     messages replicate broker-to-broker across three TLS clusters over the new-path handshake (2/2);
     `ReplicatorTlsTest` unmodified green on the legacy default. Debugging note banked: setting the
     `ServiceConfiguration` `brokerClientTlsEnabled` flag (as `ReplicatorTlsTest` does) perturbs the remote
     `getClusterPulsarAdmin` HTTP path (216× PKIX on `__change_events` + the test topic) **independently of
     PIP-478** — confirmed by reproducing with the factory gate off; the gate test therefore keys off
     `ClusterData` TLS only and does not set that flag.
   - **Item 2 — OAuth2 plugin-carried IdP TLS → CLIENT_OAUTH2 fold, commit (this one) `[feat][client]`.**
     **OAuth2 fold design**: the plugin's IdP TLS parameters (`trustCertsFilePath` / `tlsCertFile` /
     `tlsKeyFile`, issue #24944) are exposed as an `Optional<TlsPolicy>` (`FlowBase.idpTlsPolicy` →
     `AuthenticationOAuth2.idpTlsPolicy`) and folded into **`CLIENT_OAUTH2`** by
     `ClientTlsFactorySupport.composePolicies` — the single compose home that runs for both the v5-builder
     `tlsPolicy` path and the v4 flip path (no `pulsar-client-v5` change needed). The fold is naturally
     opt-in-respecting: `composePolicies` only runs when the client is already on the new TLS path, so a
     legacy/plain-v4 client is untouched. `FrameworkHttpClientFactory.configureTls` already resolves
     `config.tlsPurpose()` = `CLIENT_OAUTH2`, so the framework `PulsarHttpClient` now serves the IdP
     mTLS/custom-trust that previously forced the deprecated private client. `FlowBase`'s discriminator
     **narrows** to "no TLS factory available": framework client when `httpClientFactory != null` and either
     no own IdP material **or** a TLS factory is present (`FrameworkHttpClientFactory.hasTlsFactory()`); the
     deprecated private client stays reachable only on the legacy path (no factory) — removed in 4c. Gates:
     `OAuth2IdpTlsFoldTest` (fold parsing, 4/4), `OAuth2IdpTlsFrameworkClientTest` (WireMock HTTPS + custom CA:
     the framework client trusts the IdP **only** because the plugin's `trustCertsFilePath` was folded into
     `CLIENT_OAUTH2`; without the fold the same request is PKIX-rejected — 2/2; `pulsar.test-certs-conventions`
     added to `pulsar-client` for the shared CA certs). Full oauth2 suite + `TokenOauth2AuthenticatedProducer
     ConsumerTest` (2/2) + `CredentialOffloadTest` (3/3) unmodified green.
   - **Cross-cutting gates green**: `ClientCnxAsyncAuthTest`, `V5TlsProducerConsumerTest`, `SaslAuthenticateTest`
     (both items touch shared `ClientTlsFactorySupport`), `:pulsar-client-original` jar +
     `:pulsar-client-v5`/`:pulsar-client-admin-original` compile, `:pulsar-common` build; spotless + checkstyle
     clean across touched modules.
   - **Stage-4b/4c handoff**: (a) admin (`PulsarAdmin`) outbound HTTP still rides the legacy path — its
     `AsyncHttpConnector` is a separate connector from the `createClientImpl` funnel, so the broker's
     `getClusterPulsarAdmin` / `getCreateAdminClientBuilder` are **not** yet on the new SPI (stage 4b). (b) The
     deprecated OAuth2 private `AsyncHttpClient` fallback in `FlowBase.getHttpClient` is now unreachable when a
     TLS factory exists, but stays for the legacy/plain-v4 path — **stage 4c removes it** together with the
     PIP-337 factory. (c) proxy/websocket/functions-worker TLS metrics stay noop until an OTel-root accessor is
     plumbed. (d) a non-default `brokerClientTlsFactoryClassName` custom class works but its
     `brokerClientTlsFactoryConfig` params aren't yet delivered to the client init context.

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

26. **Post-fold flip CI GREEN — 41/41** (PR #232, flip rebased onto full stage-3c): the whole
    CI matrix passes with the new TLS SPI as the *default* server path; both classified
    failures dissolved with the 3c fold, the unrelated flake did not recur. D8's evidence
    gate satisfied.

27. **Stage 4b (flip) executed**: the validated flip cherry-picked onto v2 as `e5394319f44`
    (message updated to record the D8 validation trail). The new TLS SPI is now the DEFAULT
    on the main implementation branch; the legacy path remains selectable via a custom
    `sslFactoryPlugin` until stage 4c applies the dispositions. Remaining before 4c, from
    the 4a handoff: (a) broker outbound **admin** clients (AsyncHttpConnector path, not the
    createClientImpl funnel) onto the new SPI; (b) OTel roots for proxy/websocket/worker TLS
    metrics; (c) brokerClientTlsFactoryConfig params delivery to custom factories.

28. **Stage 4b remainder complete** (stage4b-remainder, Opus, 3 commits) — the three items
    from entry 27's remainder list, closing the pre-4c gap. Each is its own commit; all
    locally green. The flip is live, so default-config server suites exercise the NEW path;
    nothing here regressed a default-path suite (verified below).
    - **Item 1 — admin (`AsyncHttpConnector`) onto the new SPI, commit `22818309907`
      `[feat][admin]`.** For an https admin URL, `AsyncHttpConnector` now resolves the new-SPI
      factory via `ClientTlsFactorySupport.resolveClientTlsFactory` — adopting a factory
      attached through the 3b `tlsFactory` seam (the broker's admin-client attach) or the
      client-side flip selection; `null` on the legacy path, where the reflective
      `PulsarSslFactory` block is kept **verbatim**. On the new path it subscribes once to the
      `CLIENT_DEFAULT` `SslContext` and backs the AsyncHttpClient `SslEngineFactory` with the
      latest (rotated) context — the exact `HttpClient`/`FrameworkHttpClientFactory` pattern
      (hostname verification / trust / ciphers baked into the factory context, so the AHC
      endpoint-id / insecure-trust flags are not applied). The connector owns the factory + a
      single-thread rotation executor, closing both in `close()`. **PulsarAdminBuilder seam
      used:** the broker reaches the admin builder's internal `ClientConfigurationData`
      through `PulsarAdminBuilderImpl.getConf()` (a public Lombok `@Getter`) and attaches a
      per-client factory in `PulsarService.applyBrokerClientTlsFactoryToAdmin(PulsarAdminBuilder)`,
      called from `getCreateAdminClientBuilder` and `BrokerService.getClusterPulsarAdmin`
      (gated on `brokerClientTlsFactoryClassName` + an https URL, mirroring 4a's
      `maybeApplyBrokerClientTlsFactory`). `opentelemetry-api` added `compileOnly` to
      `pulsar-client-admin` (init context carries a root; real root supplied at runtime).
      Gate: new `BrokerAdminClientTlsFactoryTest` — the broker's own admin client rides the
      new SPI (`getAsyncHttpConnector().getTlsFactory() != null`) and a real create/get-cluster
      round-trip completes over the new-path handshake (HTTP 200). Regression: `AsyncHttpConnectorTest`
      + `AsyncHttpConnectorSocks5Test`, `BrokerAdminClientTlsAuthTest` (legacy default), and
      `ReplicatorTlsFactoryTest` unmodified green; `:pulsar-client-admin-original:build` FULL green.
    - **Item 2 — OTel roots for proxy/worker TLS metrics, commit `f4b7075dab9`
      `[feat][misc]`.** `PulsarProxyOpenTelemetry` / `PulsarWorkerOpenTelemetry` expose the
      OpenTelemetry root (`getOpenTelemetry()`) — the only new surface. **OTel outcome per
      component:** *proxy* — PROXY (`ServiceChannelInitializer`) and BROKER_CLIENT
      (`ProxyService`) feed `proxyService.getOpenTelemetry().getOpenTelemetry()`; WEB
      (`WebServer`) gains an overloaded 3-arg constructor fed by `ProxyServiceStarter` (2-arg
      callers keep noop); the admin proxy (`AdminProxyHandler`) gains a 4-arg constructor fed
      by the same starter (3-arg kept for tests) — **all four proxy TLS factories now emit**.
      *functions-worker* — `WorkerServer` feeds the `PulsarWorkerService` root into the WEB
      init context (noop when unavailable) — **emits**. *websocket* — the standalone WebSocket
      proxy has **no OpenTelemetry infrastructure** on its classpath (no `OpenTelemetryService`),
      so the init context **stays noop, documented in `ProxyServer`** (the broker's own listeners
      emit when WebSocket runs embedded). Gate: new `ProxyTlsFactoryMetricsTest` — the proxy's
      default PROXY factory records `pulsar.tls.reload{purpose=proxy,result=success}` = 1 and
      advances the last_reload_success gauge through an in-memory-reader root, and
      `PulsarProxyOpenTelemetry.getOpenTelemetry()` is not the no-op. `:pulsar-proxy`
      compileJava + `ProxyServiceTlsStarterTest` (default/new path) green;
      `:pulsar-functions-worker` compileTestJava green.
    - **Item 3 — `brokerClientTlsFactoryConfig` params delivery, commit (this one)
      `[feat][broker]`.** A new transient `ClientConfigurationData.tlsFactoryParams` seam
      carries a custom factory's params to `TlsFactoryInitContext.params()`:
      `ClientTlsFactorySupport.initContext` now reads it (empty map when unset; the default
      file-based factory ignores params). The broker parses `brokerClientTlsFactoryConfig` the
      same way the server-side `tlsFactoryConfig` is parsed (`TlsFactorySupport.parseFactoryConfig`)
      and sets it on the config at **both** 4a/1 attach points (`maybeApplyBrokerClientTlsFactory`
      for the binary funnel and `applyBrokerClientTlsFactoryToAdmin` for the admin path), so the
      admin path (item 1, which shares `resolveClientTlsFactory`) gets params for free. Gate: new
      `ClientTlsFactoryParamsDeliveryTest` — a params-capturing fake `PulsarTlsFactory` receives
      the delivered `{issuerUrl, region}` through the init context, and an unset config yields an
      empty (never-null) map.
    - **Cross-cutting gates green** (both items 1 and 3 touch shared `ClientTlsFactorySupport` /
      `PulsarService`): `ClientCnxAsyncAuthTest`, `V5TlsProducerConsumerTest`,
      `OAuth2IdpTlsFrameworkClientTest` + `OAuth2IdpTlsFoldTest` (the OAuth2 tests exercise the
      changed `resolveClientTlsFactory`/`initContext`), and `BrokerAdminClientTlsFactoryTest`
      (re-run after the item-3 `PulsarService` change) all green; spotless + checkstyle clean
      across touched modules. No default-path regression observed — the flip stays green.
    - **Stage-4c go/no-go: GO.** The three pre-4c prerequisites (admin on the new SPI; OTel roots
      wired where infra exists; custom-factory params delivered) are done, so 4c can now remove
      the PIP-337 `PulsarSslFactory` path wholesale (the legacy blocks in `AsyncHttpConnector` /
      `HttpClient` / the server `*ChannelInitializer`s, the `sslFactoryPlugin` config family, and
      the deprecated OAuth2 private `AsyncHttpClient` fallback) without leaving a component on a
      dead path. Residual 4c notes: (a) the built-in SASL plugin's private JAX-RS admin-warmup
      transport (entry 24 handoff) is orthogonal to TLS and still pending; (b) the WebSocket
      standalone TLS metrics stay noop by design until/unless that module gains OTel infra —
      not a 4c blocker.

29. **Stage 4c (PIP-337 removal sweep) executed** (stage4c-builder, Opus, 6 commits on
    `lh-pip-478-impl-v2`, all local, per the scout checklist + orchestrator rulings R1–R8). The
    PIP-337 `PulsarSslFactory` path is now fully removed; the `PulsarTlsFactory` SPI is the only TLS
    path everywhere. Commits, in compile-safe order (each compiles + passes its chunk gates):
    - **`[improve][misc]` decouple defaults** — every default that named the removed
      `DefaultPulsarSslFactory` FQCN goes to `""`/null (ServiceConfiguration×2, ProxyConfiguration×2,
      ClientConfigurationData, ClientBuilderImpl blank-fill, ClusterDataImpl builder); both
      `isLegacyCustom` predicates match the removed FQCN as a string literal (R2). Behaviour
      unchanged; just severs the compile-time class dependency.
    - **`[improve][broker]` server collapse** — `TlsFactorySupport.selectPath`/`TlsPath`/
      `LEGACY_WARNED` deleted; all 8 server call sites (broker PulsarChannelInitializer + WebService,
      proxy ServiceChannelInitializer + WebServer + AdminProxyHandler + ProxyService↔DirectProxyHandler,
      functions-worker WorkerServer, websocket ProxyServer) simplified to new-path-only. `isLegacyCustom`
      survives as the fail-loud trigger (R3). `TlsFactorySupportTest` migrated (R8-adjacent: its
      selection cases → an `isLegacyCustom` case, needed here for the module test to compile).
    - **`[improve][client]` client collapse + R4 binding** — `ClientTlsFactorySupport.useNewTlsPath`
      and its `pulsar.client.tls.useNewTlsFactory` flip property deleted; `resolveClientTlsFactory`
      always builds a factory; HttpClient / client PulsarChannelInitializer / AsyncHttpConnector /
      FlowBase lose their reflective `PulsarSslFactory` arms. **R4:** verified that `PulsarAdminImpl`
      did **not** bind `ClientAuthenticationServices` (only `PulsarClientImpl` did) — admin-only OAuth2
      previously rode the deleted private client — so the 3b binding was **extended to PulsarAdminImpl**
      (mirrors PulsarClientImpl; HTTP-only, so AsyncHttpClient self-provisions its event loop). FlowBase's
      genuinely-unbound path throws the actionable `IllegalStateException` (R4).
    - **`[feat][misc]` dispositions** — fail-loud at PulsarService.start / ProxyServiceStarter /
      ClientBuilder.build / PulsarAdminBuilder.build (R1); ClusterData per-cluster plugin
      retained-but-ignored with a once-per-cluster WARN in BrokerService and the plugin params stripped
      from `configTlsSettings`/`configAdminTlsSettings` + all 4 call sites, plus the stale forwardings in
      PulsarService/NamespaceService/CompactorTool unwired (R6); testclient CLI options removed;
      CmdClusters options kept-but-deprecated (R5); `@Deprecated` + migration javadoc on the retained v4
      builder methods and config fields; Lombok-propagated getter-deprecation localized with
      `@SuppressWarnings` on the enforcement paths.
    - **`[fix][client]` auth-identity fold** — a regression the collapse *exposed* (not introduced by
      design): `resolveClientTlsFactory` never folded the auth plugin's own TLS identity
      (AuthenticationTls / AuthenticationKeyStoreTls), which on the base was served by the now-removed
      per-host/legacy path. Fixed by folding the plugin material into CLIENT_DEFAULT (auth-cert-wins)
      **only when the client configures no tls\* key/keystore of its own**, via a supplier evaluated
      lazily by AuthProvidedMaterialSource (no eager `getAuthData()` at construction; a token/OAuth2
      plugin yields nothing rather than failing). This was the trickiest part — three iterations to
      satisfy all of: keystore-TLS-auth clients/admins present their cert (KeyStoreTls…WithAuthTest,
      AdminApiKeyStoreTlsAuthTest), no eager auth at build (ClientInitializationTest), fake/non-TLS auth
      untouched (TlsProducerConsumerTest.testTlsWithFakeAuthentication), OAuth2 folds intact.
    - **`[improve][misc]` deletions + test migration** — deleted `PulsarSslFactory`,
      `DefaultPulsarSslFactory`, `PulsarSslConfiguration`, `KeyStoreSSLContext`, `JettySslContextFactory`,
      `PulsarHttpAsyncSslEngineFactory` and the 4 tests coupled to them; `SslContextTest`'s
      SslProvider×cipher×keystore/PEM matrix ported onto `FileBasedTlsFactory` (OpenSSL still rejects the
      JDK-named ciphers) and the old file deleted (R8); `FileModifiedTimeUpdaterTest` lost only its
      legacy method (R8); `OAuth2MockHttpClient` deleted (its 3 callers only exercise config parsing);
      `AdminApiKeyStoreTlsAuthTest` rebuilt on the JDK KeyStore/SSLContext APIs; new
      `AdminOnlyOAuth2AuthTest` (R4 gate, WireMock IdP); stale `{@code}`/conf comments reworded.
      `SSLContextValidatorEngine` left in place (now test-orphaned — a follow-on cleanup outside this PIP,
      R7).
    - **R4 verification outcome:** PulsarAdminImpl needed the binding (it did **not** bind before); the
      extension makes admin-only OAuth2 acquire its token over the framework client, proven green by the
      new `AdminOnlyOAuth2AuthTest` and the unchanged `TokenOauth2AuthenticatedProducerConsumerTest`.
    - **Gates (targeted `--tests`, all green):** full builds `:pulsar-common` `:pulsar-broker-common`
      `:pulsar-client-original` `:pulsar-client-admin-original` `:pulsar-client-v5`; compileJava+
      compileTestJava for broker/proxy/websocket/functions-worker/testclient/client-tools; checkstyle on
      all touched modules; ConfigValidationTest; FileBasedTlsFactoryTest (incl. the R8 port) +
      FileModifiedTimeUpdaterTest; the oauth2 unit suite + OAuth2IdpTlsFrameworkClientTest +
      OAuth2IdpTlsFoldTest + ClientInitializationTest; TlsProducerConsumerTest, V5TlsProducerConsumerTest,
      ReplicatorTlsFactoryTest, ClientCnxAsyncAuthTest, AdminApiKeyStoreTlsAuthTest,
      KeyStoreTlsProducerConsumerTestWithAuthTest, TokenOauth2AuthenticatedProducerConsumerTest,
      AdminOnlyOAuth2AuthTest; AuthedAdminProxyHandlerTest (+NewTlsPath variant), ProxyTlsTest. One
      **pre-existing flake** observed and dismissed: `AsyncHttpConnectorTest.testShouldStopRetriesWhenTimeoutOccurs`
      (a 500 ms http retry-timeout test, no TLS, unrelated to this change) failed once under concurrent
      build load and passed in isolation.
    - **Checklist deviations / drift:** (a) `TlsFactorySupportTest` migration and the OAuth2MockHttpClient
      retarget were pulled forward from the "delete last" commit into their owning collapse commits where
      module test-compile required it (compile-safe ordering). (b) `useNewTlsPath` +
      `USE_NEW_TLS_FACTORY_PROPERTY` were **deleted** (no external callers) rather than pinned always-true —
      cleaner than the checklist's "→ always new". (c) The client auth-identity fold (the `[fix]` commit)
      was **not** in the checklist; it is a real regression the collapse exposed and required a scoped,
      lazy solution. (d) PIP `pip-478.md` untouched (the R1/R5 edits remain the orchestrator's, per the
      caution). No line-ref drift found — the checklist anchors matched `edf08889c8e`.

30. **Closing cross-model review fixup (F1/F2/F3/F5)** — the ship-blockers from the final review, 3 commits
    on `lh-pip-478-impl-v2` (all local). Trailer `Assisted-by: Claude Code (Opus 4.8)`.
    - **F1 `[fix][misc]` — wire the `SSLContext`-fallback synthesis.** The PIP's tier-3 story (framework asks
      the factory for the Netty `SslContext`; on `empty()` it requests `javax.net.ssl.SSLContext` for the same
      purpose and synthesizes) was **never wired** — every binary/HTTP consumer requested `SslContext.class`
      directly and failed on `empty()`. Added one shared acquisition point in `pulsar-common`
      (`TlsContextAcquisition.acquireNettyContext`, three forms: one-shot, one-shot+endpoint, subscribing) that
      tries `SslContext` → on `empty()` tries `SSLContext` (same form; subscriptions re-wrap each delivery) →
      synthesizes with the role from `purpose.role()`, else the existing empty/failure. Routed **all** listed
      consumers through it: broker `PulsarChannelInitializer`, proxy `ServiceChannelInitializer` + `ProxyService`
      (BROKER_CLIENT), client `HttpClient` + `PulsarChannelInitializer` + `FrameworkHttpClientFactory` +
      `AsyncHttpConnector`, and the `ClientTlsFactorySupport` fail-fast probe.
      - **Synthesized-client hostname verification (the flagged design question) — resolved faithfully, no
        compromise.** A JDK `SSLContext` cannot carry `endpointIdentificationAlgorithm` (engine-level, not a
        context property), and the client consumers rely on the context to carry verification (they do **not**
        re-apply `SecurityUtility.configureSSLHandler`). The synthesized fallback is **always** JDK-backed
        (`JdkSslContext` over the factory's `SSLContext`), and PIP-478 Detailed Design (Well-Known Classes,
        "client-side hostname verification") states the JDK backend permits a clean per-`SSLEngine` override.
        So `TlsContexts.synthesizeNettyClientFromJdk(ctx, enableHostnameVerification)` wraps the JDK context in
        `HostnameVerifyingSslContext` (a delegating `SslContext` whose `newEngine(...)` sets
        `endpointIdentificationAlgorithm="HTTPS"` on every engine) when verification is on — the faithful
        equivalent of the native path's `SslContextBuilder.endpointIdentificationAlgorithm("HTTPS")`. The flag
        is threaded from each consumer's own config (`TlsSynthesisSpec.client(tlsHostnameVerificationEnable)`;
        proxy→broker uses `tlsHostnameVerificationEnabled`) because the framework can't read a custom factory's
        internal policy. Server purposes thread `TlsSynthesisSpec.server(tlsRequireTrustedClientCertOnConnect)`.
      - **F5 folded in:** deleted the dead `LegacyV4TlsAdapter.extractTlsMaterial()` + its stale "stage 3" TODO
        (nothing called it — `unwrapV4` reads `tlsAdapter.v4` directly) and the now-orphaned nested `LOG` field.
      - **Gate:** new `SslContextFallbackSynthesisTest` (pulsar-proxy) with `SslContextOnlyTlsFactory` (a factory
        that returns `empty()` for `SslContext` and supplies only `SSLContext`, delegating load to a
        `FileBasedTlsFactory`; counts refused Netty requests to prove synthesis fired). Verifies broker binary
        listener + client binary (direct v5 over `pulsar+ssl`), broker admin client (`AsyncHttpConnector`, the
        same client-HTTP synthesis path `HttpClient` uses), and the proxy's BROKER_CLIENT synthesized context
        (`getBrokerClientSslContext()` — usable client context) + PROXY synthesis at startup. 3/3 green.
    - **F2 `[fix][broker]` — Jetty optional-client-cert trust scoping.** `applyServerConfig` set
      `setTrustAll(true)` unconditionally under optional auth; changed to `setTrustAll(allowInsecureConnection)`,
      threaded from each web caller's `tlsAllowInsecureConnection` (broker `WebService`, proxy `WebServer`,
      websocket `ProxyServer`, functions `WorkerServer`). **IMPORTANT nuance discovered (flagged below):** on
      the framework's `setSslContext` path Jetty's `setTrustAll` is **inert** — Jetty's `load()` honours
      `trustAll` only when it builds the `SSLContext` itself (`_setContext == null`). The trust scoping is
      **actually enforced by the WEB `SSLContext`'s trust managers**, which `DefaultBrokerTlsFactory` already
      builds from `tlsAllowInsecureConnection` (CA-validating when secure, `InsecureTrustManagerFactory` when
      insecure). So the change removes the misleading unconditional `setTrustAll(true)` and aligns the flag for
      defence-in-depth; it does not itself alter runtime behaviour (the desired semantics were already correct
      via the `SSLContext`). Comment in `applyServerConfig` documents this.
      - **Gate:** new `JettyTlsFactoryTest.optionalClientAuthScopesTrustAllToInsecureFlag` — a real Jetty HTTPS
        server + a forcing `X509ExtendedKeyManager` that presents the cert regardless of the server's CA hints,
        over TLSv1.2 (synchronous client-auth rejection). Untrusted client cert **rejected** when the WEB policy
        is secure, **accepted** when insecure; trusted cert accepted in both. Existing web TLS-auth suites pass
        unchanged (none relied on lax behaviour — there was none in the new path): `AdminApiTlsAuthTest` (5/6;
        see pre-existing failure below), `BrokerAdminClientTlsAuthTest`, `AdminApiKeyStoreTlsAuthTest`,
        `AuthedAdminProxyHandlerTest` (+`NewTlsPath`).
    - **F3 `[fix][proxy]` — `AdminProxyHandler` reloading broker-client.** `newHttpClient()` snapshotted a
      one-shot `SSLContext` that never rotated. Added `JettyTlsFactory.createReloadingClientFactory` +
      `ReloadableClientTls` (mirrors the server `createReloadingServerFactory`: subscribe to `SSLContext`,
      `setSslContext` before start, `reload(...)` on later deliveries); `AdminProxyHandler` now holds the
      subscription and disposes it in `destroy()` (replacing any prior on repeated `newHttpClient()`). The
      `!tlsHostnameVerificationEnabled` → `setEndpointIdentificationAlgorithm(null)` is preserved.
      - **Gate:** new `JettyTlsFactoryTest.reloadingClientFactoryPresentsRotatedClientCertOnNewConnections` —
        rotate the client identity (admin→user1 cert, both clientAuth-EKU) while the factory stays alive; a
        client-auth-requiring server observes the rotated serial on new connections. `AuthedAdminProxyHandlerTest`
        (+`NewTlsPath`) exercise the real proxy→broker admin path and pass.
    - **Verify:** `spotlessApply` + `checkstyle` clean on all touched modules; full builds
      `:pulsar-common` `:pulsar-broker-common` `:pulsar-client-original` `:pulsar-client-admin-original`
      `:pulsar-client-v5` (one **pre-existing flake** dismissed: `AsyncHttpConnectorTest.testShouldStopRetries…`,
      a 500 ms no-TLS retry-timeout test — fails under concurrent build load, passes in isolation, as noted in
      entry 29); targeted gates per finding + `TlsFactoryProducerConsumerTest`, `V5TlsProducerConsumerTest`,
      `ReplicatorTlsFactoryTest`, `ProxyTlsTest`, `ProxyTlsFactoryTest`, `BrokerAdminClientTlsFactoryTest` all
      green.
    - **Two PRE-EXISTING failures found (NOT caused by this fixup; reproduce on clean `bdac317` HEAD via
      `git stash`), flagged for the orchestrator:** (a) `ProxyKeyStoreTlsTransportTest.testProducer` — the
      proxy's lookup `ConnectionPool` never resolves a client TLS factory for its broker-facing config
      (`ProxyConnection.createClientConfiguration` sets `tls*` fields but no factory; nothing plays the broker's
      `maybeApplyBrokerClientTlsFactory` role for the proxy), so `tlsEnabledWithBroker=true` produce/consume
      fails with a null-factory NPE in the endpoint `createInstance`. This is why F1's proxy coverage asserts the
      BROKER_CLIENT synthesized context rather than end-to-end produce-through-proxy. (b)
      `AdminApiTlsAuthTest.testCertRefreshForPulsarAdmin` — the admin `AuthenticationTls` cert-refresh does not
      take effect on the new SPI path (auth-fold material rotation not observed). Both are stage-4c gaps
      orthogonal to F1/F2/F3 and should be tracked separately.

31. **Post-removal CI regression fixes (four stage-4c regressions)** — the last build work of the mission, on
    `lh-pip-478-impl-v2` (local commits only). Trailer `Assisted-by: Claude Code (Opus 4.8)`. Each regression
    was re-reproduced on the current HEAD (`cb7237ed50a`, i.e. **after** the entry-30 F1 fixup) before fixing;
    the F1 acquisition refactor did move regression #2's surface, exactly as the work order flagged.
    - **P0 `[fix][proxy]` — proxy binary-lookup TLS null factory (deterministic NPE).** Since 4c deleted the
      lazy per-host `PulsarSslFactory` fallback, `ProxyConnection.createClientConfiguration` built a
      `tlsEnabledWithBroker` client config with the `tls*` fields set but **no** `PulsarTlsFactory`, and passed
      it straight to `new ConnectionPool(...)`; the client `PulsarChannelInitializer` then NPE'd on a null
      `CLIENT_DEFAULT` factory. Fix: `ProxyService` builds one shared `CLIENT_DEFAULT`-serving factory at
      startup via `ClientTlsFactorySupport.resolveClientTlsFactory(<representative config>, statsExecutor,
      statsExecutor, otel)` — off the event loop, from the same broker-client `tls*` material — exposed via
      `getLookupClientTlsFactory()` and closed with the service; `createClientConfiguration` (now also a static
      form, reused to derive the representative config) stashes it on every per-connection config. The existing
      `brokerClientTlsFactory` serves `BROKER_CLIENT` (DirectProxyHandler) and cannot serve the lookup pool's
      `CLIENT_DEFAULT` request, so this is a deliberate second factory. Gate:
      `ProxyAuthenticatedProducerConsumerTest.testTlsSyncProducerAndConsumer` (1/1); the entry-30
      `ProxyKeyStoreTlsTransportTest.testProducer` keystore sibling is covered by the same fix (`:pulsar-proxy`
      full build). The `ClientTlsTest` integration scenario is on the same path — CI validates it.
    - **P1 `[fix][client]` — broker-internal client presented a server-EKU cert (auth-cert-wins broken).** The
      queue framed this as native hostname-verification; the reproduced mechanism is different (the F1 fixup had
      shifted it). `AuthenticationTlsHostnameVerificationTest.testTlsSyncProducerAndConsumerCorrectBrokerHost`
      failed with "Connection already closed" because the broker's internal system-topic (`__change_events`)
      client — configured (by the base `MockedPulsarServiceBaseTest`) with both `brokerClientCertificateFilePath`
      (broker.cert, **serverAuth-EKU only**) and an `AuthenticationTls` plugin (admin.cert, clientAuth-EKU) —
      presented the server-EKU cert for TLS client auth and was rejected ("Extended key usage does not permit
      use for TLS client authentication"), failing topic-policy load and tearing the app subscribe down (the app
      client itself, admin.cert via auth with no `tls*` files, connected fine). Root cause:
      `ClientTlsFactorySupport.clientDefaultAuthMaterialSuppliers` skipped the auth fold whenever any `tls*` key
      material was configured, inverting the removed-PIP-337 "auth-cert-wins" precedence. That gate existed only
      to keep `TlsProducerConsumerTest.testTlsWithFakeAuthentication` from calling `getAuthData()` on a **non-TLS**
      fake plugin. Fix: gate on the plugin **type**, not the mere presence of `tls*` files — a genuine TLS-auth
      plugin (`AuthenticationTls`/`AuthenticationKeyStoreTls`) always overrides the `tls*` files
      (auth-cert-wins); a non-TLS plugin is still not consulted when `tls*` files supply the identity, so its
      `getAuthData()` stays untouched. Gate: full `AuthenticationTlsHostnameVerificationTest` (4/4 — negative
      sibling still fails-as-required) and `testTlsWithFakeAuthentication` still green.
    - **P1/P2 `[fix][client]` — admin `AuthenticationTls` cert-rotation not picked up.**
      `AdminApiTlsAuthTest.testCertRefreshForPulsarAdmin` (deterministic, not a flake — failed in isolation and
      under the class's TestNG retry) swaps the admin key file mid-test. The 1 s material poll re-reads fine, but
      during the test's brief delete window `AuthenticationTls.getAuthData()` throws `NoSuchFileException`, and
      the `clientDefaultAuthMaterialSuppliers` supplier **swallowed it to `null`** — so `AuthProvidedMaterialSource`
      treated it as "no auth material", overlaid the (empty, for the admin) base material, and **delivered a
      no-client-cert context** instead of keeping the last-good one. The admin then opened a connection that
      handshook presenting no cert (broker `401 Authentication required`); being non-5xx it was kept alive and
      **reused for every retry** (same client port in the logs), so the later restored cert never took effect.
      Fix: for a TLS-auth plugin register the supplier **directly** (no `null` swallow) so a transient read
      failure propagates and `AuthProvidedMaterialSource` keeps the last-good cert; the mismatched-key connection
      then fails the handshake (never pooled) and, once the good key is delivered, a fresh connection
      authenticates. The `null`-swallow is retained only for non-TLS plugins (token/OAuth2), where a lookup
      failure legitimately means "nothing to fold, don't fail the build". Gate: full `AdminApiTlsAuthTest`
      (20/20).
    - **Log-honesty note (the missed ninth call-site).** The 4c call-site sweep for the deleted lazy-factory
      fallback missed `ProxyConnection.createClientConfiguration`: it hand-rolls a `ClientConfigurationData`
      outside `PulsarClientImpl` (where the client factory is normally resolved via `setupClientTlsFactory`), so
      it was not on the `maybeApplyBrokerClientTlsFactory` / `setupClientTlsFactory` entry-point list. Entry 30
      had in fact already surfaced this exact gap as a "pre-existing failure"
      (`ProxyKeyStoreTlsTransportTest.testProducer`), now fixed. Lesson for future sweeps: a sweep keyed on the
      framework's TLS-factory entry points misses components that build a `ClientConfigurationData` by hand.
    - **Verify:** `spotlessApply` + `checkstyleMain` clean (`pulsar-client-original`, `pulsar-proxy`). Gates:
      `ProxyAuthenticatedProducerConsumerTest.testTlsSyncProducerAndConsumer` (1/1),
      `AuthenticationTlsHostnameVerificationTest` (4/4), `AdminApiTlsAuthTest` (20/20). Cross-gates all green:
      `TlsProducerConsumerTest` (10), `TlsFactoryProducerConsumerTest` (2), `ReplicatorTlsFactoryTest` (2),
      `V5TlsProducerConsumerTest` (2), `ClientCnxAsyncAuthTest` (7), `ProxyTlsTest` (2),
      `AuthedAdminProxyHandlerTest` + `NewTlsPath` (2 + 2). Full builds: `:pulsar-broker:compileTestJava` green,
      `:pulsar-client-original:build` green, `:pulsar-proxy:build` green **except** one **local-environment**
      failure — `ProxyEnableHAProxyProtocolTest.setup` got `HTTP 407 Proxy Authentication Required` because the
      admin `AsyncHttpConnector` honours JVM proxy properties (`setUseProxyProperties(true)`) and this dev machine
      has an authenticating `HTTP_PROXY` set; it is a plaintext (non-TLS) test that never touches the changed
      code path, and it passes when re-run with the proxy env vars cleared. `ProxyKeyStoreTlsTransportTest` (the
      keystore P0 sibling entry 30 flagged) passes (1/1) inside the full `:pulsar-proxy` build. Nothing left
      before the branch is CI-ready.

32. **Post-removal CI regression fix (fifth stage-4c regression) — standalone OAuth2 broker-client.** CI run
    28703128714 / job 85124702262 (fork PR #231, branch `lh-pip-478-impl-v2`) failed
    `ProxyTlsWithAuthTest > setup`. Trailer `Assisted-by: Claude Code (Opus 4.8)`. Local commits only.
    - **Predecessor crash / recovery.** A first agent diagnosed this and built the fix below but crashed
      mid-work (API error), leaving the change **uncommitted** in the worktree (`FlowBase.java` modified +
      new `StandaloneOAuth2HttpClientFactory` and its test + a draft of this entry). A second agent stashed
      the partial work (`stash@{0}` = `ad6cf9dc5ec`), **re-reproduced the failure on the clean baseline**
      (confirming the CI stack), independently reviewed the design against the requirements (kept it — see
      below), restored the work, and re-ran every gate itself (the predecessor's speculative "Verify" claims
      were replaced with the second agent's actually-observed results).
    - **CI stack (setup FAILED):** `IllegalStateException: OAuth2 requires the authentication to be initialized
      by a PulsarClient/PulsarAdmin; standalone use is unsupported since PIP-478` at
      `FlowBase.getHttpClient(FlowBase.java:111)` ← `createMetadataResolver(:190)` ← `initialize(:182)` ←
      `ClientCredentialsFlow.initialize(:146)` ← `AuthenticationOAuth2.start(:251)` ←
      `ProxyTlsWithAuthTest.setup(:82)`. The line numbers match **stage 4c (`86a59df845f`)** exactly.
    - **Root cause = the stage-4c R4 guard, not R1/F3/F1 (the queued suspects were a red herring — the failure
      is in `proxyClientAuthentication.start()` at test line 82, *before* `ProxyService` is even
      constructed).** Stage 4c removed the private OAuth2 `AsyncHttpClient` and made
      `FlowBase.getHttpClient()` **throw** whenever no framework HTTP client factory was bound (R4:
      "genuinely-unbound path throws"). That rests on the assumption that OAuth2 only ever runs inside a
      `PulsarClient`/`PulsarAdmin` — **false**: the proxy (`ProxyServiceStarter` L280-284, production),
      the broker broker-client and the CLI all create `AuthenticationOAuth2` via
      `AuthenticationFactory.create(plugin, params).start()` **standalone** and never bind a factory. So this
      is a **real production regression** in the proxy's OAuth2 broker-client auth (and any direct
      `Authentication.start()` user), breaking the `Authentication` public-API contract that has supported
      standalone `start()` + `getAuthData()` for years. `ProxyTlsWithAuthTest` was green at stage 3c (which
      still had the private-client fallback) and was not re-run after 4c, so the regression slipped the
      cross-gate. (Athenz was checked and is unaffected: it builds its own `ZTSClient` off its own
      `SSLContext`, not the framework factory, so it has no sibling guard.)
    - **Fix `[fix][client]` — restore standalone support on the new SPI, do not weaken the test.** New
      package-private `StandaloneOAuth2HttpClientFactory` (pulsar-client `…auth.oauth2`) wraps a
      `FrameworkHttpClientFactory` with null event-loop/timer/resolver suppliers (HTTP-only, AsyncHttpClient
      self-provisions — mirroring `PulsarAdminImpl.bindAuthenticationServices`) and, **only** when the flow
      carries its own IdP TLS material, a standalone `FileBasedTlsFactory` serving it on `CLIENT_OAUTH2` with
      rotation (owns a single-thread scheduler, mirroring `AsyncHttpConnector.resolveNewTlsFactory`); with no
      IdP material the framework client uses the platform default trust store (v4 behaviour for a
      system-trusted IdP). `FlowBase.getHttpClient()` no longer throws — a new `resolveHttpClientFactory()`
      returns the bound framework factory when present, else lazily builds and owns the standalone one (closed
      in `FlowBase.close()`). This restores exactly the pre-4c standalone capability (incl. issue #24944 IdP
      trust/mTLS) through the new SPI, so the proxy/broker/CLI paths work again without any change to their
      call sites or the test.
    - **New gate:** `StandaloneOAuth2HttpClientFactoryTest` (pulsar-client) — a WireMock IdP (both HTTP and
      HTTPS/test-CA): (a) plain-HTTP IdP with no IdP TLS material → 200; (b) test-CA HTTPS IdP with the flow's
      `trustCertsFilePath` → 200 (standalone IdP trust fold); (c) test-CA HTTPS IdP with **no** trust material
      → handshake rejected (system default trust), proving the flow's material carries the CA. 3/3. The prior
      oauth2 unit suite (`AuthenticationOAuth2Test` etc.) mocks `Flow`, so it never exercised the real
      `getHttpClient` — which is why the 4c guard passed the unit suite.
    - **Verify (second agent, all local, worktree `async-auth-interface`, per-class JUnit XML 0 failures / 0
      errors unless noted).** Baseline first: with the partial work stashed, `ProxyTlsWithAuthTest` reproduced
      the exact CI `IllegalStateException` at `FlowBase.java:111` (root cause confirmed), then the work was
      restored. `spotlessApply` made no changes (files already conformed); `checkstyleMain` + `checkstyleTest`
      clean on `:pulsar-client-original`.
      - **Target:** `ProxyTlsWithAuthTest` **1/1** (was the failing gate) — green in isolation *and* inside the
        full `:pulsar-proxy:build`.
      - **New gate:** `StandaloneOAuth2HttpClientFactoryTest` **3/3**.
      - **Full oauth2 client unit suite:** `AuthenticationOAuth2Test` (29), `AuthenticationFactoryOAuth2Test`
        (5), `AuthenticationOAuth2StandardAuthzServerTest` (1), `TlsClientAuthFlowTest` (1),
        `TokenClientTest` (4); `OAuth2IdpTlsFoldTest` (4); `OAuth2IdpTlsFrameworkClientTest` (2);
        `CredentialOffloadTest` (3).
      - **R4 gate + untouched-plugin gate (`:pulsar-broker`):** `AdminOnlyOAuth2AuthTest` **1/1**;
        `TokenOauth2AuthenticatedProducerConsumerTest` **2/2** (unmodified).
      - **Proxy TLS gate suites:** `ProxyTlsTest` (2), `ProxyMutualTlsTest` (3), `AuthedAdminProxyHandlerTest`
        (2), `AuthedAdminProxyHandlerNewTlsPathTest` (2), `ProxyAuthenticatedProducerConsumerTest` (1).
      - **Full builds:** `:pulsar-client-original:build` green; `:pulsar-proxy:build` green (56 test classes,
        all 0 failures / 0 errors). Both were re-run once each after a **transient local Gradle
        test-executor infra flake** — `:pulsar-client-original` first hit a `NoClassDefFoundError:
        ClientBuilderImpl` in `BuildersTest.clientBuilderTest` (unrelated to oauth2; passed in isolation and
        on the clean re-run), and `:pulsar-proxy` first died with `NoSuchFileException:
        …/test-results/test/binary/in-progress-results-generic.bin` **after** `ProxyTlsWithAuthTest` had
        already passed (no OOM/`hs_err`, 64% RAM free) — both cleared on a plain re-run. The stale-`compileJava`
        build-cache hazard the predecessor's draft worried about **did not reproduce**: the fixed
        `pulsar-client-original` recompiled on its changed source and `ProxyTlsWithAuthTest` passed inside the
        full proxy build.

33. **`SSLParameters` companion class (user-approved refinement) — implements the well-known-table row +
    merge-order paragraph that landed in `pip/pip-478.md` at HEAD (`888f80e4a99`).** New **optional** well-known
    class `javax.net.ssl.SSLParameters`: the framework asks the factory for it **only on the `SSLContext`-
    fallback synthesis path** (a factory natively supplying Netty/Jetty objects bakes policy there and is never
    asked), carrying the engine-level baseline a bare `SSLContext` cannot express (protocols, cipher suites,
    algorithm constraints, application protocols, endpoint identification, server client-auth mode). No SPI
    signature change — it rides the generic `createInstance(purpose, Class<T>)` form. Trailer
    `Assisted-by: Claude Code (Opus 4.8)`. Local commit only.
    - **`pulsar-common` `.tls.impl` (acquisition + synthesis).** `TlsContextAcquisition` now, on the synthesis
      fallback, additionally requests `createInstance(purpose, SSLParameters.class)` — in the **same form** as
      the `SSLContext` (one-shot / endpoint-carrying), lazily so it is not requested when the JDK context is
      unsupported; on the **subscribing** form it re-requests the companion with each `SSLContext` delivery so
      engine policy rotates with material (the request is synchronous inside the reload callback, which the SPI
      runs off any event loop — documented as a factory obligation). `TlsContexts` gained
      `synthesizeNettyServerFromJdk(sslContext, requireTrustedClientCert, factoryBaseline)` and a 3-arg
      `synthesizeNettyClientFromJdk(..., factoryBaseline)`; the merge is: factory members form the baseline
      (non-null only) → `endpointIdentificationAlgorithm` factory-wins-else-consumer-`"HTTPS"` (client) → SNI
      `serverNames` **never** overlaid (per-connection wins) → server `needClientAuth`/`wantClientAuth`
      authoritative when the companion is supplied. The generalized `HostnameVerifyingSslContext` was
      **replaced** by `SynthesizedEngineSslContext`, which applies the composed overlay per `newEngine(...)` via
      the read-`getSSLParameters()` / overlay-non-null / write-back round-trip (SNI preserved). The **defensive
      copy** of the mutable companion is `TlsContexts.copyBaselineMembers` (one snapshot per acquisition, fully
      disconnected from the factory's object). `FileBasedTlsFactory` already returns `empty()` for
      `SSLParameters` (its `isSupported` allows only `SslContext`/`SSLContext`) — verified and asserted; its
      javadoc now states it bakes policy natively.
    - **`pulsar-broker-common` `JettyTlsFactory`.** The synthesized server/client paths now one-shot-request the
      companion and map its non-null members onto the Jetty setters: `setIncludeProtocols` /
      `setIncludeCipherSuites`, and (server, rule 4, **authoritative**) `setNeedClientAuth` / `setWantClientAuth`
      (need wins; neither ⇒ none). One-shot because a synthesized `SslContextFactory` applies
      protocols/ciphers/client-auth once at build time (a bare `reload(...)` cannot change them).
    - **Tests.** `SslParametersSynthesisTest` (pulsar-common, new, 9): (a) protocol restriction on the
      synthesized client engine + live in-memory handshake (matching TLSv1.2 succeeds, TLSv1.3-vs-TLSv1.2
      fails); (b) factory `endpointIdentificationAlgorithm` wins, consumer-flag `"HTTPS"` fallback when the
      companion leaves it null, and no-companion behavior unchanged; (c) server `needClientAuth` from the
      companion enforced at a live handshake (trusted-cert client passes, no-cert client rejected) + the
      inverse (companion NONE overrides a consumer REQUIRE); (f) mutation-after-supply does not affect an
      already-acquired context; plus two end-to-end `TlsContextAcquisition` cases proving the companion is
      requested and threaded (one-shot and subscribing). `FileBasedTlsFactoryTest` +1 (e:
      `returnsEmptyForSslParametersCompanion`, 28 total). `JettyTlsFactoryTest` +2 (d: server maps
      protocols/ciphers/needClientAuth, client maps protocols; 5 total).
    - **Verify (all local, worktree `async-auth-interface`).** `spotlessApply` clean. Full
      `:pulsar-common:build` **green**, `:pulsar-broker-common:build` **green**. Targeted:
      `SslParametersSynthesisTest` **9/9**, `FileBasedTlsFactoryTest` **28/28**, `JettyTlsFactoryTest` **5/5**.
      Proxy: `SslContextFallbackSynthesisTest` **3/3**, `ProxyTlsTest` **2/2**,
      `AuthedAdminProxyHandlerNewTlsPathTest` **2/2**. Shared acquisition code touched → client gates:
      `:pulsar-client-original:compileJava` green, `V5TlsProducerConsumerTest` **2/2** (pulsar-broker).
    - **Contract note (Jetty scope).** Per the work order the Jetty mapping is scoped to
      protocols/ciphers/client-auth; the finer companion members (`algorithmConstraints`,
      `applicationProtocols`, `endpointIdentificationAlgorithm`) are applied only on the Netty synthesis path
      (`SynthesizedEngineSslContext`), since Jetty's `SslContextFactory` has no direct include-setter for them
      and the PIP merge-order paragraph is written against the Netty `SSLContext`-fallback synthesis. Not a
      silent deviation — it follows the work order's explicit Jetty setter list.

34. **DEFINITIVE CI GREEN — 41/41 pass, 0 failures** (PR lhotari/pulsar#231 @ `f9a0bff86ea`,
    2026-07-04). The complete implementation — all four stages, PIP-337 removed, the new TLS
    SPI as the default everywhere, all review-driven hardening, the R4 standalone-OAuth2
    restoration, and the SSLParameters companion — passes the full Pulsar CI matrix. The
    implementation phase of PIP-478 is complete.

    Remaining before anything apache-facing (deliberately deferred): rebase onto current
    master (the branch sits on the master it was designed against); a fresh full-matrix CI
    after the rebase; human review of the PIP + branch by the author. Known out-of-scope
    follow-ons recorded in entries above: websocket OTel root, SSLContextValidatorEngine
    orphan cleanup.

35. **GATE-critical TLS-rotation memory-safety + lifecycle hardening (8-model review, FIX-1)**
    (2026-07-04, local commits on `lh-pip-478-impl-v2`, no push). Four commits address the
    review's GATE and lifecycle findings; each closes the listed findings.

    - **Commit A — `[fix][misc] PIP-478 GATE: fix OpenSSL TLS-rotation use-after-free (F1+F2)`.**
      The Netty **OpenSSL** engine frees the native `SSL_CTX` when a context's refcount hits 0;
      CI runs the JDK provider where refcount ops are no-ops, so this regime was UNTESTED.
      *F1*: `FileBasedTlsFactory.Subscription.deliver` released the superseded context to
      refcount 0 on the poll thread the instant the new one was published, while subscribing
      consumers hold a bare volatile borrow and call `newHandler`/`newEngine` off another thread
      with no pin. Fixed **both sides**: (1) **deferred release** — the subscription keeps the
      just-superseded instance one extra generation (releases the N-1th on the N+1th delivery);
      (2) **per-use pinning** — new `TlsContextAcquisition.withPinnedContext(source, build)` reads
      the borrow, retains it across the build, releases after (balanced pin, never disturbs
      factory ownership), re-reading on `IllegalReferenceCountException`. Adopted at all six
      subscribing consumers: broker `PulsarChannelInitializer`, proxy `ServiceChannelInitializer`,
      `DirectProxyHandler` (also narrowed — the context is read at connect time inside
      `initChannel` instead of captured before the async connect), `HttpClient`,
      `AsyncHttpConnector`, `FrameworkHttpClientFactory`. *F2*: `ProxyService.close` disposed the
      TLS factories BEFORE quiescing the worker/extension event loops → an in-flight broker-connect
      could run `newHandler` on a freed context; reordered so TLS disposal happens only after the
      loops are shut down (mirrors the broker). **Test**: OpenSSL-provider (`SslProvider.OPENSSL`)
      rotation tests forcing the release/use interleaving while a consumer holds a borrow, plus a
      `withPinnedContext` re-read unit test — the regime CI never exercises. Native OpenSSL is
      present in this env, so all three run (0 skipped). Closes **F1, F2**.
    - **Commit B — `[fix][misc] keep-last-good hardening for TLS reload (L1+F4+L3/S-F3)`.**
      *L1*: `RegisteredSource` gained a `pendingRedeliver` flag so a rotation whose rebuild failed
      for a subscriber retries on the next poll even when the source reports changed=false (no
      permanent wedge). *F4*: `loadInitialInstance` serves the last-good cached context (new
      `cachedInstance`) on a load failure — extending keep-last-good to the one-shot / initial
      acquisitions (was subscription-only), failing only when no last-good exists. *L3/S-F3*:
      `ClientTlsFactorySupport.brokerClientTlsFactory` registers the broker-client TLS fold via
      `shouldFoldBrokerClientAuthTls` = `isTlsAuthPlugin` (match built-in TLS-auth plugins by TYPE,
      robust to a transient read failure) OR the material probe (keeps custom-plugin support).
      **Tests**: one-shot keep-last-good, and fail-fast when no last-good. Closes **L1, F4, L3, S-F3**.
    - **Commit C — `[fix][client] bound pooled TLS rotation on lookup/admin HTTP clients (L2/T3/H4)`.**
      Only `FrameworkHttpClientFactory` set an AHC `connectionTtl` (5 min); `HttpClient` (HTTP
      lookup) and `AsyncHttpConnector` (admin) set none, so a pooled HTTPS connection kept
      pre-rotation material indefinitely. Centralized in
      `TlsContextAcquisition.httpTlsRotationConnectionTtlMillis()` (default 5 min, injectable via
      the `pulsar.tls.http.connectionTtlMillis` system property); both HTTP clients now set it on
      the rotating-factory path; all three read the shared helper. **Test**:
      `AsyncHttpConnectorTest.tlsFactoryPathBoundsPooledConnectionTtl` asserts the https path
      bounds the pooled TTL to the injected value. Closes **L2, T3, H4**.
    - **Commit D — `[fix][misc] TLS/auth lifecycle hardening (F3+F5+F7+F8+F9 + S-F1)`.**
      *F3* `V5AuthContexts.supplyBlocking` catches a synchronous `RejectedExecutionException` from
      a saturated bounded auth pool → `failedFuture` (never sync-throws on the event loop). *F5*
      `PulsarClientImpl.bindAuthenticationServices` closes the previous `FrameworkHttpClientFactory`
      before overwriting (updateAuthentication rebind leaked one per AutoClusterFailover swap) and
      gates the rebind against Closing/Closed. *F7* `FlowBase.close` is now `synchronized` (pairs
      with the synchronized lazy `getHttpClient`); `StandaloneOAuth2HttpClientFactory` uses daemon
      IdP-TLS threads and its ctor releases the executor/factory on a build failure. *F8*
      `TlsContextAcquisition` subscribing synthesis pre-fetches the `SSLParameters` companion ONCE
      off the delivery thread instead of `.join()`ing it inside the reload callback (that
      self-deadlocked when a custom factory dispatched creation to the same single-thread executor
      running the callback). *F9* `FileBasedTlsFactory` rejects createInstance after close and
      cancels a prior poll on a second initialize(); the client `PulsarChannelInitializer` disposes
      the one-shot handle when newHandler/pipeline wiring throws; ctor-throw cleanup for broker
      `PulsarChannelInitializer` and admin `AsyncHttpConnector` (the latter leaked a NON-daemon
      rotation executor). *S-F1* `TlsContexts` logs a one-time WARN (deduped per policy) when an
      insecure trust-all policy is applied at context build (covers the WEB/Jetty path too, whose
      SSLContext is built via `TlsContexts.buildJdkContext`). Closes **F3, F5, F7, F8, F9** (factory
      closed-check + initialize-twice + client handle-dispose + broker/admin ctor-throw), **S-F1**.

    - **Verify (all local, worktree `async-auth-interface`).** `spotlessApply` clean; full-compile
      green for `:pulsar-common :pulsar-broker-common :pulsar-client-original
      :pulsar-client-admin-original :pulsar-proxy` (main + test). Gate suites, all **0 failures /
      0 errors**: `FileBasedTlsFactoryTest` **34/34** (incl. the new OpenSSL + pin tests),
      `TlsMaterialSourceTest` **4/4**, `SslParametersSynthesisTest` **9/9**, `JettyTlsFactoryTest`
      green, `V5TlsProducerConsumerTest` **2/2**, `TlsProducerConsumerTest` **10/12 (2 pre-existing
      skips)**, `ProxyTlsTest` **2/2**, `ClientCnxAsyncAuthTest` **7/7** (carve-out untouched and
      green), `ReplicatorTlsFactoryTest` **2/2**, `AuthedAdminProxyHandlerNewTlsPathTest` **2/2**,
      `CredentialOffloadTest` **3/3**, `StandaloneOAuth2HttpClientFactoryTest` **3/3**,
      `FrameworkHttpClientFactoryTest` **5/5**, `AsyncHttpConnectorTest` **9/9** (incl. the new
      TTL-bound test). F1 discipline chosen: **deferred-release AND per-use pinning** (both sides).

    - **Deferred (reported, not done in this pass):**
      - **S-F2** (synthesis HV default TRUE for CLIENT): the change contradicts the PIP's documented
        merge rule 2 (`endpointIdentificationAlgorithm` — factory wins else the *consumer's* HV flag
        applies "HTTPS") and the locked-in `SslParametersSynthesisTest` assertions. It is a
        documented-contract change best made together with the PIP merge-rule amendment in the
        doc/PIP pass (FIX-5), not silently here.
      - **F8 "don't hold the RegisteredSource monitor during callbacks"**: the `.join()` self-deadlock
        (the concrete bug) is fixed; releasing the monitor around callback invocation is a
        `poll()`/`deliver()` refactor with real concurrency risk for a latent tier-3 concern
        (callbacks in practice only set a volatile), deferred as backlog.
      - **F9 ctor-throw for `WebService` / websocket `ProxyServer` / functions `WorkerServer`**: these
        are server-startup ctors where a failure aborts the process (leaked scheduler is moot);
        the high-value non-daemon-executor leaks (`AsyncHttpConnector`, `StandaloneOAuth2`) and the
        broker binary initializer are fixed. The three web/worker ctors follow the same
        try/finally-cleanup pattern if picked up later.

36. **TlsPolicy v4-parity + TLS defaults (user-flagged P1 + review B3/B1/A-L1, FIX-2)**
    (2026-07-04, local commits on `lh-pip-478-impl-v2`, no push). Four commits; each closes the
    listed finding. Re-based against the FIX-1 HEAD (`a3f0f5552d2`), which had reshaped
    `FileBasedTlsFactory`/`TlsMaterialSource`/`TlsContexts`.

    - **Commit A — `[fix][misc] GATE: TlsPolicy separate key/trust store types (P1)`.** The only
      real v4-parity gap. `TlsPolicy` carried one `storeType()` applied to the keystore, the key
      cert chain, AND the truststore; v4 has separate `tlsKeyStoreType` / `tlsTrustStoreType`
      (both default JKS). A mixed setup (PKCS12 keystore + JKS truststore, or a BCFKS/FIPS mix)
      loaded the truststore with the wrong type and broke at handshake. Since `TlsPolicy` is
      unreleased there is no compat constraint: `storeType` → `keyStoreType` + `trustStoreType`
      across fields/accessors/builder/`equals`/`hashCode`/`toString` (password masking kept). The
      5-arg `keyStore(...)` factory keeps its single `storeType` arg but now sets BOTH types (the
      common case); split types go through the builder. `TlsMaterialSource` routes the truststore
      load through `trustStoreType()` and the key/chain loads through `keyStoreType()`.
      `ClientTlsFactorySupport.clientDefaultPolicy` drops the `keyStorePath!=null` heuristic and
      maps both conf types directly. Six server compose sites map both types and drop their
      `firstNonBlank` helpers: `DefaultBrokerTlsFactory` (server + brokerClient), `ProxyTlsFactories`
      (server + brokerClient), functions `WorkerServer`, websocket `ProxyServer`.
      `PulsarClientBuilderV5.keyStoreBuilder` now preserves `base.trustStoreType()` when folding an
      `AuthenticationKeyStoreTls` (previously the key store type would clobber it). **Test**:
      mixed-type regression at the `TlsMaterialSource` level (PKCS12 keystore + JKS truststore both
      load; invalid-type discrimination — a bad `trustStoreType`/`keyStoreType` fails the respective
      load — proves each store is parsed with its OWN type, which the pre-fix single type could not)
      and at `clientDefaultPolicy` (`ClientDefaultPolicyStoreTypeTest`: the two types map
      independently, not collapsed). Note: on this JDK `keystore.type.compat=true` masks a
      JKS-as-PKCS12 cross-read, so the regression is made deterministic via the invalid-type path
      rather than relying on a cross-read `IOException`. **Only parity gap found** — the rest of the
      v4 TLS surface already maps (sslProvider stays factory-level, per the earlier P2 DEFER).

    - **Commit B — `[fix][misc] preserve default TLS protocol set {TLSv1.3, TLSv1.2} (B3)`.** The
      removed `DefaultPulsarSslFactory` forced `{TLSv1.3, TLSv1.2}` at engine build when
      `tlsProtocols` was empty; the PIP-478 path only set protocols when the policy configured them,
      so an unconfigured deployment silently deferred to the JVM/provider default on upgrade. Single
      well-named constant `TlsContexts.DEFAULT_ENABLED_PROTOCOLS`, applied when the effective
      protocol list is empty at: the native Netty client+server build (`applyCiphersAndProtocols`),
      the JDK/synthesis overlays (`composeClientOverlay` + `synthesizeNettyServerFromJdk`, which now
      always wrap to carry the pinned protocols), and both Jetty include-protocols paths (server
      `applyServerConfig` + client factory). A factory-supplied `SSLParameters` companion still wins.
      **Test**: native-path default (`FileBasedTlsFactoryTest`), synthesis client/server defaults
      (`SslParametersSynthesisTest`), Jetty server/client defaults (`JettyTlsFactoryTest`). Verified
      the old Jetty web path also used the JVM default (no historical `{1.3,1.2}` force), so this
      makes web consistent with the binary path at ~zero practical risk on modern JDKs (their default
      is already `{1.3,1.2}`).

    - **Commit C — `[fix][broker] per-cluster custom TLS factory fail-loud (B1/H2)`.**
      `BrokerService.getReplicationClient` / `getClusterPulsarAdmin` only LOGGED when a per-cluster
      `ClusterData.brokerClientSslFactoryPlugin` named a CUSTOM PIP-337 factory, then built file TLS
      — so a per-cluster KMS/HSM factory silently downgraded to file-based TLS on 5.0 upgrade
      (wrong-identity or failed replication/admin). Escalates ruling R6:
      `warnIfRemovedClusterSslFactoryPluginConfigured` → `failIfRemovedCustomClusterSslFactoryPlugin
      Configured` throws `IllegalStateException` with an actionable migration message (same
      `TlsFactorySupport.isLegacyCustom` literal-FQCN logic as the broker-level fail-loud sites); a
      blank or removed-default FQCN is not custom and is still tolerated for metadata compat.
      **Test** (`ReplicatorTlsFactoryTest`): a custom value fails both client and admin creation with
      the migration hint; a removed-default FQCN is tolerated and the client builds.

    - **Commit D — `[fix][misc] TlsPolicy.build() validates format consistency (A-L1)`.** `build()`
      silently accepted keystore fields on a PEM policy (and PEM file fields on a keystore policy);
      it now throws `IllegalArgumentException` naming the offending cross-format field (a builder may
      throw synchronously — not a future-returning API). Mixed store TYPES stay valid within KEYSTORE
      format. **Test** (`TlsPolicyValidationTest`): rejection both ways; consistent PEM / keystore /
      flag-only policies still build. Verified the full `org.apache.pulsar.common.tls.impl.*` suite
      builds nothing that the validation rejects.

    - **PIP updated** (in the relevant commits): the `TlsPolicy` listing (`storeType()` →
      `keyStoreType()`/`trustStoreType()`, factory javadoc), the v4-mapping note (separate store
      types preserved), one sentence on the `{TLSv1.3, TLSv1.2}` default in Security Considerations,
      and the removal-impact `ClusterData` row + geo-replication note (custom-factory fail-loud).

    - **Verify (all local, worktree `async-auth-interface`, `dangerouslyDisableSandbox` for
      Gradle wrapper).** `spotlessApply` clean; full compile green (main + test) for `:pulsar-common`
      `:pulsar-common-api` `:pulsar-broker-common` `:pulsar-client-original`
      `:pulsar-client-admin-original` `:pulsar-client-v5` `:pulsar-proxy`
      `:pulsar-functions:pulsar-functions-worker` `:pulsar-websocket` `:pulsar-broker`. Gate suites,
      all **0 failures / 0 errors**: `TlsPolicyValidationTest` **3/3**, `ClientDefaultPolicyStoreType
      Test` **2/2**, `TlsMaterialSourceTest` **7/7** (+3 mixed-type), `FileBasedTlsFactoryTest`
      green (+1 default-protocols), `SslParametersSynthesisTest` **12/12** (+3 default-protocols),
      `JettyTlsFactoryTest` green (+1 default-protocols), `DefaultBrokerTlsFactoryTest` **3/3**,
      `PulsarClientBuilderV5Test` green, `KeyStoreTlsProducerConsumerTestWithAuthTest` **7/7** (proves
      the keystore rename end-to-end), `TlsProducerConsumerTest` **10/10**, `V5TlsProducerConsumerTest`
      **2/2**, `ProxyTlsTest` **2/2**, `ReplicatorTlsTest` **1/1** (legacy path unaffected by the
      protocol default), `ReplicatorTlsFactoryTest` **4** (1 retry-artifact skip, incl. the new
      fail-loud test), `ConfigValidationTest` **7/7**.

    - **Deferred (reported, not done in this pass):** **P2** sslProvider JCE-provider-name delta
      (niche; one-sentence PIP note belongs in the doc/PIP pass FIX-5). No other v4 parity gap found
      beyond `storeType`.

37. **SecurityUtility decomposition + dead TLS-utility removal (user-requested, FIX-6)** — five staged
   commits (`61f3972bd98..bd66cb32687`), each compiling; applies this PIP's anti-kitchen-sink philosophy
   (Motivation #2/#4) to the internal TLS util layer. `SecurityUtility` carried no `@InterfaceStability`
   and had no client-API importer, so it is internal — safe to decompose/remove, not a public-API break.

   - **Commit A — `[feat][misc]` cohesive TLS-util containers.** Three single-concern classes in a new
     package `org.apache.pulsar.common.util.tls` (kept out of the dependency-light `pulsar-common-api` —
     these pull BouncyCastle/Netty — and out of `...common.tls` to avoid a split package with
     pulsar-common-api): `PemReader` (loadCertificatesFromPemFile/Stream + loadPrivateKeyFromPemFile/
     Stream), `JcaProviders` (getProvider/isBCFIPS + BC/Conscrypt constants + the Conscrypt
     hostname-verifier workaround; resolveProvider and the TrustManager processing package-private),
     `JdkSslContexts` (the createSslContext JDK-`SSLContext` assembly, composing PemReader / JcaProviders /
     the existing `KeyStoreHolder`). Purely additive; `SecurityUtility` untouched so the tree compiles.

   - **Commit B — `[improve][misc]` migrate callers.** 18 files across pulsar-client, pulsar-common,
     pulsar-broker-common, pulsar-broker (tests) and the bouncy-castle bc/bcfips loaders moved off
     `SecurityUtility` onto the containers (imports + call sites + two javadoc `{@link}`s):
     `AuthenticationDataTls` → PemReader; `TlsMaterialSource` / `AuthProvidedMaterialSource` → PemReader;
     `JettyTlsFactory` → `JcaProviders.CONSCRYPT_PROVIDER`; the bcloaders → static `JcaProviders.BC` /
     `BC_FIPS`; `TlsContexts` + the common/broker/broker-common TLS test suites → JdkSslContexts / PemReader.

   - **Commit C — `[improve][misc]` delete dead utilities + SecurityUtility.** Each re-verified as 0
     external callers at HEAD: `KeyManagerProxy`(+Test), `TrustManagerProxy`(+Test) (delegate-swap
     rotation proxies, obsoleted by the rebuild-not-mutate model — Appendix B), `SSLContextValidatorEngine`
     + the whole `keystoretls` package (no importers after `KeyStoreSSLContext` went in stage 4c), and
     `SecurityUtility` itself — taking with it the dead `createAutoRefreshSslContextForClient`,
     `createNettySslContextForClient` (all 4 overloads), `createNettySslContextForServer`, and
     `configureSSLHandler` (0 callers; hostname verification is baked into the built context —
     `PulsarChannelInitializer`'s javadoc reworded). **createSslContext decision:** relocated to the public
     focused `JdkSslContexts`, NOT package-private on `TlsContexts` as first sketched, because it is shared
     by TLS tests across four modules (`SslParametersSynthesisTest`, `JettyTlsFactoryTest`,
     `AdminApiTlsAuthTest`, `BrokerServiceLookupTest`, `ProxyPublishConsumeTlsTest`) that build a JDK
     `SSLContext` from PEM material — a single cohesive concern, not the grab-bag; `TlsContexts` delegates
     to it. End-state: no `org.apache.pulsar.common.util.SecurityUtility`.

   - **Commit D — `[improve][misc]` cleaner `TlsContexts` assembly.** With the grab-bag gone, `TlsContexts`
     already composes only the focused primitives (JDK via `JdkSslContexts`, Netty inline via
     `SslContextBuilder`); rewrote the class javadoc to state that end-to-end-ownership contract and drop
     the now-stale "file-path helpers" contrast. No behaviour change; primitives composed, not duplicated.

   - **Commit E — `[improve][pip]` PIP + this log.** The PIP Removal section + compat summary now list the
     `SecurityUtility` decomposition, the obsolete proxies (cross-referencing the Appendix B rotation
     model), and the unused `SSLContextValidatorEngine` (superseding the earlier leave-in-place decision),
     all flagged internal-only / not a compatibility break.

   - **Verify (all local, worktree `async-auth-interface`, `dangerouslyDisableSandbox` for the Gradle
     wrapper).** spotless clean; per-module compileJava+compileTestJava+checkstyle GREEN for `:pulsar-common`
     `:pulsar-broker-common` `:pulsar-client-original` `:pulsar-broker` `:bouncy-castle:bouncy-castle-bc`
     `:bouncy-castle:bcfips`; no residual reference (java or non-java) to any deleted symbol. Gate suites,
     all **0 failures / 0 errors**: `FileBasedTlsFactoryTest` **35/35**, `SslParametersSynthesisTest`
     **12/12**, `TlsMaterialSourceTest` **7/7**, `AuthProvidedMaterialFoldTest` **4/4**, `TlsReloadMetricsTest`
     **4/4** (pulsar-common); `JettyTlsFactoryTest` **6/6**, `DefaultBrokerTlsFactoryTest` **3/3**
     (broker-common); `AdminApiTlsAuthTest` **20/20** (exercises JdkSslContexts+PemReader e2e),
     `TlsProducerConsumerTest` **10/10** (AuthenticationDataTls→PemReader e2e). `netty/SslContextTest` was
     already absent (removed earlier) — no migration needed there. Heavier broker/proxy TLS e2e beyond
     those two are compile-verified (all migrated test files compile + checkstyle) but not each executed —
     this is a verbatim-move refactor with no behaviour change.

38. **Auth-SPI contract fixes + the two API-freeze blockers (multi-model review, FIX-3)**
    (2026-07-04, local commits on `lh-pip-478-impl-v2`, no push). Five commits, each closing one review
    finding group; re-located against HEAD after FIX-1/2/6 landed (FIX-6 had removed `SecurityUtility`, so
    auth code now reaches PEM material through `PemReader`).

    - **Commit A — `[fix][client] GATE: load v5-native auth plugins by class name (A-H1/G-IS2)`.** The
      flagship "implement a v5 `Authentication`, deploy it by class name" was broken: every reflective
      load blind-cast to the v4 `Authentication`, so a v5-only class threw `ClassCastException`. New
      `V5AuthenticationLoader` detects a v5-native class
      (`org.apache.pulsar.client.api.v5.auth.Authentication.isAssignableFrom`), instantiates it no-arg and
      calls `configure(parsedParams)` (JSON or `key:val`, matching `AuthenticationUtil`); a legacy v4
      class keeps the existing wrap path (raw-string handling preserved for
      `EncodedAuthenticationParameterSupport`). `PulsarClientProviderV5.createAuthentication` (both
      overloads) and `PulsarClientBuilderV5.authentication(className, params)` route through it — the
      builder string path also now eagerly builds the plugin (it previously stashed only the class name
      and silently yielded `AuthenticationDisabled`) while keeping the serializable `authPluginClassName +
      authParams` form. **Test** `V5AuthenticationLoaderTest` (5): a fake v5-native
      `SinglePassAuthentication` is configured and drives the binary transport; v4 still bridges; blank →
      disabled.

    - **Commit B — `[fix][client] unify the REFRESH-sentinel contract on fresh-exchange (A-H2)`.** The
      contract was stated three inconsistent ways; the implementation does fresh-exchange-per-REFRESH
      (`ClientCnx` opens a new exchange and re-produces via `getAuthDataAsync`), verified in code. Blessed
      that everywhere — **doc/javadoc only, no code change**: `pip-478.md` binary routing rule 2 + the
      SASL-spanning paragraph + the `AsyncAuthenticationDriver` exchange-scoping paragraph; the
      `AsyncAuthenticationDriver` class doc + `authenticateAsync` (renamed param `challengeOrRefresh` →
      `challenge`, now documented to never receive the sentinel); `AuthenticationCallContext` slot lifetime
      (one exchange; a REFRESH begins a new one).

    - **Commit C — `[fix][client] v5-builder keeps async offload for credential-I/O v4 plugins (G-IS6)`.**
      `PulsarClientBuilderV5` unwrapped EVERY bridged v4 back to the raw engine, so
      `LegacyV4AuthenticationAdapter`'s blocking-executor offload never engaged and a third-party v4
      credential plugin blocked the Netty loop. **Unwrap-selectivity rule implemented:** the decision moved
      to `build()` (`applyAuthentication`), where the bridged plugin is probed on the application thread
      (off the event loop, the same place the stage-3c TLS fold already probes). A plugin with **no
      credential I/O** — `AuthenticationTls`/`AuthenticationKeyStoreTls` (TLS material folded, empty binary
      payload) and `AuthenticationDisabled`, type-based; plus a generic plugin whose probed
      `getAuthData()` reports neither `hasDataFromCommand` nor `hasDataForHttp` — runs raw; anything
      reporting command/HTTP credential stays wrapped in `V5ToV4AuthenticationAdapter` so it off-loads. The
      probe (not pure type-matching) is required so a TLS-only third-party plugin with a non-`"tls"` method
      name still folds + runs raw instead of failing at `start()` — this keeps the two
      `PulsarClientBuilderV5Test` generic-fold gates green. `unwrapV4` de-staled to the pure inverse of
      `wrap`. **Test** `CredentialOffloadThroughV5BuilderTest` (2): a blocking v4 credential plugin stays
      wrapped and its `getAuthData` runs on the blocking executor, not the caller; a TLS-only plugin runs
      raw. **Follow-up** (self-review, commit `c4b85da2418`): a bridged v4 plugin presenting BOTH mTLS
      material AND a fetched credential, used *without* `tlsPolicy(...)`, must run raw so the legacy TLS
      path still presents its client certificate (the wrapping adapter's synthesized v4 provider carries
      only the binary credential, not TLS material) — it forgoes off-load with a WARN, restored by
      configuring `tlsPolicy(...)`. Regression test added; the two generic-fold gates stay green.

    - **Commit D — `[fix][client] freeze-forever auth/TLS API items (A-M2, A-L5, A-M4)`.** **A-M2:**
      `TlsPurpose.equals`/`hashCode` now cover `(role, name)` only — the fallback is resolution metadata,
      not key identity, so `client("x")` and `client("x", CLIENT_DEFAULT)` resolve to the same
      purpose→policy slot (the old fallback-in-key split one config key into two → silent lookup misses).
      **Blast radius:** all well-known purpose constants have a `null` fallback so their `(role, name)`
      identity — and every existing map lookup keyed on them — is unchanged; only plugin-minted
      `client(name, fallback)` / the new `server(name, fallback)` change, which is the intended fix.
      `FileBasedTlsFactoryTest` **35/35** confirms the purpose→policy map still resolves. **A-L5:** the
      three binary-auth records renamed their component to a uniform `bytes()`
      (`BinaryAuthData.authData()`/`AuthChallenge.challenge()`/`ChallengeResponse.responseBytes()`); every
      reader updated (compile-driven), positional constructors unchanged. **A-M4:** added
      `TlsPurpose.server(String, TlsPurpose)`. **Test** `TlsPurposeTest` (4).

    - **Commit E — `[fix][misc] auth/TLS SPI contract javadoc + the 2 missing client-auth metrics
      (A-M1/M5/M3/M7, G-metrics)`.** Docs: SSLParameters well-known row + synthesis merge-order on
      `PulsarTlsFactory` + tls `package-info` (A-M1); `v5.auth` `package-info` rewritten as a
      capability-model overview — 2×2 matrix, two config paths, 3 binary routing rules — + the
      "sentinel never reaches me" note on `BinaryAuthChallengeHandler` (A-M5); `ProxyConfig.toString`
      masks the password (A-M3); `AuthenticationFactory.tls` documents the fold + per-field plugin-wins +
      PEM-only (A-M7). **Metrics (G-metrics):** new `AuthMetrics` emits
      `pulsar.client.auth.credential.duration` (histogram, s, `{auth_method}`) and
      `pulsar.client.auth.failure` (counter, `{auth_method, error=terminal|transient}`) from the init
      context's OpenTelemetry, wired at `BinaryAuthenticationExchange.getAuthDataAsync` (both driver sites)
      and `HttpAuthenticationDriver` (`getHttpHeadersAsync`); no-op under the default no-op root. **Test**
      `AuthMetricsTest` (3, in-memory OTel reader): success → duration only; rejected → `error=terminal`;
      generic fetch error → `error=transient`.

    - **Verify (all local, worktree `async-auth-interface`, `dangerouslyDisableSandbox` for the Gradle
      wrapper).** spotless clean. Full build GREEN for `:pulsar-common-api` `:pulsar-client-api-v5`;
      compile (main+test) + spotlessCheck GREEN for `:pulsar-client-original` `:pulsar-client-v5`
      `:pulsar-client-auth-sasl`; `:pulsar-proxy` compile (main+test) GREEN. Gate suites, all **0 failures /
      0 errors**: `ClientCnxAsyncAuthTest` **7/7** (carve-out untouched), `V5ToV4AuthenticationAdapterTest`
      **6/6**, `LegacyV4AuthenticationAdapterTest` **7/7**, `TlsAuthenticationTest` **2/2**,
      `BinaryAuthenticationExchangeTest` **4/4**, `PulsarClientBuilderV5Test` **9/9**, `CredentialOffloadTest`
      **3/3**, `HttpAuthenticationDriverTest` **8/8**, `AuthenticationOAuth2Test` **29/29**,
      `SaslAuthenticateTest` **4/4** (binary+HTTP), `FileBasedTlsFactoryTest` **35/35**, plus the new
      `V5AuthenticationLoaderTest` **5/5**, `CredentialOffloadThroughV5BuilderTest` **2/2**, `TlsPurposeTest`
      **4/4**, `AuthMetricsTest` **3/3**.

    - **Deferred (reported, not done in this pass):** the remaining A-L / DOC-POLISH items (A-L1/M6/L9a/L9b/
      L7/L3/L4/L8/L2, G-IS7 PIP layering amend, Athenz-SDK-HTTP PIP note, stale "until stage N" comment
      sweep) belong to the FIX-5 doc/PIP batch. Single-pass HTTP `getHttpHeadersAsync` remains dead-code for
      the v4 built-ins (goals-review DEFER), so the HTTP metric currently only fires on the SASL-over-HTTP
      driver path.

39. **FIX-5 doc/PIP/polish batch** (2026-07-04, four local commits on `lh-pip-478-impl-v2`, no push;
    predecessor authored the edits and went idle before verify/commit — this pass verifies + commits them).
    Comment/javadoc/PIP only, no behaviour change; the four commits are grouped by concern:

    - **Commit `ed5f822cb63` — `[improve][misc]` stale implementation-marker sweep** (~51 files across
      client, client-v5, client-admin, common(-api), broker(-common), proxy, websocket, functions-worker,
      athenz/sasl plugins). Removes PIP-478 authoring scaffolding that leaked into shipped javadoc/comments —
      "stage 3b/3c/4a/4c" tags, decision-ids, and sweep-narrative asides (e.g. `ProxyConnection`'s "ninth
      call-site the stage-4c sweep missed" reworded to state the current self-built-config contract). Closes
      the deferred "stale 'until stage N' comment sweep" flagged in entry 38.
    - **Commit `152ee01f5ce` — `[improve][conf]` deprecate ignored PIP-337 cluster fields + standalone.conf.**
      `ClusterData` getters/builder-setters `brokerClientSslFactoryPlugin`/`...Params` marked `@Deprecated`
      (with `@deprecated` javadoc: removed by PIP-478, retained for wire/metadata compat, ignored with WARN,
      factory selection is broker-level `brokerClientTlsFactoryClassName`); `ClusterDataImpl`'s two builder
      overrides marked `@Deprecated` to match the interface (silences the override-deprecation warnings).
      `standalone.conf` documents the four PIP-478 TLS-factory keys already present in `broker.conf`
      (`tlsFactoryClassName`/`Config`, `brokerClientTlsFactoryClassName`/`Config`).
    - **Commit `c7c853f7590` — `[improve][client]` tighten auth/TLS/HTTP SPI contract javadoc** (8 files):
      `TlsHandle` (get() never blocks / after-dispose unspecified / dispose() idempotent), `PulsarTlsFactory`
      (initialize-once-before-createInstance, concurrent createInstance, close-at-most-once ordering),
      `PulsarHttpClientFactory.newHttpClient` (construction-time `IllegalStateException`, no request I/O),
      `HttpAuthHeaders`/`HttpResponse`/`HttpRequest` (single-value-per-canonical-name, last-wins collapse),
      `Authentication` (capabilities never queried before `initializeAsync` completes),
      `PulsarClientBuilder.authentication(Authentication)` (programmatic-path adopt contract: instance must be
      pre-configured; client drives `initializeAsync`+`close`).
    - **Commit `112720c45f3` — `[improve][pip]` PIP amendments.** Athenz/ZTS reconciliation (only OAuth2 among
      built-ins gets a framework `PulsarHttpClient`; Athenz stays on the SDK transport); OAuth2/Athenz
      reuse-not-duplicate layering note (v5 bodies are thin readers over a Supplier, gaining the async path,
      not a v5-native reimplementation); `ClientAuthenticationServices`/`...Aware` added to the public-API
      inventory; `SSLContext`/`SSLParameters` added to the well-known TLS instance list; the `sslProvider`
      JCE-provider-name → JDK-engine delta documented (niche, not a security regression); and the **M-F6
      known gap** — the proxy's own broker-client credential I/O still runs inline on the Netty loop (proxy
      lookup/data paths are not a `PulsarClientImpl` and bind no client auth services), same as v4, deferred
      to a follow-up. **M-F6 decision: document the gap, do NOT attempt proxy service-binding in this PIP.**
    - **Verify (all local, worktree `async-auth-interface`, `dangerouslyDisableSandbox` for the Gradle
      wrapper).** `spotlessApply` made no changes across the touched modules. `compileJava`+`compileTestJava`+
      `checkstyleMain`+`checkstyleTest` all **BUILD SUCCESSFUL** for `:pulsar-common-api`
      `:pulsar-client-api-v5` `:pulsar-client-admin-api` `:pulsar-common` `:pulsar-client-original`
      `:pulsar-client-v5` `:pulsar-client-admin-original` `:pulsar-client-auth-athenz` `:pulsar-client-auth-sasl`
      `:pulsar-broker-common` `:pulsar-proxy` `:pulsar-websocket` `:pulsar-functions:pulsar-functions-worker`
      `:pulsar-broker`. Only non-fatal deprecation warnings (the intended `@Deprecated` additions); no fixes
      needed — the edits introduced no compile/checkstyle breakage. **Out-of-scope note:** the pre-existing
      "**Confined removal.** … `SecurityUtility` … remains" line in `pip-478.md` is now stale relative to
      FIX-6 (which deleted `SecurityUtility`); it is unchanged context, not a FIX-5 edit, so left untouched
      here — a candidate for a later PIP-Removal-section reconciliation.

40. **Security-negative + e2e test batch (multi-model review "don't-trust-green-CI", T1/T2/T4/T5)**
    (2026-07-04, local commits on `lh-pip-478-impl-v2` via an isolated worktree, no push). Four additive
    test groups closing the coverage gaps a green CI does *not* prove — the codex test-coverage aspect's
    top items. Each group committed separately. No product code changed; one stale test-coverage note
    corrected. **No shipping defect surfaced** — the two security-critical negatives (T1 secure-reject, T4
    needClientAuth-reject) both fail the handshake as required, and the proxy lookup-pool null-factory NPE
    the old note warned of is already fixed on this branch.

    - **T1 — `[improve][test] PIP-478 T1: forced-untrusted-cert Netty mTLS rejection negative`.** The
      existing `FileBasedTlsFactoryTest` secure/insecure client-cert pair only proved the client silently
      *withholds* its cross-CA cert (the server advertises only its trusted CA), never that a *presented*
      untrusted cert is rejected. Added a real negative on the Netty engine mirroring the Jetty forced test:
      a `ForcingKeyManager` makes the client actually present the untrusted (EC, cross-CA) certificate.
      `forcedUntrustedClientCertRejectedBySecureServer` (secure server, real trust manager,
      `requireTrustedClientCert=true` → handshake `SSLException`) +
      `forcedUntrustedClientCertAcceptedAndCapturedByInsecureServer` (insecure server → accepted and
      captured, the D3 behavior). TLSv1.2 pinned so the rejection surfaces synchronously in the in-memory
      handshake pump. `FileBasedTlsFactoryTest` **37/37** (was 35).

    - **T4 — `[improve][test] PIP-478 T4: SSLParameters companion LIVE-handshake enforcement`.** Existing
      companion coverage asserted getters (Jetty) / the low-level synthesize helper (Netty). Added live
      handshakes flowing a companion through the full factory → `TlsContextAcquisition` path on BOTH
      engines: Netty (`SslParametersSynthesisTest` +2 — protocol restriction and `needClientAuth` via
      `acquireNettyContext`) and Jetty (`JettyTlsFactoryTest` +1 — a live `CompanionFactory`-driven server
      rejects a no-cert client and a TLSv1.3-only client, accepts a trusted-cert TLSv1.2 client).
      `SslParametersSynthesisTest` **14/14** (was 12); `JettyTlsFactoryTest` **7/7** (was 6).

    - **T2 — `[improve][test] PIP-478 T2: proxy e2e SSLContext-fallback synthesis on the data path`.**
      `SslContextFallbackSynthesisTest` had skipped the client→proxy→broker produce/consume data path,
      citing a proxy lookup-pool null-factory NPE. That NPE was already fixed on this branch (commit
      `51929992e84`, R1, resolves the binary-lookup pool's `CLIENT_DEFAULT` factory), so the note was
      stale. Added the e2e: a v5 client with the SSLContext-only factory produces/consumes through the TLS
      proxy; every synthesized leg (client transport, proxy PROXY listener, proxy `BROKER_CLIENT` via
      `DirectProxyHandler`) is exercised and the refused-Netty counter advances. The stale coverage note is
      corrected — the proxy binary-*lookup* leg uses a default file-based context by design (it does not
      honor the custom `brokerClientTlsFactoryClassName`; a deliberate `ProxyService.start` choice), which
      does not affect the synthesis proven on the client and proxy-data legs.
      `SslContextFallbackSynthesisTest` **4/4** (was 3).

    - **T5 — `[improve][test] PIP-478 T5: websocket + functions-worker WEB TLS via tlsFactoryClassName`.**
      No test had selected a custom factory via `tlsFactoryClassName` for these modules. Added a shared
      counting fixture `CountingWebTlsFactory` (counts *every* `createInstance`, so the JDK-`SSLContext` WEB
      path registers — unlike `SslContextOnlyTlsFactory`, which counts only refused Netty requests) and two
      broker-free e2e tests, placed in `pulsar-broker/src/test` (the module that carries the test-cert
      convention and depends on both ws + worker): `WebSocketProxyTlsFactoryTest` (standalone `ProxyServer`,
      HTTPS `GET /status.html` → 200 "OK") and `WorkerServerTlsFactoryTest` (standalone `WorkerServer` with
      a mocked `WorkerService`, HTTPS `GET /version` → 200). Each asserts the custom-factory counter
      advanced, proving the WEB purpose is served through the new SPI end-to-end. **1/1** each.

    - **Verify (all local, isolated worktree, `dangerouslyDisableSandbox` for the Gradle wrapper).** spotless
      clean. Targeted `--tests` runs, all **0 failures / 0 errors**: `FileBasedTlsFactoryTest` **37/37**,
      `SslParametersSynthesisTest` **14/14**, `JettyTlsFactoryTest` **7/7**, `SslContextFallbackSynthesisTest`
      **4/4**, `WebSocketProxyTlsFactoryTest` **1/1**, `WorkerServerTlsFactoryTest` **1/1**; the pre-existing
      `ProxyKeyStoreTlsTransportTest` **1/1** was run to confirm the R1 lookup-pool fix. Touched modules
      compile: `:pulsar-common`, `:pulsar-broker-common`, `:pulsar-proxy`, `:pulsar-broker` (which pulls
      `:pulsar-websocket` + the `:pulsar-functions` worker).

41. **Multi-model review cycle COMPLETE (8 reviewers → 7 fix workstreams → verified).** An 8-lens
    review board (5 Fable: api-design, concurrency, security, goals, TlsPolicy-parity; 3 Codex:
    lifecycle/rotation, backward-compat, test-coverage) delivered the verdict that the mission's core
    goal is achieved — all 5 Motivations materially delivered with thread-level evidence, the ClientCnx
    async carve-out **verified production-ready** (concurrency lens could not construct a failing
    interleaving), security ship-ready. Every finding was edge-hardening, triaged against the original
    goals (documented-intentional items — PIP-337 removal, the Jetty trust-scoping security fix, ruled
    CLI removals — rejected-as-defect, not "fixed"). Fixes landed as FIX-1..6 + the test batch (commits
    `2165753bc7b`..`4e300e58c7f`):
    - FIX-1 [GATE]: the one serious defect — an OpenSSL TLS-rotation use-after-free (release-before-
      publish + unpinned volatile borrows; CI never exercised the OpenSSL regime) — fixed both sides
      (deferred-release + per-use `withPinnedContext`) with a new OpenSSL-provider rotation test.
      **Codex re-reviewed the exact diff: zero findings, fix sound.**
    - FIX-2: TlsPolicy `storeType`→separate `keyStoreType`/`trustStoreType` (v4-parity regression),
      `{TLSv1.3,TLSv1.2}` default preservation, per-cluster custom-factory fail-loud.
    - FIX-3: the two API-freeze blockers (v5-native reflective config path = In-Scope #2 gap; REFRESH
      contract unified) + freeze-forever items (TlsPurpose equals over (role,name), uniform `bytes()`)
      + 2 missing metrics; self-review caught a combined-TLS+credential mTLS-drop bug.
    - FIX-6 (user-requested): dissolved the `SecurityUtility` grab-bag into `PemReader`/`JcaProviders`/
      `JdkSslContexts`; deleted the obsolete `KeyManagerProxy`/`TrustManagerProxy` delegate-swap helpers
      (rebuild-not-mutate rotation makes them dead) + `SSLContextValidatorEngine`.
    - FIX-4: security-negative + e2e tests (forced untrusted-cert rejection Netty+Jetty, proxy e2e
      fallback synthesis, SSLParameters live handshakes, ws/worker HTTPS) — **no defects, no weakened
      assertions.**
    - FIX-5: doc/PIP/javadoc polish; M-F6 (proxy→broker credential offload) documented as a v4-parity
      known-gap (deferred, not a regression); corrected the review's wrong A-L4 assumption.
    Deferred non-blocking follow-ups (logged): proxy binary-lookup leg honoring a custom
    `brokerClientTlsFactoryClassName`; the M-F6 proxy credential-offload gap. Remaining before
    apache-facing: master rebase + post-rebase CI + human review.

42. **CI note (2026-07-04): Personal-CI Preconditions gate blocked by a GitHub diff-endpoint error.**
    After the review-fix push, the Pulsar CI `Preconditions` job (which fetches the PR's changed-files
    list from the GitHub API for path-based test selection) began failing with
    `Server Error: Sorry, this diff is temporarily unavailable due to heavy server load.
    {"resource":"PullRequest","field":"diff","code":"not_available"}` — a GitHub-side failure to compute
    PR #231's diff, not a code/test failure (the same branch passed 41/41 at c9749a16afc; every fix
    workstream was locally verified compile + targeted gates green + codex-cleared for FIX-1). Mitigations:
    advanced the CI base branch `pulsar-pip478-ci-base` from the original merge-base `1fa9e3532b4` to
    `c9749a16afc` (PR diff 249→130 files) and opened a fresh PR (#233, head=tip, base=c9749a16afc) so the
    changed-files computation isn't burdened by PR #231's long update history. CONFIRMED sustained
    GitHub outage: the fresh PR #233 failed Preconditions with the identical error, so it is server-side,
    not PR #231's history (#233 closed). The full-matrix Personal-CI pass on the review-fix delta is
    therefore a "retry when GitHub's diff endpoint recovers" item; the code itself is complete, 8-model
    reviewed, hardened across FIX-1..6 + test batch, FIX-1 codex-cleared, and every workstream
    locally verified (compile + targeted gates green) on top of the 41/41-green c9749a16afc base.
