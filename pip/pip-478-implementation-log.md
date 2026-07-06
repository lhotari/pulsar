
61. **PART A COMPLETE — full series CI-GREEN on lhotari/pulsar (2026-07-06).** All 6 PRs green:
    #241 design, #242 SPI modules, #243 tls-impl, #244 core migration, #245 auth plugins, #247 removal.
    The TopicPoliciesTest.setupTestTopic failure was the documented flake (server-side TopicFencedException in
    PersistentTopic.delete — pure topic-lifecycle race, zero connection to TLS/PIP-337/CN/the removal chunk);
    cleared on rerun. The series is byte-for-byte equivalent to the monolith (equivalence=0, excl impl-log),
    both CI gates (GATE_A assemble+checkstyle + GATE_B checkBinaryLicense) green per chunk.
    Final SHAs: 01=fb967fbfc4e0, 02=6a1b469bdc91, 03=71d5fe7b615b, 04=b2574608810e, 04b=8fe505175c44,
    06=8995e82330f8. Monolith tip = 32df9e3f77b (+ this log entry).
    Secure-by-default/SAN-only shipped clean after 3 CI iterations of principled test fixes (SAN cert regen,
    advertised-address pins, R3 test-determinism, cert-set chunk placement) — HV kept genuinely ON throughout,
    no production bugs. NEXT: Part B — the minimalism / 5-year-hold review loop.

62. **PART B — minimalism review round 1 + cuts (2026-07-06).** 3-model panel (Fable 5, Opus 4.8 xhigh,
    Codex GPT-5.5 high), PRIMARY lens simplicity + 5-year durability. All three judged the FOUR major decisions
    (capability(Class) discovery, three-SPI split, TlsPurpose-as-key, rebuild-not-mutate) 5-year-durable as-is;
    the core type set was already near-minimal. Converged cuts APPLIED (commits 63967041dbc/35615f212f1/
    4397049482e/8e4c6f230a0/b7709e7d0c8, net -232 lines, all additive add-back paths):
    - A [3/3]: AuthenticationFactory.tls(cert,key) → no-arg tls() marker; TLS material now ONLY via
      tlsPolicy(TlsPolicy.pem(...)). Removes the 2nd material-carrying auth path. v4 AuthenticationTls→
      CLIENT_DEFAULT fold is the separate, retained mechanism.
    - B [2/3]: AuthenticationInitContext.params() removed (redundant w/ configure(Map); unthreaded the vestigial
      chain). ClientAuthenticationServices collapse DEFERRED (real design call: nullness contract + public-
      extends-internal-visibility leak — not a mechanical merge).
    - C [2/3]: ProxyConfig + PulsarHttpClientConfig.proxy() removed — a shipped-but-silently-ignored knob
      (FrameworkHttpClientFactory wires SOCKS5 from ClientConfigurationData); violated fail-loud.
    - D [2/3]: TlsPurpose.fallback() + (name,fallback) overloads removed → flat (role,name) with terminal
      resolution; also removes the equals-excludes-fallback identity wart. Kept 1-arg mints + well-known
      constants + terminal rule (client→system default, server→config error).
    - F: TlsAuthentication(String) custom-name ctor removed (only caller passed default "tls").
    KEEPs (documented rationale, weighed + held): SSLParameters companion (load-bearing for the zero-non-JDK-dep
    tier-3 HSM/KMS factory to express protocol floor/client-auth/endpoint-id — Fable+Opus); SinglePassAuthentication
    (tier-1 pitch + 4 implementors); TlsEndpoint (plumbed SNI, cheap); the 4 capability interfaces; value records.
    NOT reversed (user-directed, flagged for user): SslContextFactory.Client well-known — all 3 reviews said cut
    (symmetry-only, no in-tree supplier), but the user explicitly added it this session for proxy→broker admin
    TLS customization; kept, tension recorded for the user's return.
    NON-CUTS (correct stops): wrap/unwrapV4 stays public (cross-package same-module); HTTP header case-asymmetry
    is DELIBERATE (request=wire-verbatim fidelity, response/auth-headers=lowercase lookup-convenience). NEXT: doc
    pass (removals + decision records + single-valued-header contract) → re-cut series → CI → fixpoint re-review.

63. **PART B minimalism round 1 — series re-cut + pushed; a base-integrity issue caught & resolved (2026-07-06).**
    Re-cut all 6 chunks from the minimalism monolith + force-pushed to the fork. SHAs: 01=009551706dd,
    02=b1c13b67178, 03=50afe6f8b6d, 04=12d5e945c3c, 04b=15c4929d487, 06=9301a1b36c5.
    BOUNDARY ISSUE (rebuild8 caught it): the minimalism work landed on a DETACHED HEAD (4ddd7466ff7) while the
    lh-pip-478-impl-v2 label went stale (rtk tooling desync around an earlier canary checkout), AND that 4ddd
    snapshot did NOT carry the chunk-6 PIP-337/CN removal (the 18 deletions were deferred, per the deletions-to-
    last-chunk rule — 4ddd had them PRESENT). Resolved WITHOUT papering over it: (A) removal is definitively in
    scope; chunk 6 re-stacks the byte-identical 18-file removal (== the CI-green fork-06). Proven benign by TWO
    trust anchors: (1) chunk-6 FULL GATE_A (assemble + checkstyleTest = whole-repo main+test compile) BUILD
    SUCCESSFUL + git grep of all 18 deleted class names = 0 matches → the 18 files are DEAD CODE in 4ddd; (2)
    base sanity — ServiceConfiguration/ClientConfigurationData HV default = true (R5), JcaProviders has zero CN-
    class references (b9a26f6 edit present), the 10 migration deletions absent. Constructed the true complete
    monolith C = 4ddd + 18-file removal (= rebuild-06 tree), re-pointed lh-pip-478-impl-v2 → 1f053fe9a17
    (backups: tags pip478-implv2-stale-32df, pip478-mono-4ddd). Equivalence rebuild-06 == lh-pip-478-impl-v2
    (excl impl-log) = ZERO. All chunk gates green (chunk4 FULL GATE_A+GATE_B + proxy-TLS runtime + 5 spot tests;
    chunk6 FULL GATE_A+GATE_B). LESSON: the detached-HEAD/stale-label desync is a real rtk hazard — verify the
    branch LABEL points where you think before trusting "monolith tip", and the per-chunk compile gate is what
    catches a deferred-deletion base drift. NEXT: CI green on the fork → fixpoint re-review.

64. **PART B minimalism round 2 — fixpoint check found 2 more (2026-07-06).** 3-model fixpoint re-review:
    Codex + Opus returned FIXPOINT (no new cuts; kept-contested SSLParameters/SinglePass/TlsEndpoint all
    re-verified load-bearing), but Fable 5 — grepping production CALL SITES, not just declarations — surfaced 2
    genuine residuals the other two missed. Applied (commits bc2bff28643/2da449a1985/49b00b2c493):
    - CUT AuthenticationCallContext.brokerPort() [Fable]: zero readers AND always fabricated as 0 (the async
      exchange seam is host-only: newAuthenticationExchange(String); both callers passed 0). A shipped LYING
      accessor — Cut-C's class in stronger form, the inverse of the TlsEndpoint keep (which carries real values).
      Context is now host-only; per-destination auth would re-enter via the seam, not this context. Additive add-back.
    - CUT HttpRequest.Form [Fable+Codex]: zero in-tree producers (OAuth2 TokenClient hand-encodes into Bytes;
      Form appeared only in the framework decode branch). CUT rather than exercise because exercising cannot
      preserve wire-equivalence — verified vs AHC 2.15.0: escaping + Content-Type match, but Form's Map.copyOf is
      unordered while TokenClient emits TreeMap-sorted, so producing Form would reorder token-request bytes on the
      wire. Body is now a single-member sealed interface (Bytes only); NOT collapsed to byte[] (loses Content-Type)
      — that flattening left as an open round-3 question. TokenClient + OAuth2 wire untouched (all OAuth2 tests green).
    - CONSISTENCY (round-1 cut residue): PulsarTlsFactory createInstance javadoc rewritten off the removed
      TlsPurpose#fallback() to the terminal rule (was a WRONG NORMATIVE contract, not just a dead @link — escaped
      round 1 via Xdoclint:none); PulsarHttpClientFactory @param dropped "proxy"; package-info "single-level
      fallback" → terminal; HttpResponse.bodyAsString UTF-8 assumption documented.
    All KEEPs re-confirmed; 4 major decisions durable. NEXT: doc pass (line 184 + brokerPort/Form records) →
    re-cut → CI → ROUND 3 fixpoint (judges the single-member Body + confirms convergence).

65. **PART B minimalism round 3 — the "contract-weight vs projection" line (2026-07-06).** 3-model fixpoint
    re-review of the post-round-2 design. Opus = FIXPOINT (no new cuts; verified AuthenticationCallContext
    didn't collapse post-brokerPort; single-member sealed Body is a KEEP — flattening would break the public
    Optional<Body> accessor when the anticipated streaming body lands, and the seal blocks a Body the framework's
    instanceof-Bytes-else-throw encoder can't handle). Codex + Fable = 3 HTTP-config residuals, which Fable's
    PRINCIPLED LINE resolved: cut what carries CONTRACT WEIGHT (interaction rule / merge precedence / duplicate
    path) with no consumer; KEEP zero-rule PROJECTIONS that serve the SPI's third-party audience. Applied (commit
    dd62471a168b, -60 lines, all zero-producer, additive add-back):
    - CUT PulsarHttpClientConfig.defaultHeaders()/defaultHeader() [Fable+Codex] — untested "defaults-first-then-
      per-request-wins" precedence contract, no producer, duplicates per-request header() + the userAgent field.
    - CUT HttpRequest.Builder.headers(Map) [Fable] — zero callers; a literal second way to do per-entry header()
      (even AuthenticationSasl iterates calling header(k,v)).
    - CUT HttpRequest per-request timeout override [Fable-lean+Codex] — no producer; per-instance requestTimeout
      is the real path.
    - javadoc wording: HttpRequest body dispatch is instanceof-else-throw, not an "exhaustive switch".
    KEPT (categorically different, verified): maxResponseBodyBytes (a load-bearing SAFETY GUARD — the 16 MiB cap
    fails-the-future unconditionally; cutting the knob makes it a hard wall with no recourse = a trap; AND it has
    a test producer) — NOT the same class as the no-producer duplicate paths; the sealed Body; the zero-rule
    projections (bodyAsString/header(name)/HttpAuthHeaders.of); the InitContext service seams (real values, TLS
    twin consumed, anti-Motivation-#3); token(Supplier) (v4 parity — flagged as deserving a follow-up test); the
    RFC Method enum. No round-2 orphans; 4 major decisions re-affirmed with call-site evidence. NEXT: doc pass →
    re-cut → CI → ROUND 4 fixpoint (the principled honesty-list suggests convergence).
