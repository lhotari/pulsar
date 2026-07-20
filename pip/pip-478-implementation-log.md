
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

66. **PART B COMPLETE — minimalism FIXPOINT reached, unanimous (2026-07-06).** Round 4 (fixpoint confirmation):
    ALL THREE models — Codex GPT-5.5, Fable 5, Opus 4.8 — independently returned FIXPOINT with the anti-
    manufactured-cut guard in force. The reviewers who drove every prior cut (Codex + Fable) now find NOTHING:
    the contract-weight-with-no-consumer category (the class rounds 1-3 removed) is empty; round-3 cuts left no
    dangling refs; the "honesty list" keeps (safety guards + escape valves, zero-rule projections for plugin
    authors, service seams with consuming twins, RFC vocabulary, SSLParameters/SinglePass/TlsEndpoint) are all
    correctly kept — verified against production call sites, not declarations. The four major decisions hold IN
    SHIPPED CODE: capability(Class) sole discovery (zero instanceof-capability dispatch anywhere), one-way module
    arrows (tls-factory-api imports no http/client packages), flat (role,name) TlsPurpose (no fallback residue),
    rebuild-not-mutate rotation (delegate-swap helpers removed). No forcing shape remains.
    ARC: Part B ran 4 review rounds + 3 cut-iterations. Round 1 (-232 lines), round 2 (brokerPort + Form), round 3
    (defaultHeaders + headers(Map) + per-request timeout), each fully re-cut into the 6-PR series + CI-green.
    Fable's PRINCIPLED LINE — "cut what carries contract weight (interaction rule / merge precedence / duplicate
    path) with no consumer; KEEP zero-rule projections that serve third-party authors, safety guards with their
    escape valves, and service seams with consuming twins" — is the deciding rule and a KEY skill lesson.
    Fixed a trivial record-K wording nit (Opus: "sole timeout knob" → "sole request timeout knob; connect/read
    remain separate"). NEXT: final re-cut (folds the K nit) + CI green, then Part C — the PIP-design skill.

67. **PART C COMPLETE — PIP-design skill created + mission A/B/C done (2026-07-07).** Created the Claude skill
    `pip-design` in ~/workspace-pulsar/pulsar-contributor-toolbox/claude-code/skills/pip-design/SKILL.md,
    capturing this mission's methodology: (1) understand the draft against the code (grep production call sites,
    not declarations — verify, don't trust prose); (2) multi-model multi-viewpoint review (Fable 5 design-
    coherence/subtractive + Opus 4.8 xhigh durability/implementability + Codex GPT-5.5 adversarial; convergence =
    confidence); (3) the minimalism judgment — the "cut contract-weight, keep zero-rule projections" principle,
    additive-add-back, the lying-accessor/duplicate-path tests, and the manufactured-minimalism guard; (4) the
    full doc+code+CI pipeline with decision records (cut: what/why/add-back/tradeoff; keep: rationale + honesty
    list); (5) iterate to a fixpoint (review until a full round finds nothing). Embeds both quotes (Saint-Exupery
    + Jobs) as the north star. Also documents the equivalence-checked stacked-PR decomposition + CI/orchestration
    discipline for PIPs with a reference implementation.
    MISSION STATUS: A (secure-by-default series, CI-green) + B (minimalism fixpoint, unanimous 3-model) + C
    (skill) all COMPLETE. Final fixpoint series on lhotari/pulsar: 01=460190e14db 02=64cd17f89bb 03=86dff7b2fdb
    04=71c3c582bd7 04b=ca5c4d25606 06=668de490c82 (code byte-identical to the CI-validated round-3 series; only
    a doc-word differs; final CI clean except the recurring TopicPoliciesTest.setupTestTopic flake, rerun-
    clearing). Monolith lh-pip-478-impl-v2 @ d8721a1978a. Reserved for the human author (deliberately NOT done):
    master rebase + apache-facing PRs + human review.

68. **CONFIG-REMOVAL + jcaProvider + FIPS design & impl, reviewed & fixed (2026-07-07).** Two user directives:
    (a) remove ALL PIP-337 sslFactoryPlugin config in 5.0 for the v4 client too (not deprecate), add tlsFactoryClassName/
    tlsFactoryConfig successor on the v4 client+admin; (b) add jcaProvider (renamed from the legacy "jceProvider" per user)
    to TlsPolicy — names a java.security.Provider (ServiceLoader on app classloader, else Security.getProvider, fail-loud);
    when set the default factory builds SslProvider.JDK + sslContextProvider (overrides engine). DESIGN also finalized:
    FIPS-140-3 goal/motivation incl. the L1 (BC-FIPS + file PEM, plaintext key entry permitted) vs L3 (PKCS#11 HSM, keys
    never leave the module — served by a custom PulsarTlsFactory) distinction; pulsar-client-shaded correction (v5 will NOT
    ship shaded, users relocate themselves); and REMOVED the "Minimalism review" section + the "five-plus years" durability
    statement + the Saint-Exupery/Jobs quotes + all multi-model-review framing (user: read as the author's own document).
    IMPL Parts A-D (config removal 4819ed630dc; removed-key validation 068a3f5dc5c; v4 successor 54b0cfda6da; jcaProvider
    wired every component f6535cdbe46). MULTI-MODEL REVIEW (Codex adversarial high + own wiring sweep) found 6 real defects
    the impl's 118 tests missed: #1 brokerClient jca/sslProvider not propagated to the internal PulsarClient (FIPS gap),
    #2 brokerClient_ prefix bypassed the removed-key reject, #3 sslProvider=JDK misrouted to jcaProvider (hard v4 regression),
    #4 ServiceLoader could abort before the Security.getProvider fallback, #5 KeyManagerFactory not provider-pinned, #6 reject
    fired on the old default FQCN. ALL FIXED + tested (803a0cfa67a FIX3 code+doc, 944f3f566c0 FIX2+6, 79b34162ab5 FIX4,
    11fb3dd98bc FIX5, 1efb5228bdf FIX1); every touched module builds main+test, all suites green. LESSON: a clean local run
    (118 passing tests) is weak evidence — the adversarial review caught FIPS-undermining + v4-breaking bugs the tests didn't.
    PUBLISHED (human-directed, this cycle): apache/pulsar #25890 (the human's official PIP-478 PR, head 31bc080) updated with
    the finalized+corrected pip-478.md + a FIPS-140-3/TLS-transport description; fork #241 design series (head 46dea9ae48c)
    matched. Drafted a friendly PIP-489 (FIPS-mode proposal) reply in ~/Downloads/pip-489-comment.txt suggesting PIP-489 build
    upon PIP-478 for the TLS-transport slice (jcaProvider L1 + PulsarTlsFactory/HSM L3 + PulsarHttpClient), for the human to post.
    Monolith lh-pip-478-impl-v2 @ 1efb5228bdf (design finalized + impl A-D + 6 fixes). NEXT: fold the fixed impl into the fork
    code chunks #242-247 (still fixpoint code) — full 6-chunk re-cut, deletions to chunk 6, equivalence to monolith — + Personal CI.

69. **Multi-model Codex per-PR review + 2 fix rounds + jsseProvider rename (2026-07-07).** Ran 4 Codex (gpt-5.5 high)
    read-only per-PR reviews of the fork series. Round 1 (config-removal/jcaProvider) fixed 6 defects; ROUND 2 found 7 more
    real bugs the impl's tests missed — dominant theme = the event-loop-blocking the PIP set out to kill, leaking through:
    A BrokerService.configTlsSettings/configAdminTlsSettings dropped the brokerClient provider (geo-rep + cross-cluster admin
    silent FIPS downgrade — FIX 1 was incomplete); B built-in token plugin read the token file on the event loop; C SASL-over-
    HTTP lost its cross-request role-token cache (v4 wire/behavior regression); D SASL GSSAPI ran inline; E FileBasedTlsFactory
    dispose double-release race; F close()-vs-in-flight leak; G reflective factory ignored the TCCL. ALL 7 fixed + tested green
    (SASL cache restored via a volatile body-level cachedRoleToken mirroring v4). DESIGN findings: #1 (jcaProvider mis-framed —
    sslContextProvider needs a JSSE provider; jcaProvider=BCFIPS FAILS since BC-FIPS is crypto-only with no SSLContext.TLS; FIPS
    needs BCJSSE) → user chose to RENAME jcaProvider→jsseProvider + reframe (jsseProvider = the JSSE/SSLContext provider; FIPS =
    jsseProvider=BCJSSE with BC-FIPS registered separately as the crypto provider). #3 ClusterData purpose contradiction →
    reconciled to the verified code model (per-client factory from each client's own config carrying per-cluster material,
    fixed BROKER_CLIENT purpose, NO minted per-cluster purposes). #4 break-summary augmented (ClusterData accessors + CmdClusters
    CLI). #6 "non-default value" defined (default FQCN tolerated). REMAINING design notes (surfaced, not blocking): #2 v4
    in-memory TLS material not representable in the bridge; #5 "custom PIP-337 factories are rare" is an unverified assumption.
    GIT-RECOVERY LESSON: a re-cut builder left the worktree on a temp branch `recut-build`; the next fix builder built on that
    wrong base. Recovered by cherry-picking the 5 fix commits onto the correct monolith (clean) + re-verifying (compile + all
    fix tests pass). Added an explicit base-verification guard (branch+SHA+clean check, STOP if wrong) to the rename builder —
    it passed. Monolith lh-pip-478-impl-v2 @ 0730ba49414 (complete: config removal + jsseProvider + round-1&2 fixes + reframe +
    #3/#4/#6). NEXT: re-cut the mega-chunk-6 series (fixpoint chunks + corrected design; chunk 6 = monolith) + re-push apache
    #25890 & fork #241 with the corrected design + CI.
70. **MASTER REBASE + ROUND-1 gpt-5.6-sol/Fable REVIEW CYCLE (2026-07-19, user-directed).** Directive: rebase on
    latest origin/master, keep improving with Fable + Codex "gpt5.6", Personal-CI PRs, iterate to fixpoint.
    REBASE: merged origin/master 3d00280d122 (185 commits: Netty 4.1.134→4.2.16, AHC 2.15→3.0.10 Duration APIs,
    Swagger v3/jakarta ee10 PIP-472, BK 4.18, #26020 publishing, master-side v5 migrations of pulsar-perf + CLI)
    into the monolith — 27 conflicts (23 via resolve+verify agent pipeline, 4 modify/delete by hand; backup tag
    pip478-pre-master-sync-0b4600e, merge 385ab68e66c). The merge audit surfaced a REAL latent bug: Netty 4.2
    defaults client SslContexts to HTTPS endpoint identification; buildNettyClientContext never cleared it when HV
    is off → HV-disabled clients still hostname-verified (fix 25274b5, existing test proven fail-without/pass-with;
    the synthesized-JDK path was already correct). Master adaptations: MessageCryptoBc→JcaProviders, CLI/perf→
    org.apache.pulsar.tls.TlsPolicy, AHC Duration wrapping. Series RE-CUT onto master via deterministic tree
    builder (36 shared-path intermediates from 3-way merge-file base=old-06/ours=merged/theirs=old-04b; 2 hand-
    resolved config intermediates keep deprecated sslFactoryPlugin fields in @Schema syntax at chunk 4); per-chunk
    sanityCheck gates VERIFIED-tree green (a silent checkout-abort on the untracked impl-log invalidated one gate
    run — always assert rev-parse HEAD==expected); pushed 01=8e501b189b2b..06=374ee9b8df59, series-base→master.
    CODEX MODEL: "gpt5.6" = gpt-5.6-sol (flagship; bare gpt-5.6 alias rejected on ChatGPT accounts; CLI self-
    updated 0.141→0.144.6; family sol/terra/luna).
    ROUND 1: 5 Codex gpt-5.6-sol per-chunk reviews (16 findings) + Fable 6-dimension workflow (14 findings) →
    EVERY finding adversarially verified before fixing. REFUTED with evidence (7): S3-2/S3-6 (already fixed by the
    entry-69 FIX E/F), S3-5 (scheduler-owns-rotation is the documented contract), S6-2 (ClusterData lenient-drop
    is the documented deliberate model), S6-3 (Jersey async pool unbounded → no starvation), 2 chunk-placement
    artifacts (SASL fixes live in chunk 6). CONFIRMED+FIXED (all test-verified): SASL round bound orTimeout
    (97a834a); jsseProvider KMF/TMF algorithm negotiation — BCJSSE registers X.509/PKIX not SunX509, verified in
    bc-java (2bba508); v4 auth-header composition offloaded off the shared event loop — BLOCKER, OAuth2 shim
    self-deadlock via shared ELG on the redirect path, thread-asserting test (c2c1d2e); 4 TLS factory hardenings —
    generation-guarded lastBaseline, volatile Subscription.current, companion-failure handle dispose, async null-
    callback reject (993aacd); round-1 batch (a56bb32): CLI HV secure default + keystore translation (the 'not
    supported' warning was FALSE), proxy lookup-leg brokerClientTlsFactory propagation (HSM outage fix), OAuth2
    redirect parity + streaming maxResponseBodyBytes enforcement, channel-init reject-dispose, jsseProvider conf
    docs + CN→SAN wording + comment corrections. Doc: merge-rule 2 states the explicit endpoint-id clear (477d223).
    NEXT: partition restructure (fold fixes+jsseProvider into introducing chunks; chunk-6 message must disclose
    config-removal+successor — Fable BLOCKER on message honesty), re-gate, re-push, CI, ROUND 2 to fixpoint.

71. **ROUND 2 COMPLETE — Fable(brain)/Opus-4.8(exec)/Codex-gpt-5.6-sol(review), 2026-07-20.** User re-directed the
    model split mid-round. Codex round-2: 5 per-chunk reviews → 13 distinct findings; Fable panel (Opus) added
    fix-regression + restructure + server-integration dimensions. EVERY finding Opus-verified before fixing.
    REFUTED (4, evidence-backed): C1 (Netty KMF/TMF not provider-pinned — but pip-478.md:860 documents
    sslContextProvider-only, and it's MORE pinned than v4 which pinned nothing on the Netty path; BCJSSE/Conscrypt
    consume standard X509KeyManagers — I had wrongly been confident C1 was real, verify-before-fix caught it), C2
    (buildJdkContext omits engine policy for the SSLContext fallback — but every in-tree consumer applies policy
    via Jetty setters or the synthesis wrapper; no consumer builds raw engines; and a JDK SSLContext has no API to
    bake protocols anyway), C4 (broker jsseProvider IS wired — reviewed an intermediate), C10 (admin SASL inline —
    Jersey async pool unbounded, no starvation). NON-DETERMINISM LESSON: C5/C11/C13 FLIPPED refuted→real between a
    partial run and the full re-run — adjudicated each against the code MYSELF (Fable): all 3 real (copyFlags omits
    jsseProvider; currentRoleToken flattens the v4 subtype via RuntimeException; v4 keystore path DID honor
    tlsProvider=Conscrypt as the JSSE provider). Don't trust a single LLM verification pass on a flip — read the
    code. CONFIRMED+FIXED (all gate-green, test-verified):
    WAVE 1 (a0c840552fc): C12 MAJOR broker-client purpose delegate (BrokerClientPurposeFactory translates
    CLIENT_DEFAULT→BROKER_CLIENT at both custom-factory sites + ProxyService wired brokerClientPurpose=true) — a
    compliant BROKER_CLIENT-only custom factory was getting CLIENT_DEFAULT → empty/downgrade; C9 MAJOR (bug in my
    round-1 SASL fix — orTimeout bounded only the GET not the GSSAPI eval; now whole-round); C6 MAJOR cross-format
    trust-anchor silent drop → fail-loud; C7 MAJOR CLI reads jsseProvider; C3 MINOR Jetty companion rotation; C8
    MINOR WebService factory leak; ServiceChannelInitializer cleanup; DefaultBrokerTls + ServiceConfig javadoc.
    WAVE 2 (6719e525f0d): C5 MAJOR jsseProvider dropped in the v4-auth fold (copyFlags) → preserve it; C11 MINOR
    Athenz exception subtype (RuntimeException→CompletionException); C13 MAJOR broker keystore Conscrypt parity
    (two-axis resolveJsseProvider mirroring the client + pip-478.md update); C3-REDO MAJOR (wave-1's C3 fix
    introduced a .join() self-deadlock on the Jetty reload delivery thread — the Fable server-integration reviewer
    caught it; now async whenComplete like SynthesizingSubscription); WorkerServer factory-leak guard.
    Monolith tip 6719e525f0d. NEXT: re-cut v7 (fold round-2 fixes to their chunks; move the chunk-6 "CmdClusters
    CLI flags" clause to chunk-4 per the Fable restructure MINOR), gate, re-push, ROUND 3.
