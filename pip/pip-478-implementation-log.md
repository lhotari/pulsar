
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
