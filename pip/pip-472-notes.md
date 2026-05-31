# PIP-472 Implementation Notes (javax.* → jakarta.* migration)

These are working notes for the implementation of [PIP-472](pip-472.md). They record the
**actual discovered state** of the codebase (which differs significantly from the PIP's
estimates), decisions made during implementation, and deviations from the PIP.

Branch: `lh-javax-to-jakarta-migration` → pushed to `forked` remote (`lhotari/pulsar`).
PR opened against `lhotari/pulsar` (NOT `apache/pulsar`) purely to exercise GitHub Actions CI.

## Decision: single coordinated branch (big-bang) instead of the PIP's 4 phases

The PIP describes 4 mergeable phases and explicitly *rejects* a single big-bang branch for
the upstream contribution workflow. For this CI-validation exercise on a personal fork we
implement everything on one branch so the whole migration can be validated end-to-end by CI
in one shot. The phased structure is still followed conceptually within the single branch.
When this is eventually contributed upstream it should be split back into the phased PRs.

## Discovered state (2026-05-31) — much smaller than the PIP estimated

Counts are from the current `master` (`a6af80198f2`). A large part of the migration the PIP
anticipated has already happened in git history.

| Surface | PIP estimate | Actual now | Notes |
|---|---|---|---|
| `javax.ws.rs` | ~660 | **169 files** | broker 90, client-admin 30, functions/worker 28, proxy 7, broker-common 7, websocket 4, common 2, client-auth-sasl 1 |
| `javax.servlet` | ~368 | **94 files** | broker 36, broker-common 19, websocket 15, proxy 9, functions/worker 5, + jetty-upgrade/test-plugins/auth-sasl (out of scope) |
| `javax.annotation` (JSR-250) | ~48 | **0 files** | all 12 `javax.annotation` imports are JSR-305 `concurrent.NotThreadSafe`/`ThreadSafe` — out of scope, no jakarta equivalent |
| `javax.validation` | in scope | **0** | already migrated / unused |
| `javax.xml.bind` | in scope | **0** | already migrated / unused |
| `javax.activation` | in scope | **0** | |
| `javax.inject` | in scope | **0** | |
| `javax.websocket` | in scope | **0** | Pulsar uses Jetty's native WebSocket API, not JSR-356 |
| Swagger 1.x (`io.swagger.annotations`) | n/a | **61 files** | broker 36, functions/worker 9, common 9, client 4, websocket 1, proxy 1, docs-tools 1 |

**Conclusion:** the real migration surface is `javax.ws.rs` (169), `javax.servlet` (94, minus
AdditionalServlet retention), and the Swagger 1→2 annotation transform (61). The annotation /
validation / xml.bind parts the PIP describes are already done or never existed.

### Important correction vs PIP CI-lint section

The PIP's Checkstyle ban list includes `javax.annotation` (excluding `javax.annotation.processing`).
But `javax.annotation.concurrent.*` and `javax.annotation.Nonnull`/`Nullable` are **JSR-305**
(findbugs), not Jakarta EE JSR-250, and have **no jakarta counterpart**. They must NOT be banned
and must NOT be migrated. The Checkstyle rule bans only the JSR-250 subset
(`javax.annotation.PostConstruct|PreDestroy|Resource|Generated|Priority|ManagedBean|Resources`),
not `javax.annotation.concurrent` / `javax.annotation.Nonnull` etc.

## Out of scope / left on javax (confirmed)

- `jetty-upgrade/zookeeper-with-patched-admin`, `jetty-upgrade/*-prometheus-metrics*` — these are
  compat shims that re-host ZooKeeper/BookKeeper admin & metrics servlets on patched Jetty; they
  intentionally stay on ee8/`javax.servlet`. Not Pulsar's own REST/servlet code.
- `tests/docker-images/java-test-plugins` AdditionalServlet test plugin — exercises the legacy
  `javax.servlet` registration path on purpose; kept on javax.
- `pulsar-broker-auth-sasl` `javax.security.*` — JAAS/SASL JDK APIs, stay javax.

## Build facts

- Gradle, Kotlin DSL (`*.gradle.kts`), version catalog `gradle/libs.versions.toml`.
- Current: `jetty = 12.1.9`, `jersey = 2.42`, `swagger = 1.6.2`, `jackson = 2.21.3`.
- Convention plugins under `build-logic/conventions/src/main/kotlin/`, incl.
  `pulsar.client-shade-conventions.gradle.kts` (shading rules).

## Subsystem blueprint (from parallel mapping, 2026-05-31)

- **AdditionalServlet SPI already pre-generalized** (commit 39dbbf01a26): `AdditionalServletType` enum
  (only `JAVAX_SERVLET`) + `getServletInstance():Object`. Extension = add `JAKARTA_SERVLET` + route in
  `PulsarService.addBrokerAdditionalServlets` (switch ~L1281) and `ProxyServiceStarter` (switch ~L416):
  `JAVAX_SERVLET`→ee8 `ServletHolder`/`ServletContextHandler`, `JAKARTA_SERVLET`→ee10.
- **Jersey**: only `javax.ws.rs`→`jakarta.ws.rs`; all `org.glassfish.jersey.*` packages unchanged
  (multipart, client, server, servlet, test). `ServletContainer` is `jakarta.servlet` in 3.1 → must run
  in ee10 container (couples Jersey↔Jetty). `jclouds-shaded` already forces `jakarta.ws.rs-api:3.1.0`.
- **Jetty ee8→ee10** for Pulsar's own wiring: `WebService` (broker), `WebServer` (proxy),
  `ProxyServer`+WS handlers (websocket), `WorkerServer` (functions). Key API diffs:
  ee8 `ServletContextHandler.get()` (bridge to core Handler) → ee10 `ServletContextHandler` IS a core
  Handler (drop `.get()`); `org.eclipse.jetty.ee8.nested.Request.getBaseRequest(...)` (WebService
  AddListenerAttributeFilter) has no ee10 equivalent — must port to core Request connector lookup;
  ee8.websocket.api.{Session,WebSocketAdapter,WriteCallback,JettyServerUpgradeResponse} → ee10.websocket.api.
- **Swagger**: annotations ONLY, no runtime bootstrap, no in-repo swagger.json (generated out-of-tree in
  pulsar-site). 61 files. `BaseGenerateDocumentation` reflects over `@ApiModelProperty.value()/name()/required()`
  → must hand-fix to `@Schema.description()/name()/requiredMode()`.
- **Shading** (`pulsar.client-shade-conventions`, `localrun-shaded`): relocations of `javax.{ws,annotation,
  inject,xml.bind,activation,servlet,validation}` → must become `jakarta.*`. Rename checked-in service files
  `pulsar-client-admin-shaded`/`pulsar-client-all` `META-INF/services/org.apache.pulsar.shade.javax.ws.rs.*`
  → `...jakarta.ws.rs.*`.
- **Activation impl gotcha**: `com.sun.activation:jakarta.activation` has no 2.1.3 → use
  `org.eclipse.angus:angus-activation:2.0.2` as the EE10 impl (API stays `jakarta.activation:jakarta.activation-api:2.1.3`).
- **tiered-storage/file-system** pins Jetty 9 (Hadoop MiniDFSCluster) — leave as-is, exempt from lint.

## Execution plan (phased within the single branch)

- **Phase A — core jakarta** (must compile): catalog (jersey 3.1.10, jakarta 3.x/4.x, jetty-ee10 aliases,
  jakarta-servlet, jackson-jakarta-rs, angus-activation, simpleclient_servlet_jakarta); per-module build files
  (ee8→ee10, retain ee8 in broker+proxy for legacy AdditionalServlet; javax-servlet→jakarta-servlet); source
  renames (ws.rs blanket; servlet selective); manual Jetty/Jersey/AdditionalServlet wiring; shading. **Keep
  swagger 1.6.2** in Phase A (decoupled). Compile broker/proxy/websocket/client-admin/functions-worker, fix.
- **Phase B — Swagger 1→2** (workflow, 61 per-file agents + BaseGenerateDocumentation + catalog v3 + shade
  group filters). Compile, fix.
- **Phase C** — Checkstyle import ban + OpenRewrite tooling (Phase-0 deliverable) + LICENSE/NOTICE.
- Push to `forked`, open PR to `lhotari/pulsar`, monitor CI, iterate.

## Compile milestone (2026-05-31)

All **main** source compiles: `pulsar-broker-common`, `pulsar-broker`, `pulsar-proxy`, `pulsar-websocket`,
`pulsar-functions-worker`, `pulsar-client-admin`, `pulsar-client`, `pulsar-broker-auth-sasl`,
`tiered-storage-jcloud`. Remaining: test sources (the reverted ee8.nested mock servers + AdditionalServlet
SPI tests need attention), shaded client jars (verify relocations), Phase B (Swagger), Phase C (import order,
checkstyle lint, LICENSE/NOTICE).

## Local validation (2026-05-31, before first push)

- ✅ All main source compiles (broker, broker-common, proxy, websocket, functions-worker, client-admin,
  client, auth-sasl, jcloud).
- ✅ All changed-module test source compiles.
- ✅ Checkstyle green across changed modules (import order: `jakarta.*` sorts before `java.*`; fixed 175 files).
- ✅ Spotless green.
- ✅ Shaded client jars build (`pulsar-client-shaded`, `pulsar-client-admin-shaded`, `pulsar-client-all`).
- ✅ Runtime tests pass: `BrokerAdditionalServletTest` (web tier + jakarta AdditionalServlet end-to-end),
  `ProducerHandlerTest` (modern Jetty 12 WebSocket API), `AuthenticationFilterTest` (jakarta auth filter).

Pushed to `forked` (lhotari/pulsar); PR opened to exercise full CI. Remaining known follow-ups for the CI loop:
some AdditionalServlet unit tests / `CounterBrokerInterceptor` may need runtime tweaks; Swagger 1→2 (Phase B);
Checkstyle import-ban lint + LICENSE/NOTICE (Phase C).

## CI iteration log

- **Run 1** (`cb35f7ed5bd`): build failed — `jetty-upgrade/*-prometheus-metrics` compile error from the global
  `simpleclient_servlet`→jakarta swap. Reverted that alias to the javax variant (`fa3b0c45f86`).
- **Run 2** (`fa3b0c45f86`): build **compiled** + checkstyle + spotless **passed**; only `checkBinaryLicense`
  failed (server + shell). Updated both `LICENSE.bin.txt` for the jersey-3.1/hk2-3.x/jakarta jar set; also fixed
  `CounterBrokerInterceptor` runtime (jakarta servlet API) (`b3c153c5120`).
- **Run 3** (`b3c153c5120`): `checkBinaryLicense` failed on 2 residual entries — `com.sun.activation:jakarta.activation:1.2.2`
  (the javax.activation JAF impl) is still bundled transitively alongside Angus; my agent prompt wrongly said it was
  replaced, so it was removed. Re-added it to both LICENSE files (`7a93fa09cd4`).
- **Run 4** (`7a93fa09cd4`): **Build and License check job PASSED** ✅ — full multi-module build + checkstyle + spotless
  + binary LICENSE/NOTICE all green. The deterministic build gate is fully cleared. The Pulsar CI **unit + integration
  test matrix** is now running (Broker Groups 1–5, Client Api/Impl, Proxy, Pulsar IO/Client/Metadata, Other, + docker
  images, MacOS build) — hours of runtime; monitoring for failures and iterating.

### Extra local runtime validation (high-risk Jersey 3 / Jetty ee10 areas)
- `FunctionApiV3ResourceTest` (Jersey 3 multipart `FormDataParam`): **71 pass, 0 fail**.
- `AsyncHttpConnectorTest` (custom Jersey Connector SPI + internal `*PropertiesDelegate`): 7 pass; the one
  failure `testShouldStopRetriesWhenTimeoutOccurs` is a **pre-existing timing-flaky test** (`Thread.sleep` +
  WireMock scenario-state race, already carries a flaky-retry analyzer) — unrelated to the namespace migration,
  which does not change the retry/timeout logic. Left for CI flaky-test handling.

## Test-job failures triaged (run `26698057762`)

ALL **integration tests passed** (Standalone, Transaction, Upgrade, Backwards-Compat, Kubernetes, Metrics,
Shade on Java 17/21/25, etc.) and most unit groups passed. Three unit-tier issues found:

1. **Ambiguous URI (FIXED).** `AMBIGUOUS_PATH_SEPARATOR` HTTP 400 for `%2F`-encoded path segments (topic names).
   Jetty 12 ee10's `ServletHandler` rejects ambiguous URIs at the servlet layer independent of the connector's
   `UriCompliance.LEGACY`. Fix: `servletContextHandler.getServletHandler().setDecodeAmbiguousURIs(true)` in the
   broker `WebService`, proxy `WebServer`, and functions `WorkerServer`. (Affected `AdminApiDynamicConfigurationsTest`,
   `PartitionedProducerConsumerTest.testPartitionedTopicNameWithSpecialCharacter`, and similar.)
2. **Athenz `javax.xml.bind` (FIXED).** `NoClassDefFoundError: javax/xml/bind/annotation/XmlElement` — the Athenz
   ZTS client shades `jackson-module-jaxb-annotations`, which needs the `javax.xml.bind` package that the old
   `jakarta.xml.bind-api:2.3.3` happened to ship; the bump to 4.0.2 (real `jakarta.xml.bind`) removed it. Fix: add
   `javax.xml.bind:jaxb-api:2.3.1` (`runtimeOnly`) to `pulsar-client-auth-athenz` + `pulsar-broker-auth-athenz` for
   the third-party transitive need (Pulsar's own code uses jakarta.xml.bind).
3. **Transaction unit tests — server-side request-body over-read (OPEN, documented).** All `TransactionTestBase`
   subclasses fail in `setup` at `admin.clusters().createCluster(...)` with `HTTP 400 Trailing token (START_OBJECT)
   after value bound as ClusterDataImpl ... column 310`. **Definitively root-caused as server-side** (5 ruled-out
   fixes, see below). A `prepareRequest` diagnostic prints `body=309 jerseyCL=null`: the client serialises a single
   **309-byte** body and sets **no** Content-Length header itself, so async-http-client computes `Content-Length: 309`
   from the actual bytes — i.e. the wire request is correct (one 309-byte object, framed at 309). The broker still
   reads **one byte past** (column 310 = a `{`, the START_OBJECT of the *next* pipelined request on the keep-alive
   connection). This is a Jetty 12.1.9 / Jersey-ee10 server-side over-read of the request entity `InputStream` past
   Content-Length, **below the handler layer**. Ruled out (each reverted): (a) no-copy in `prepareRequest`
   (`new ClientRequest`+`writeEntity`); (b) `enableBuffering`; (c) async-http-client `maxRequestRetry(0)`;
   (d) bypassing the Jetty 12.1 `CompressionHandler` (`GzipHandlerUtil`) entirely — still fails, so it is not the
   compression layer; (e) stripping a stale/duplicate `Content-Length` in the header-copy loop — no-op because
   `jerseyCL` is null. **Only the in-process multi-broker UNIT setup trips this — the Transaction *integration* test
   passes**, so production is unaffected. Fix direction for follow-up: this is a Jetty-internals issue (HTTP/1.1
   request-body framing / connection reuse in `org.eclipse.jetty.server.internal.HttpConnection` or the
   ee10 `ServletApiRequest` input stream); candidate remedies are a Jetty patch/version bump (verify against
   12.1.10+), forcing `Connection: close` for admin writes in the test path, or wrapping the ee10 servlet input
   stream to hard-stop at Content-Length. Validate against `TransactionStablePositionTest`.

4. **Tiered-storage offloader unit tests — legacy javax APIs dropped from the classpath (FIXED).**
   `BlobStoreBackedInputStreamTest` (jcloud) and `FileSystemManagedLedgerOffloaderTest` (file-system) failed in
   `start` with `NoClassDefFoundError` for `javax.xml.bind.JAXBException`, `javax.ws.rs.ext.ExceptionMapper`,
   `javax.annotation.Priority`, and `javax.validation.Validator`. jclouds 2.6.0 and Hadoop are pre-jakarta libraries
   that reference the legacy `javax.*` EE APIs which the migration removed from the transitive classpath. Fixes:
   - **jcloud:** `runtimeOnly(libs.jaxb.api)` (`javax.xml.bind:jaxb-api:2.3.1`) — jclouds' `ContextBuilder.build()`
     references `javax.xml.bind`. (Production + test path, hence `runtimeOnly`, not test-only.)
   - **file-system:** Hadoop's `MiniDFSCluster` NameNode web UI is a **fully-javax** stack (already forced to Jetty 9
     in this module). The migration force-upgraded its `jersey-*:2.46`→`3.1.10` (jakarta), so its
     `org.glassfish.jersey.servlet.ServletContainer` stopped being a `javax.servlet.Servlet`
     (`UnavailableException`). Pinned the legacy stack **for test configs only** (mirrors the existing Jetty-9 force):
     Jersey `2.46` (client/common/server/container-servlet/container-servlet-core/jersey-hk2) + hk2 `2.6.1`
     (api/locator/utils/aopalliance-repackaged/jakarta.inject), plus `testRuntimeOnly` legacy
     `javax.ws.rs:javax.ws.rs-api:2.1.1`, `javax.annotation:javax.annotation-api:1.3.2`,
     `javax.validation:validation-api:2.0.1.Final`, and `runtimeOnly(libs.jaxb.api)` (Hadoop common needs
     `javax.xml.bind` at runtime). All `FileSystemManagedLedgerOffloaderTest` (3) + jcloud tests pass locally.
     Production offloaders are NARs that bundle their own deps and use HDFS RPC (no web UI), so they are unaffected.

## Decisions log

- **D1 (swagger sequencing):** Swagger 1→2 deferred to Phase B (separate commits) so the javax→jakarta core
  validates independently. Swagger is `io.swagger` (not javax), compile-only + doc-only, fully decoupled.
- **D2 (filters / dual env):** Pulsar's own filter chain (AuthenticationFilter, RateLimitingFilter, etc.)
  migrates to jakarta.servlet (ee10). Legacy javax AdditionalServlet (ee8) path filter handling decided after
  inspecting the AdditionalServlet tests (auth coverage). [resolve during wiring]
- **D3 (activation):** impl → `org.eclipse.angus:angus-activation` (EE10).
- **D4 (websocket endpoints stay ee8):** Jetty 12's ee10 websocket environment exposes only the *redesigned*
  Jetty 12 WebSocket API (`org.eclipse.jetty.websocket.api.Session.Listener`, `Callback`); the legacy
  `WebSocketAdapter`/`WriteCallback`/`RemoteEndpoint`/`Session.getRemote()` API that Pulsar's WS handlers are
  written against exists **only** in the ee8 environment (`org.eclipse.jetty.ee8.websocket.api.*`). Rewriting
  the handlers to the modern API is behaviorally sensitive, so the WS **endpoint layer** (the 4 `WebSocket*Servlet`
  classes + `AbstractWebSocketHandler`/`Consumer`/`Producer`/`Reader`/`MultiTopicConsumerHandler` +
  `WebSocketHttpServletRequestWrapper`) is **kept on ee8 + javax.servlet**, registered via `addWebSocketServlet`
  into an ee8 Jetty context. The WS **REST/admin tier** (`WebSocketWebResource`, `WebSocketProxyStats*`) still
  moves to jakarta.ws.rs/Jersey 3 (ee10). Both run in the same Jetty 12 server (multi-environment). Modernizing
  the WS endpoint API to Jetty 12's native API is a separate follow-up. WS wire protocol/behavior unchanged.
- **D4 REVISED:** The ee8-endpoint approach was abandoned: the WS handlers call the **shared jakarta auth SPI**
  (`AuthenticationService`/`AuthenticationProvider.newHttpAuthState`/`AuthenticationDataHttps`, also used by the
  ee10 REST `AuthenticationFilter`), which takes a `jakarta.servlet.http.HttpServletRequest`. An ee8 (javax)
  WS upgrade request can't be passed to it, and the auth SPI must stay jakarta because the REST tier is jakarta.
  So the WS **endpoints are migrated to ee10 + the modern Jetty 12 WebSocket API**
  (`org.eclipse.jetty.websocket.api.Session.Listener` / `Callback`): `WebSocketAdapter`→`Session.Listener`
  (store Session in `onWebSocketOpen`), `getRemote().sendString(s, WriteCallback)`→`session.sendText(s, Callback)`,
  `sendPing(buf)`→`session.sendPing(buf, Callback)`, `getRemoteAddress()`→`session.getRemoteSocketAddress()`,
  `close(code,reason)`→`session.close(code,reason,Callback)`. WS wire protocol unchanged.
