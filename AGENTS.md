# AGENTS.md

This file provides guidance to AI coding agents working with code in this repository.

Apache Pulsar is a distributed pub-sub messaging and streaming platform. The codebase is performance-critical, heavily asynchronous, and concurrency-sensitive (brokers, storage, networking). Prioritize correctness, thread safety, performance, maintainability, and backward compatibility.

## Build system

The build is **Gradle** (migrated from Maven via PIP-463; much existing tooling and docs elsewhere still reference Maven). Use the wrapper `./gradlew` — no separate Gradle install needed. **JDK 21 or 25 is required to run the build** (`-PskipJavaVersionCheck` bypasses the check). Bytecode targets Java 17 (`options.release = 17`).

Key build infrastructure:
- `settings.gradle.kts` — all modules, organized in dependency tiers (Tier 0 has no internal deps, higher tiers build on lower ones).
- `build-logic/conventions/` — convention plugins (`pulsar.java-conventions`, `pulsar.code-quality-conventions`, `pulsar.shadow-conventions`, etc.) applied by modules. This is where shared compile/test/dependency config lives — edit conventions here rather than duplicating config across modules.
- `gradle/libs.versions.toml` — version catalog (single source of truth for dependency versions; `libs.*` references in build scripts).
- `pulsar-dependencies` — enforced platform (BOM) pinning all dependency versions; applied to every module.

When changing the build, **follow the [Gradle best practices](https://docs.gradle.org/current/userguide/best_practices_index.html)**. The build enables both the **configuration cache** (`org.gradle.configuration-cache=true`) and **configure-on-demand** (`org.gradle.configureondemand=true`), so any task used by a commonly-run task (`assemble`, `test`, `integrationTest`, `rat`/`spotlessCheck`/`checkstyle*`, `checkBinaryLicense`, `docker*`, etc.) **must be configuration-cache and configure-on-demand compatible** — no reading of mutable state at execution time, no `Project` access in task actions, use `Provider`/value sources instead. Verify with `--configuration-cache`. Tooling/one-off tasks that are not part of these common flows (e.g. `verifyTestGroups`, dependency-report and ad-hoc maintenance tasks) may be exempt.

### Module name vs. directory name gotcha

Several Gradle project paths do **not** match their directory because the Maven artifactId is preserved. Most importantly:
- Directory `pulsar-client/` → project **`:pulsar-client-original`**
- Directory `pulsar-client-admin/` → project **`:pulsar-client-admin-original`**
- Directory `pulsar-functions/localrun/` → project `:pulsar-functions:pulsar-functions-local-runner-original`

Always use the Gradle project path (left of any `--tests`), e.g. `./gradlew :pulsar-client-original:test`. Check `settings.gradle.kts` when a path is ambiguous.

## Common commands

```bash
# Compile and assemble everything (or a single module)
./gradlew assemble
./gradlew :pulsar-broker:assemble

# Lint / verify (license headers, formatting, checkstyle) — run before pushing
./gradlew rat spotlessCheck checkstyleMain checkstyleTest
./gradlew spotlessApply            # auto-fix license headers/formatting

# Verify bundled-dependency LICENSE/NOTICE coverage (run after changing a runtime dependency)
./gradlew checkBinaryLicense

# Always scope test runs with --tests — running a whole module's test task is slow.
# Run a single test class
./gradlew :pulsar-client-original:test --tests "ConsumerBuilderImplTest"
# Run a single test method
./gradlew :pulsar-client-original:test --tests "ConsumerBuilderImplTest.<methodName>"
# Run all tests in a specific package
./gradlew :pulsar-broker:test --tests "org.apache.pulsar.broker.admin.*"
```

### Test groups (TestNG)

Tests use **TestNG** and are tagged with `@Test(groups = "...")`. By default the build **excludes the `quarantine` and `flaky` groups** (`excludedTestGroups` default = `quarantine,flaky`), so to run a single test that lives in one of those groups you must clear the exclusion:

```bash
# Run a specific test that is in the flaky/quarantine group (otherwise excluded by default)
./gradlew :pulsar-broker:test -PexcludedTestGroups='' --tests "<SomeFlakyTest>"
```

CI selects whole groups with `-PtestGroups=<groups>` and `-PexcludedTestGroups=<groups>` (e.g. `broker,broker-admin`); locally prefer `--tests` to scope to specific classes instead of running an entire group. CI splits `pulsar-broker` tests into groups (see `pulsar-build/run_unit_group_gradle.sh` and `gradle/verify-test-groups.gradle.kts`). Tests with no group are treated as `other` at runtime. `./gradlew verifyTestGroups` reports group assignments and flags tests not covered by any CI group.

Other test-related properties: `-PtestJavaVersion=17` (run tests on a different JDK toolchain), `-PtestRetryCount=N`, `-PtestFailFast=true|false`, `-PprotobufVersion=4.31.1` (protobuf v4 compatibility tests).

Failed tests are retried once by default (`testRetryCount=1`; `0` when running inside the IDE). When running tests locally, prefer **`-PtestRetryCount=0`** to catch failures (including flakiness) early instead of having retries mask them.

### Integration tests

Integration tests live in `tests/` (see `tests/README.md`). They use [Testcontainers](https://www.testcontainers.org/) to bring up Pulsar services in Docker, so **Docker must be installed and running**. Build the test image first, then run the tests.

The full integration suite is heavy and slow. **In local development, always run individual integration tests** rather than the whole suite — pass `--tests` to select a class (TestNG then discovers it directly from the classpath):

```bash
./gradlew :tests:latest-version-image:dockerBuild     # build the docker test image
./gradlew :tests:integration:integrationTest --tests "org.apache.pulsar.tests.integration.<TestClass>"
```

To run the **entire** integration test set, use **Personal CI** (see below) rather than running it locally. (`integrationTest` also accepts `-PtestGroups`/`-PexcludedTestGroups` and `-PintegrationTestSuiteFile=<suite>.xml` to pick a specific TestNG suite.)

### Running the full CI pipeline (Personal CI)

The full test suite is large and slow to run locally. While iterating on a change, run only the narrowly-scoped tests relevant to the change (a single test class or package, see above) rather than a module's entire test task. To validate a larger change against the **full** CI pipeline, do not run everything locally — use **Personal CI**, which runs Pulsar's CI workflows in the contributor's own GitHub fork.

If Personal CI is not yet set up, **ask the user to follow** the [Personal CI documentation](https://pulsar.apache.org/contribute/personal-ci/) to enable it on their fork. Once it is set up, the agent can drive the loop:

1. Keep the local `master` up-to-date with `apache/pulsar` and rebase the feature branch on it.
2. Push the feature branch to the **fork** to trigger CI runs there. CI runs against the PR opened in the user's own fork (it is normal to have a PR open in the fork *and* a PR for the same branch open in `apache/pulsar` at the same time).
3. Monitor CI status on the fork and fix failures.
4. Open the PR to `apache/pulsar` only after the fork's CI is green.

Once the PR to `apache/pulsar` has been opened, stop rebasing as part of this loop: step 1's rebase no longer applies — bring in upstream changes by merging `apache/pulsar`'s `master` instead (see the Pull requests section below).

## Architecture (big picture)

Pulsar separates a stateless serving layer (brokers) from durable storage (Apache BookKeeper) and a metadata store (ZooKeeper/etcd/others). The modules layer accordingly:

- **`pulsar-client-api`, `pulsar-client-admin-api`** — public, backward-compatible interfaces only. `pulsar-client-api-v5`/`pulsar-client-v5` are the newer V5 client API (PIP-466/468).
- **`pulsar-client` (`:pulsar-client-original`)** — the Java client implementation (producer/consumer/reader, connection pooling). `pulsar-client-admin` implements the admin REST client.
- **`pulsar-common`** — wire protocol and shared types. Protobuf / lightproto messages are **generated** into `generated-lightproto/` / `generated-sources/` (excluded from checkstyle and spotless).
- **`pulsar-metadata`** — pluggable metadata store abstraction (ZooKeeper, etcd, RocksDB, memory) used by broker and bookkeeper.
- **`managed-ledger`** — the storage abstraction over **Apache BookKeeper**: append-only ledgers + cursors that track consumer/subscription positions. This is the durability layer the broker reads/writes through.
- **`pulsar-broker`** — the server. `PulsarService` is the composition root wiring everything together; `BrokerService` manages topics, subscriptions, and client connections. Entry points: `PulsarBrokerStarter` (broker), `PulsarStandalone`/`PulsarStandaloneStarter` (all-in-one), `PulsarClusterMetadataSetup` (cluster init).
- **`pulsar-proxy`** — optional proxy/gateway in front of brokers.
- **`pulsar-functions/*`** — serverless compute (Functions): `proto`, `api-java`, `instance`, `runtime`, `worker`, `localrun`.
- **`pulsar-io/*`** — connector framework core only; most built-in connectors were moved to the separate `pulsar-connectors` repo (PIP-465).
- **`pulsar-transaction/*`** — transaction coordinator and common types.
- **`tiered-storage/*`, `offloaders/`** — offload ledger data to cloud/filesystem storage.
- **`pulsar-websocket`** — WebSocket-to-Pulsar bridge. **`pulsar-client-tools`** — the `pulsar-admin`/`pulsar-client` CLIs.
- **Shaded/distribution** — `pulsar-client-shaded`, `pulsar-client-all`, `pulsar-client-admin-shaded` produce relocated fat jars; `distribution/*` assembles server/shell/offloader tarballs.

The **`pip/`** directory holds **Pulsar Improvement Proposals** (`pip-<N>.md`) — the design documents for significant changes, referenced as `PIP-<N>` throughout commit messages and code (e.g. PIP-463 = Maven→Gradle migration, PIP-466/468 = V5 client). `pip/README.md` describes the process and `pip/TEMPLATE.md` is the proposal template. Consult the relevant PIP for the rationale behind a non-trivial feature or architectural decision.

## Coding conventions

Full conventions for code review are in `.github/copilot-instructions.md`. Highlights to apply when writing code:

- **Style**: 4-space indent, never tabs; always use curly braces (even single-line `if`); no `@author` Javadoc tags. Every `TODO` must reference a GitHub issue (`// TODO: https://github.com/apache/pulsar/issues/XXXX`). Checkstyle config: `buildtools/src/main/resources/pulsar/checkstyle.xml`. Lombok is enabled.
- **Async**: heavy use of `CompletableFuture`. Methods returning `CompletableFuture` must **not** throw synchronously — return `CompletableFuture.failedFuture(e)` (including for argument validation). Never block (`Thread.sleep`, `Future.get()`, `.join()`, blocking IO) on event-loop / async-execution threads. Avoid nested futures; flatten with `thenCompose`. Prefer `OrderedExecutor` for ordered async work.
- **Concurrency**: public classes should be thread-safe; annotate non-thread-safe ones with `@NotThreadSafe`. Give threads meaningful names.
- **Logging**: prefer the [slog](https://github.com/merlimat/slog) library (`io.github.merlimat.slog`); instantiate the logger with Lombok's **`@CustomLog`** annotation (wired in `lombok.config` to `io.github.merlimat.slog.Logger.get(TYPE)`). SLF4J is still available but is **considered deprecated** for Pulsar logging — don't add new SLF4J loggers. Never use `System.out`/`System.err`. Assume INFO in production. **Default new logs to TRACE or DEBUG, not INFO** — existing Pulsar code overuses INFO, which makes production logs excessively noisy. Use **TRACE** for fine-grained/low-level detail, **DEBUG** for diagnostics that could reasonably be enabled in production without flooding the logs, and reserve **INFO** for significant lifecycle/state-change events that are low-frequency. With slog, attach data as **structured attributes** (`log.info().attr("topic", topic).log("Published")`) rather than interpolating it into the message string. For expensive values, don't guard with `isDebugEnabled()`/`isTraceEnabled()` — instead pass a supplier lambda for the attribute value (`.attr("dump", () -> expensiveDump())`) or use the deferred form (`log.debug(e -> e.attr(...).log("msg"))`), both of which run only when the level is enabled. Avoid logging in hot paths and stack traces at INFO or lower.
- **Networking/memory**: prefer Netty `ByteBuf` over `ByteBuffer` on internal paths; always close streams/connections/executors/buffers.
- **Dependencies**: prefer existing libraries (Guava/Commons, FastUtil, JCTools, RoaringBitmap, Caffeine, Jackson, Netty, OpenTelemetry). New dependencies must be justified and have LICENSE/NOTICE updated (verify with `checkBinaryLicense`).
- **Backward compatibility**: do not break public APIs, client compatibility, wire protocol, or serialized/metadata formats. Servers must work with both older and newer clients.

## Testing conventions

- TestNG + Mockito. Prefer **AssertJ** assertions with descriptions over TestNG asserts; use **Awaitility** (with AssertJ) for async conditions instead of `sleep`-based timing. Add timeouts to prevent hangs.
- **No reflection to access private state in tests** (`WhiteboxImpl.getInternalState/setInternalState`, `setAccessible(true)`, etc.). Instead expose a **package-private** `@VisibleForTesting` accessor and place the test in the same package. New reflection-based test access should be flagged in review.
- Every feature/bugfix should include tests covering edge cases and failure scenarios; they must be deterministic.
- **Close/release resources in tests** — shut down executors/clients/services and release Netty `ByteBuf`s (and other ref-counted buffers) that the test allocates.
  - A reported **`ByteBuf`/buffer leak** (Netty pooled-allocator leak detection, enabled via `-Dpulsar.allocator.pooled=true`) is a **real bug**: fix the root cause — the missing `release()` in the code under test — rather than working around it in the test or suppressing the detector.
  - **Thread leaks** reported by `ThreadLeakDetectorListener` are currently **not a reliable signal**: it produces a high rate of false positives — notably with `SharedPulsarBaseTest`, and whenever `THREAD_LEAK_DETECTOR_WAIT_MILLIS` is not set high enough (≈`10000` is recommended) for asynchronous shutdown to complete before the check runs. That wait setting only takes effect with the **Gradle daemon disabled** (`--no-daemon`). Because of the false positives, do not rely on `ThreadLeakDetectorListener` alone to conclude a change is thread-leak-free — corroborate before treating a reported thread leak as a real bug.

## Pull requests

PRs must follow `.github/PULL_REQUEST_TEMPLATE.md`. PR titles follow the `[<type>][<optional scope>] <description>` convention (e.g. `[fix][broker] ...`, `[improve][build] ...`) — refer to `.github/workflows/ci-semantic-pull-request.yml` for the valid `[type]` and `[scope]` prefixes, which are enforced by CI. The `<description>` should be in imperative form, like a good commit message's subject line.

**Never rebase a PR branch once the PR is opened in `apache/pulsar`.** Rebasing rewrites history and disrupts reviewers (it invalidates review comments and makes incremental diffs unreadable). To bring in upstream changes, instead fetch from the `apache/pulsar` remote and **merge** its `master` into the PR branch:

```bash
git fetch <apache-pulsar-remote>          # e.g. `upstream` or `apache`
git merge <apache-pulsar-remote>/master
```

(Rebasing onto an updated `master` is fine *before* the PR is opened — see the Personal CI loop above — but not after.)

## Reporting security vulnerabilities

Please refer to https://pulsar.apache.org/security/. See https://www.apache.org/security/ for the general ASF policy.

For already-public CVEs where you want to check Pulsar's exposure, the right venue is a GitHub issue on apache/pulsar or a question on the dev@pulsar.apache.org mailing list. Before asking about an already-public CVE and whether it's already fixed in Pulsar, search PRs and issues with the CVE id at https://github.com/apache/pulsar/pulls (also check issues and closed PRs/issues). Pulsar project's supported versions are available at https://pulsar.apache.org/contribute/release-policy/. Users should upgrade to supported versions to receive security updates.
