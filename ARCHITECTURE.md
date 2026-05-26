# Architecture

Apache Pulsar is a distributed pub-sub messaging and streaming platform. The codebase is
performance-critical, heavily asynchronous, and concurrency-sensitive (brokers, storage,
networking). The authoritative documentation lives at <https://pulsar.apache.org>; this file is a
map of the repository for contributors (and AI coding agents) who need to find their way around the
modules quickly.

## Big picture

Pulsar separates a **stateless serving layer** (brokers) from **durable storage** (Apache
BookKeeper) and a **metadata store** (ZooKeeper / etcd / others). The Gradle modules layer
accordingly:

- **`pulsar-client-api`, `pulsar-client-admin-api`** — public, backward-compatible interfaces only.
  `pulsar-client-api-v5` / `pulsar-client-v5` are the newer V5 client API (PIP-466/468).
- **`pulsar-client` (`:pulsar-client-original`)** — the Java client implementation
  (producer/consumer/reader, connection pooling). `pulsar-client-admin` implements the admin REST
  client.
- **`pulsar-common`** — wire protocol and shared types. Protobuf / lightproto messages are
  **generated** into `generated-lightproto/` / `generated-sources/` (excluded from checkstyle and
  spotless).
- **`pulsar-metadata`** — pluggable metadata store abstraction (ZooKeeper, etcd, RocksDB, memory)
  used by broker and bookkeeper.
- **`managed-ledger`** — the storage abstraction over **Apache BookKeeper**: append-only ledgers +
  cursors that track consumer/subscription positions. This is the durability layer the broker reads
  and writes through.
- **`pulsar-broker`** — the server. `PulsarService` is the composition root wiring everything
  together; `BrokerService` manages topics, subscriptions, and client connections. Entry points:
  `PulsarBrokerStarter` (broker), `PulsarStandalone` / `PulsarStandaloneStarter` (all-in-one),
  `PulsarClusterMetadataSetup` (cluster init).
- **`pulsar-proxy`** — optional proxy/gateway in front of brokers.
- **`pulsar-functions/*`** — serverless compute (Functions): `proto`, `api-java`, `instance`,
  `runtime`, `worker`, `localrun`.
- **`pulsar-io/*`** — connector framework core only; most built-in connectors were moved to the
  separate `pulsar-connectors` repo (PIP-465).
- **`pulsar-transaction/*`** — transaction coordinator and common types.
- **`tiered-storage/*`, `offloaders/`** — offload ledger data to cloud/filesystem storage.
- **`pulsar-websocket`** — WebSocket-to-Pulsar bridge. **`pulsar-client-tools`** — the
  `pulsar-admin` / `pulsar-client` CLIs.
- **Shaded / distribution** — `pulsar-client-shaded`, `pulsar-client-all`,
  `pulsar-client-admin-shaded` produce relocated fat jars; `distribution/*` assembles
  server/shell/offloader tarballs.

## Concurrency model (a known gap)

Pulsar does **not** have a clearly established, documented concurrency model, which makes it hard to
evaluate whether a given piece of code is correct by construction. (Contrast Netty, which has a clear
rule: all handling on the IO thread is non-blocking, which by extension means avoiding synchronization
and locks on that path.) Pulsar does not strictly follow such a rule; modern JVMs and hardware
optimize `synchronized` code well enough that this has not blocked high performance, but it does make
reasoning about correctness harder than it needs to be.

Conventions that **should** be documented (and largely are not yet):

- which work belongs on the network-connection **event loop** vs. other threads;
- how the various **thread pools** are intended to be used, and what kind of work belongs on each;
- how threads are expected to **hand off state** to each other;
- when a `CompletableFuture`'s **completion thread should be switched** to another thread, and which one;
- **concurrency limits** for asynchronous tasks;
- preferring the **single-writer principle** to avoid concurrent state mutation.

Until such a model is written down, follow the surrounding code's conventions and the Java-Memory-Model
rules in [`CODING.md`](CODING.md#concurrency). Once a model is defined, it becomes far more tractable to
"lift and shift" existing code toward it and enforce the rules consistently rather than having each
contributor rediscover the conventions case by case.

## Backpressure

Closely tied to the concurrency model is **backpressure** — how the system avoids accepting more work
than it can handle, particularly with respect to memory. The memory side is described in
[PIP-442 "Existing Broker Memory Management"](pip/pip-442.md#existing-broker-memory-management). Broader
backpressure (beyond memory) is not yet documented and would benefit from being defined alongside the
concurrency model.

## Build infrastructure

The build is **Gradle** (migrated from Maven via PIP-463; much existing tooling and docs elsewhere
still reference Maven). Use the wrapper `./gradlew` — no separate Gradle install needed. **JDK 21 or
25 is required to run the build** (`-PskipJavaVersionCheck` bypasses the check). Bytecode targets
Java 17 (`options.release = 17`).

- `settings.gradle.kts` — all modules, organized in dependency tiers (Tier 0 has no internal deps,
  higher tiers build on lower ones).
- `build-logic/conventions/` — convention plugins (`pulsar.java-conventions`,
  `pulsar.code-quality-conventions`, `pulsar.shadow-conventions`, etc.) applied by modules. This is
  where shared compile/test/dependency config lives — edit conventions here rather than duplicating
  config across modules.
- `gradle/libs.versions.toml` — version catalog (single source of truth for dependency versions;
  `libs.*` references in build scripts).
- `pulsar-dependencies` — enforced platform (BOM) pinning all dependency versions; applied to every
  module.

When changing the build, **follow the [Gradle best practices](https://docs.gradle.org/current/userguide/best_practices_index.html)**.
The build enables both the **configuration cache** (`org.gradle.configuration-cache=true`) and
**configure-on-demand** (`org.gradle.configureondemand=true`), so any task used by a commonly-run
task (`assemble`, `test`, `integrationTest`, `rat` / `spotlessCheck` / `checkstyle*`,
`checkBinaryLicense`, `docker*`, etc.) **must be configuration-cache and configure-on-demand
compatible** — no reading of mutable state at execution time, no `Project` access in task actions,
use `Provider` / value sources instead. Verify with `--configuration-cache`. Tooling/one-off tasks
that are not part of these common flows (e.g. `verifyTestGroups`, dependency-report and ad-hoc
maintenance tasks) may be exempt.

### Module name vs. directory name gotcha

Several Gradle project paths do **not** match their directory because the Maven artifactId is
preserved. Most importantly:

- Directory `pulsar-client/` → project **`:pulsar-client-original`**
- Directory `pulsar-client-admin/` → project **`:pulsar-client-admin-original`**
- Directory `pulsar-functions/localrun/` → project `:pulsar-functions:pulsar-functions-local-runner-original`

Always use the Gradle project path (left of any `--tests`), e.g. `./gradlew :pulsar-client-original:test`.
Check `settings.gradle.kts` when a path is ambiguous.

## Pulsar Improvement Proposals (`pip/`)

The **`pip/`** directory holds **Pulsar Improvement Proposals** (`pip-<N>.md`) — the design
documents for significant changes, referenced as `PIP-<N>` throughout commit messages and code (e.g.
PIP-463 = Maven→Gradle migration, PIP-465 = IO connectors moved out, PIP-466/468 = V5 client).
`pip/README.md` describes the process and `pip/TEMPLATE.md` is the proposal template. Consult the
relevant PIP for the rationale behind a non-trivial feature or architectural decision.
