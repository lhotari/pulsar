# Architecture

Apache Pulsar is a distributed pub-sub messaging and streaming platform. The codebase is
performance-critical, heavily asynchronous, and concurrency-sensitive (brokers, storage,
networking). The authoritative documentation lives at <https://pulsar.apache.org> — see the
[Architecture Overview](https://pulsar.apache.org/docs/4.2.x/concepts-architecture-overview/) for the
conceptual model. For a deeper, generated architecture description see
[DeepWiki](https://deepwiki.com/apache/pulsar); coding agents can install the
[DeepWiki MCP](https://docs.devin.ai/work-with-devin/deepwiki-mcp) for richer coverage of Pulsar's
architecture. This file is a map of the repository for contributors (and AI coding agents) who need to
find their way around the modules quickly.

## Big picture

Pulsar separates a **stateless serving layer** (brokers) from **durable storage** (Apache
BookKeeper) and a **metadata store** (Oxia / ZooKeeper). The Gradle modules layer
accordingly:

- **`pulsar-client-api`, `pulsar-client-admin-api`** — public, backward-compatible interfaces only.
  `pulsar-client-api-v5` / `pulsar-client-v5` are the newer V5 client API (PIP-466/468).
- **`pulsar-client` (`:pulsar-client-original`)** — the Java client implementation
  (producer/consumer/reader, connection pooling). `pulsar-client-admin` implements the admin REST
  client.
- **`pulsar-common`** — wire protocol and shared types. Protobuf / lightproto messages are
  **generated** into `generated-lightproto/` / `generated-sources/` (excluded from checkstyle and
  spotless).
- **`pulsar-metadata`** — pluggable metadata store abstraction (Oxia / ZooKeeper, plus RocksDB and memory)
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

## Build and module layout

The Gradle build infrastructure — convention plugins under `build-logic/`, the
`gradle/libs.versions.toml` version catalog, the enforced `pulsar-dependencies` platform, the
dependency tiers in `settings.gradle.kts`, and the configuration-cache / configure-on-demand
requirements — is documented in [`BUILDING.md`](BUILDING.md), together with the
[module-name-vs-directory gotcha](BUILDING.md#module-name-vs-directory-name-gotcha) (e.g. directory
`pulsar-client/` is the Gradle project `:pulsar-client-original`).

## Pulsar Improvement Proposals (`pip/`)

The **`pip/`** directory holds **Pulsar Improvement Proposals** (`pip-<N>.md`) — the design
documents for significant changes, referenced as `PIP-<N>` throughout commit messages and code (e.g.
PIP-463 = Maven→Gradle migration, PIP-465 = IO connectors moved out, PIP-466/468 = V5 client).
`pip/README.md` describes the process and `pip/TEMPLATE.md` is the proposal template. Consult the
relevant PIP for the rationale behind a non-trivial feature or architectural decision. A PIP **number
is reserved by the first `dev@pulsar.apache.org` thread that uses it** — start the discussion to claim
the next free number.
