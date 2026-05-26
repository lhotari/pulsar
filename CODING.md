# Coding guidelines

Apache Pulsar follows the Sun Java Coding Conventions with additional project-specific rules. The
codebase is performance-critical, asynchronous, and concurrency-sensitive, so code review
prioritizes **correctness, thread safety, performance, maintainability, and backward
compatibility**. This file is the canonical coding reference for both human contributors and AI
coding agents; the task skills under [`.agents/skills/`](.agents/skills/) layer AI-specific
guardrails on top of it.

## Style

- **4 spaces** for indentation; **tabs must never be used**.
- Always use **curly braces**, even for single-line `if` statements.
- Do **not** include `@author` tags in Javadoc.
- Every `TODO` must reference a GitHub issue, e.g. `// TODO: https://github.com/apache/pulsar/issues/XXXX`.
- Checkstyle config: `buildtools/src/main/resources/pulsar/checkstyle.xml`. Lombok is enabled.

## Data types

- **Don't return generic tuples.** Instead of returning `org.apache.commons.lang3.tuple.Pair<L, R>`
  (or a similar generic tuple type), define a small, purpose-named **Java `record`** inline in the
  class that declares the method, with field names that document what the values mean. Give the record
  the **same visibility as the method** that returns it — `public`, package-private (default), or
  `private`.
- **Prefer record keys over concatenated strings.** For a composite `Map` key, use a small `record`
  holding the key components instead of concatenating a `java.lang.String` (e.g. `a + ":" + b`).
  A record key gives a correct `equals`/`hashCode`, keeps the key type-safe, and avoids
  delimiter/escaping bugs.

## Asynchronous programming

Pulsar relies heavily on `CompletableFuture` and asynchronous pipelines. Prefer `CompletableFuture`
APIs over `ListenableFuture` for new code.

- **Methods returning `CompletableFuture` must not throw synchronously.** Propagate failures through
  the returned future — `return CompletableFuture.failedFuture(e);` — including for argument
  validation:

  ```java
  if (arg == null) {
      return CompletableFuture.failedFuture(new IllegalArgumentException("arg"));
  }
  ```

  Throwing inside an async stage (`thenApply`, `thenCompose`, `thenRun`, `handle`, `whenComplete`)
  is fine, because the framework routes it into the resulting future.
- **Never block on event-loop / async-execution threads** — no `Thread.sleep`, `Future.get()`,
  `CompletableFuture.join()`, or blocking IO.
- **Avoid nested futures** such as `CompletableFuture<CompletableFuture<T>>`; flatten with
  `thenCompose`.
- Prefer **`OrderedExecutor`** for ordered asynchronous work.

## Concurrency

- Public classes should be **thread-safe**; annotate non-thread-safe ones with `@NotThreadSafe`.
- Protect shared mutable state; prefer fine-grained synchronization; ensure mutations occur on the
  intended thread. Prefer the **single-writer principle** — design so a given piece of state is
  mutated by only one thread — to avoid concurrent mutation entirely.
- Give threads **meaningful names** for diagnostics.
- When creating thread pools, prefer Netty's **`io.netty.util.concurrent.DefaultThreadFactory`**. It
  produces **`FastThreadLocalThread`** instances, on which `FastThreadLocal` lookups are much faster
  than on a plain `Thread` — this matters on Netty-heavy paths (e.g. the pooled `ByteBuf` allocator and
  other Netty internals that rely on `FastThreadLocal`) — and it also assigns meaningful, prefixed
  thread names.

Note that Pulsar does not yet have a documented, project-wide concurrency model; see
[`ARCHITECTURE.md` → Concurrency model](ARCHITECTURE.md#concurrency-model-a-known-gap) for the
conventions that *should* govern threads, thread pools, and event loops.

### The Java Memory Model is what makes concurrent code correct

Historically, several hard-to-investigate Pulsar bugs have come from misconceptions about Java
synchronization:

- **A `synchronized` method or block is not, on its own, thread-safe.** `synchronized` provides its
  visibility and ordering guarantees only when the **same monitor/lock guards both the reads and the
  writes** of the shared state. Synchronizing on an arbitrary monitor while another thread accesses
  the same field without that monitor guarantees nothing.
- On 64-bit JVMs a field's value is **never corrupted** — a read always returns some value that was
  actually written. What goes wrong is **visibility**: without a happens-before relationship,
  different threads can observe different values, or a thread may never see an updated value at all.
  Establish happens-before with `synchronized`, `volatile`, `final`, or `java.util.concurrent`
  constructs.
- **A field accessed by more than one thread needs explicit visibility.** If multiple threads read
  and write a field, make it `volatile` (or guard every access — both reads and writes — with the
  same lock); otherwise a writing thread's update may never become visible to a reading thread.
  `volatile` gives visibility for that single field but does **not** make compound updates
  (read-modify-write, check-then-act) atomic — use `java.util.concurrent` atomics/locks for those.
- Visibility is per-field, so a mutable object can be observed **partially updated** — some field
  writes visible to a reader, others not.
- The only way to make concurrent code reliably correct is to **conform to the Java Memory Model**.
  **Benign data races** are sometimes acceptable, and Pulsar has code that relies on this by design —
  but that must be a deliberate, documented choice, not an accident.
- **Prefer immutable objects** to sidestep these visibility and mutation hazards. Two distinct notions:
  - An object is **immutable** when all its fields are `final` *and* every nested instance it
    references is itself immutable (a Java `record` is the common case). Immutability must hold for the
    whole reachable graph — synchronization around a holder buys nothing if a referenced object can
    still be mutated independently without its own synchronization.
  - An object is **effectively immutable** when its state is never modified after construction but its
    fields are not all `final`.
- **How they must be published differs:**
  - An **immutable** object benefits from the JMM's final-field **safe initialization** guarantee — a
    thread that observes the reference sees the fully-constructed state even when the reference was
    published through a data race — so it needs **no** safe publication.
  - An **effectively immutable** object does **not** get that guarantee and must be shared via **safe
    publication**: a `final` or `volatile` field, or a `java.util.concurrent` construct (e.g. putting
    it into a `ConcurrentHashMap`).

  See [Safe initialization](https://shipilev.net/blog/2014/safe-public-construction/#_safe_initialization).

### Reproducing concurrency / memory-visibility bugs

These bugs are timing- and platform-dependent and easily masked, so a clean run is weak evidence that
a concurrency fix is correct:

- Interpreted and JIT-compiled code can behave very differently. Reproductions often need several
  **warm-up rounds with a short pause** between them so the JIT compiler kicks in; the JIT is tiered,
  asynchronous, and multi-threshold, so a short-running test may never trigger compilation. JVM flags
  can force earlier compilation, and which execution paths are exercised affects what gets compiled.
- Some races surface only on specific **hardware/OS** combinations — classically **multi-socket /
  multi-NUMA** machines, whose weaker cross-socket memory ordering exposes races that never appear on
  a single-socket machine.

## Logging

- Prefer the **[slog](https://github.com/merlimat/slog)** library (`io.github.merlimat.slog`).
  Instantiate the logger with Lombok's **`@CustomLog`** annotation, which is wired in `lombok.config`
  to create an `io.github.merlimat.slog.Logger` (`io.github.merlimat.slog.Logger.get(TYPE)`).
- **SLF4J** is still available but is **considered deprecated** for Pulsar logging — do not introduce
  new SLF4J loggers.
- Never use `System.out` / `System.err`.
- **Default new log statements to `TRACE` or `DEBUG`, not `INFO`.** Existing Pulsar code overuses
  `INFO`, which makes production logs excessively noisy. Use `TRACE` for fine-grained/low-level
  detail, `DEBUG` for diagnostics that could reasonably be enabled in production without flooding the
  logs, and reserve `INFO` for low-frequency, significant lifecycle/state-change events.
- Attach data as **structured attributes** — `log.info().attr("topic", topic).log("Published")` —
  rather than interpolating it into the message string.
- For expensive `DEBUG`/`TRACE` values, do **not** guard with `isDebugEnabled()` / `isTraceEnabled()`.
  Use slog's lazy evaluation, which only runs when the level is enabled:
  - pass a supplier lambda as the attribute value — `log.debug().attr("dump", () -> expensiveDump()).log("...")`, or
  - use the deferred event form — `log.debug(e -> e.attr("dump", expensiveDump()).log("..."))`.
- Avoid logging in hot paths and stack traces at `INFO` or lower.

## Resource and memory management

- Always close resources: streams, network connections, executors, buffers. Prefer
  try-with-resources.
- For internal networking/messaging paths, prefer **Netty `ByteBuf`** over `ByteBuffer` unless an
  external API requires `ByteBuffer`. Release ref-counted buffers you allocate.
- **Don't hand-optimize object allocation away.** Pulsar is intended to run on **ZGC**, whose
  collection overhead is very low, so the extra short-lived allocations from favouring immutable
  objects (see *Concurrency* above) are cheap. Much older Pulsar code minimizes allocation by pooling
  objects with Netty's `Recycler`; this is **no longer recommended for new code** — under ZGC the
  `Recycler` often *costs* more CPU than it saves versus simply letting the GC reclaim the objects.
  Do not introduce new `Recycler` usage. See
  [PIP-443: Stop using Netty Recycler in new code](pip/pip-443.md).

## Configuration

When adding configuration options: use clear, descriptive names; provide sensible defaults; update
the default configuration files; and document the option.

## Dependencies

Prefer existing dependencies over introducing new libraries. Pulsar commonly uses Apache
Commons / Guava (utilities), **FastUtil** (type-specific collections), **JCTools** (concurrent data
structures), **RoaringBitmap** (compressed bitsets), **Caffeine** (caching), **Jackson** (JSON),
Prometheus / **OpenTelemetry** (metrics), and **Netty** (networking and buffers).

A new dependency must be justified (explain why existing ones are insufficient) and must update the
bundled-dependency `LICENSE`/`NOTICE` — verify with `./gradlew checkBinaryLicense`.

## Backward compatibility

Pulsar maintains strong compatibility guarantees. Changes must not break public APIs, client
compatibility, wire-protocol compatibility, or serialized/metadata formats. Servers must work with
both older and newer clients. Flag any change that may break compatibility.

## Testing conventions

- **TestNG + Mockito.** Prefer **AssertJ** assertions with descriptions over TestNG asserts; use
  **Awaitility** (with AssertJ) for asynchronous conditions instead of `sleep`-based timing. Add
  timeouts to prevent hangs.
- Every feature or bug fix should include tests covering edge cases and failure scenarios; tests must
  be deterministic and stable. Integration tests may be required for distributed components.
- **Validate performance optimizations with a benchmark.** Prefer adding a **JMH benchmark** under the
  `microbench/` subproject that simulates a usage pattern realistic for Pulsar production, so the
  optimization is backed by evidence rather than intuition. See `microbench/README.md` for how to run
  JMH benchmarks.
- **No reflection to access private state in tests** (`WhiteboxImpl.getInternalState` /
  `setInternalState`, `Field.setAccessible(true)`, `Method.setAccessible(true)`, and similar).
  Instead expose what the test needs through a **package-private** `@VisibleForTesting` accessor and
  place the test in the same package:

  ```java
  @VisibleForTesting
  Map<String, Subscription> getSubscriptions() {
      return subscriptions;
  }
  ```

  New reflection-based test access should be flagged in review. See the dev@ thread
  [Stop using reflection to access private fields in tests](https://lists.apache.org/thread/7gr04sqmzyttx4ln6ydtp3qv0xgo1o6m).
- **Close/release resources in tests** — shut down executors/clients/services and release Netty
  `ByteBuf`s (and other reference-counted buffers) the test allocates.
  - A reported **`ByteBuf`/buffer leak** (Netty pooled-allocator leak detection, enabled via
    `-Dpulsar.allocator.pooled=true`) is a **real bug**: fix the root cause — the missing `release()`
    in the code under test — rather than working around it in the test or suppressing the detector.
  - **Thread leaks** reported by `ThreadLeakDetectorListener` are currently **not a reliable signal**.
    The detector produces a high rate of false positives — notably with `SharedPulsarBaseTest`, and
    whenever the `THREAD_LEAK_DETECTOR_WAIT_MILLIS` environment value is not set high enough
    (≈`10000` is recommended) to let asynchronous shutdown complete before the check runs. That wait
    setting only takes effect when the **Gradle daemon is disabled** (`--no-daemon`). Because of these
    false positives, do not rely on `ThreadLeakDetectorListener` alone to conclude that a change is
    thread-leak-free; corroborate before treating a reported thread leak as a real bug.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to *run* tests (test groups, `--tests` scoping,
integration tests, retry count).

## Code review checklist

When reviewing a pull request, verify:

- Java coding conventions are followed.
- Thread-safety risks; no blocking operations in async paths; correct `CompletableFuture` usage.
- No unnecessary dependencies; LICENSE/NOTICE updated when dependencies change.
- Logging follows the guidelines above ([slog](https://github.com/merlimat/slog), levels, structured attributes).
- Backward compatibility is preserved.
- Tests exist and are appropriate; reflection-based access to private state is flagged with a
  `@VisibleForTesting` package-private accessor suggested instead.
- The **PR description adequately explains the change** — at minimum the **Motivation (why?)** and
  **Modifications (what / how?)**, matching `.github/PULL_REQUEST_TEMPLATE.md`. A title alone, or a
  description that only restates the title, is not sufficient; ask for a proper description unless it
  is already covered.

Focus feedback on correctness, reliability, and maintainability.
