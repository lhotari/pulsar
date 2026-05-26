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
- **Use the narrowest interface type** for fields, parameters, variables, and return types — `java.util.Map`,
  `SequencedMap`, `SortedMap`, `Collection`, `List` — rather than a concrete implementation such as
  `TreeMap`. Keep the concrete type only where its behaviour is actually required (e.g. instantiate a
  `TreeMap` for stable key-ordered iteration) and still expose it through the interface type.
- **Minimize method and constructor parameters.** Don't pass values that are already reachable from a
  context object a method receives — read them from it (e.g. from `PublishContext`) instead of adding
  redundant parameters. For a constructor (or factory) with many parameters, use a **builder**: the
  project uses Lombok-generated builders (`@Builder`) for most internal classes. `@Builder` also works
  on a Java `record`, which pairs well with preferring records (above) — a record plus `@Builder` is a
  clean way to pass many values without a long parameter list. **Don't use Lombok
  `@Builder` on public client-API classes** — it's harder to maintain and its default builder class
  name carries no meaningful context; hand-write the builder there, or if Lombok is used set an
  explicit, meaningful `builderClassName`.
- **Name things for intent.** Name a boolean-returning method as a query (`shouldSkipChunk`, not
  `skipChunk`); name methods, fields, and metrics after the effect they describe rather than vague
  terms (prefer e.g. `truncated` / `migrationSegment` over `overflow` / `special`).
- **Construct via factory methods, not internal implementation types.** Use the provided factory
  (e.g. `PositionFactory.create(...)`) instead of referencing an internal class such as
  `ImmutablePositionImpl` directly.

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
- If an operation performs IO (creating authentication, starting a transaction, etc.), it should
  return a `CompletableFuture` rather than block.
- **Converting an existing synchronous-throwing method to return a failed future is not a mechanical
  change** — evaluate each call site first. Some callers historically rely on the exception being
  thrown *before* the async work starts, so a blanket conversion can change behaviour and introduce
  instability in untested error paths. Use a shared helper (e.g. a `checkArgumentAsync` in `FutureUtil`)
  to validate arguments without duplicating the try/catch in every method.

## Concurrency

- Public classes should be **thread-safe**; annotate non-thread-safe ones with `@NotThreadSafe`.
- Protect shared mutable state; prefer fine-grained synchronization; ensure mutations occur on the
  intended thread. Prefer the **single-writer principle** — design so a given piece of state is
  mutated by only one thread — to avoid concurrent mutation entirely.
- **Minimize work while holding a lock.** Capture the state you need into local variables inside the
  synchronized block, then run callbacks, listeners, and IO *outside* it. Never call out to
  listener/callback code while holding a lock — narrowing lock scope has fixed real deadlocks and
  contention in Pulsar.
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

## Performance

- **Back optimizations with evidence** — a JMH benchmark (see *Testing conventions*) or a profile —
  not intuition; and measure on JIT-warmed code (see *Reproducing concurrency / memory-visibility
  bugs* for why warm-up matters).
- **On hot paths** (dispatch, IO, per-message code): avoid `String.format` (build the string directly),
  avoid `Enum.values()` (match values explicitly), and avoid unnecessary allocation and locking. Prefer
  lock-free or single-writer designs over synchronization where practical.
- **Don't add overhead to an already-overloaded system.** Avoid doing work and then discarding it
  (e.g. reading entries from BookKeeper only to drop them before dispatch); extra work under high load
  causes cascading failures. Acquire or estimate up front and reconcile afterwards instead.
- **Bound in-memory caches** with a size or byte limit plus eviction, and de-duplicate repeated
  `String`s (cluster / tenant / namespace ids) with `org.apache.pulsar.common.util.StringInterner` to
  avoid heap duplication.
- Don't pre-emptively micro-optimize elsewhere — favour clear, correct code, and let ZGC handle
  short-lived allocations (see *Resource and memory management*).

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

**Plugin / SPI extension points are public API.** Pulsar has many pluggable interfaces selected by a
`*ClassName` configuration setting — e.g. `LoadManager`, `LedgerOffloaderFactory`,
`AuthorizationProvider` / `AuthenticationProvider`, `EntryFilter`, `TopicFactory`, `BrokerInterceptor`,
dispatcher / delayed-delivery-tracker factories, `CustomCommand`. Third parties ship implementations of
these in production. Changing such an interface — or a `protected` member of a class meant for
extension, such as `PulsarWebResource`, `PersistentTopic`, or `Producer` — breaks those implementations.
Treat these as public API: the change generally needs a PIP, and breaking changes must not land in
maintenance-branch backports (keep deprecated bridge methods that delegate to the new form instead).
When you must add a method to such an interface, give it a **`default` implementation** (e.g. one
throwing `UnsupportedOperationException`) so existing third-party implementors still compile — this
also keeps the change source-compatible enough to backport.

**Don't leak third-party types through public or plugin interfaces.** Exposing library types such as
Netty or AsyncHttpClient classes in a public / plugin API breaks consumers of the **shaded** Pulsar
client (the shaded and unshaded classes differ) and couples callers to the implementation — provide a
Pulsar-owned abstraction instead. Changing a documented behaviour or guarantee (e.g. the
exclusive-producer guarantees of PIP-68, or default rate-limiter behaviour) likewise needs a PIP and a
dev@ discussion, not just a code change.

**Introduce changes behind a backward-compatible default.** Prefer making new or changed behaviour
opt-in via configuration rather than silently changing what existing deployments do. Behaviour that can
cause data loss (e.g. skipping unrecoverable data) must be gated behind an explicit flag (such as
`autoSkipNonRecoverableData`), defaulting to the safe/old behaviour.

## Testing conventions

Most of Pulsar's so-called **"unit tests"** (under each module's `src/test`, run with
`./gradlew :<module>:test`) are in fact **integration-style** — they bring up a real in-JVM broker via
`MockedPulsarServiceBaseTest` / `pulsarTestContext` rather than testing a class in isolation. The
**actual integration tests** live under `tests/` and run against a Pulsar **Docker test image** with
Testcontainers (see [`CONTRIBUTING.md`](CONTRIBUTING.md#integration-tests)). "Unit test" below means
the former.

Ideally this would not be necessary: well-designed code lets genuine **units be unit-tested in
isolation**, with collaborators behind narrow interfaces that can be substituted by simple
fakes/mocks **without excessive mocking**. Excessive mocking is a design smell (tight coupling), not a
reason to abandon unit testing. Much existing Pulsar code isn't factored this way, which is why
integration-style tests are the pragmatic default today — but when you write or refactor code, prefer
the design that makes a true, isolated unit test possible.

- **TestNG + Mockito.** Prefer **AssertJ** assertions with descriptions over TestNG asserts; use
  **Awaitility** (with AssertJ) for asynchronous conditions instead of `sleep`-based timing. Add
  timeouts to prevent hangs. Use Awaitility only when polling/retrying for a condition:
  `untilAsserted(...)` retries assertions until they pass, `until(...)` waits for a boolean to become
  `true` — don't swap the two.
- Every feature or bug fix should include tests covering edge cases and failure scenarios; tests must
  be deterministic and stable. Integration tests may be required for distributed components.
- **For code that isn't factored for isolation, prefer an integration-style test over heavy mocking.**
  Reproduce the real production scenario with `MockedPulsarServiceBaseTest` / `pulsarTestContext`
  rather than mocking a web of internal collaborators.
  Inject faults through the test infrastructure — e.g.
  `pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor(...)` to simulate read failures or
  delays — and capture log output with `org.apache.pulsar.utils.TestLogAppender` to assert behaviour.
  A bug-fix test should **fail against the unpatched code for the actual reason**, not because the test
  forces internal state.
- **For new integration-style tests, prefer `SharedPulsarBaseTest`.** It shares a single
  `SharedPulsarCluster` and its components for the whole **test-JVM lifecycle** (avoiding per-test
  broker startup cost), while `admin` / `pulsarClient` are shared **per test class** (initialized once
  per class). Each test method gets **its own namespace** (created before the method, force-deleted
  after). Get that namespace with `getNamespace()` and create topic names with `newTopicName()` —
  never hardcode namespace or topic names, because the runtime is shared across all tests in the JVM.
  (The shared runtime is also why `ThreadLeakDetectorListener` mis-reports leaks with this base
  class — see above.)
- **Don't mutate private/internal state to force a condition.** If a test has to reach into internal
  fields to trigger the path under test, it usually isn't representative — drive the real path instead.
- It's fine to **add a new, cleanly-written test class** instead of extending an existing one whose
  base class or style makes the new case hard to read; existing Pulsar tests are not always good models.
- For asynchronous interactions, verify with Mockito's `timeout(...)` (e.g.
  `verify(x, timeout(2000)).foo()`) rather than fixed sleeps.
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
