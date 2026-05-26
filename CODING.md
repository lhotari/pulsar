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
  intended thread.
- Give threads **meaningful names** for diagnostics.

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
