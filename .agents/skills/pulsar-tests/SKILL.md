---
name: pulsar-tests
description: AI-tooling guardrails for writing and running Apache Pulsar tests. Points at the testing conventions in CODING.md and the run instructions in CONTRIBUTING.md, then adds the AI-specific constraints — scope runs with --tests (never a whole module), no reflection into private state, treat ByteBuf leaks as real bugs but ThreadLeakDetectorListener reports as unreliable, prefer -PtestRetryCount=0. Use when adding, modifying, or running unit or integration tests (TestNG/Mockito/AssertJ/Awaitility).
license: Apache-2.0
compatibility: claude, codex, copilot, cursor, gemini, aider
metadata:
  audience: contributors to apache/pulsar
  scope: ai-tooling-test-guardrails
---

# Pulsar tests

AI-tooling layer over Pulsar's test conventions
([`CODING.md` → Testing conventions](../../../CODING.md#testing-conventions)) and how to run them
([`CONTRIBUTING.md` → Running tests](../../../CONTRIBUTING.md#running-tests)).

## When to use this skill

Use it whenever adding, modifying, or running unit or integration tests, or when investigating a test
failure / flake.

## Read first

- [`CODING.md` → Testing conventions](../../../CODING.md#testing-conventions) — frameworks,
  no-reflection rule, resource/leak cleanup.
- [`CONTRIBUTING.md` → Running tests](../../../CONTRIBUTING.md#running-tests) — `--tests` scoping,
  test groups, integration tests, Personal CI.

## Guardrails

- **Always scope runs with `--tests`.** Running a module's whole `test` task is slow; pass a class,
  method, or package pattern. Mind the module-name gotcha (`pulsar-client/` → `:pulsar-client-original`).
- **Prefer `-PtestRetryCount=0` locally** so failures and flakiness surface immediately instead of
  being masked by the default single retry.
- **A test in the `flaky`/`quarantine` group is excluded by default** — add `-PexcludedTestGroups=''`
  to run it.
- **Use AssertJ (with descriptions) + Awaitility** for async conditions; never `Thread.sleep`-based
  timing. Add timeouts to prevent hangs. Tests must be deterministic.
- **No reflection into private state** (`WhiteboxImpl.getInternalState`/`setInternalState`,
  `setAccessible(true)`). Add a package-private `@VisibleForTesting` accessor and put the test in the
  same package instead. Flag any new reflection-based access.
- **Close/release what the test allocates** — executors, clients, services, and Netty `ByteBuf`s.
- **Leak signals are not equal:**
  - A **`ByteBuf`/buffer leak** (pooled-allocator detection, `-Dpulsar.allocator.pooled=true`) is a
    **real bug** — fix the missing `release()` in the code under test; don't suppress it.
  - A **thread leak** from `ThreadLeakDetectorListener` is **currently unreliable** (high
    false-positive rate, notably with `SharedPulsarBaseTest` and when `THREAD_LEAK_DETECTOR_WAIT_MILLIS`
    isn't high enough — ≈`10000`, and only effective with the Gradle daemon disabled, `--no-daemon`).
    Don't treat it as a real bug on its own; corroborate first.
- **Don't fabricate assertions or test names** — verify symbols exist.
- For a **bugfix**, add a regression test and confirm it fails before the fix and passes after.
- **Most Pulsar "unit tests" are integration-style** (real in-JVM broker via
  `MockedPulsarServiceBaseTest` / `pulsarTestContext`); the container-based integration tests are
  separate (under `tests/`). When code isn't factored for isolation, prefer an integration-style test
  over mocking a web of internal collaborators; inject faults via the test infrastructure (e.g.
  `pulsarTestContext.getMockBookKeeper().setReadHandleInterceptor(...)`) and assert on logs with
  `TestLogAppender`. But when you write or refactor code, prefer a design that allows a true isolated
  unit test (collaborators behind narrow interfaces, light mocking) — excessive mocking is a design
  smell, not the goal. The regression test must fail on the unpatched code for the *actual* reason —
  not because it mutates internal state. Adding a clean new test class is fine when an existing one's
  style/base class gets in the way. See
  [`CODING.md` → Testing conventions](../../../CODING.md#testing-conventions).
- **Prefer `SharedPulsarBaseTest` for new integration-style tests.** It shares one
  `SharedPulsarCluster` for the test-JVM lifecycle and gives each test method its own namespace. Use
  `getNamespace()` for the namespace and `newTopicName()` for topic names; never hardcode namespace or
  topic names (the runtime is shared across tests).
- **Concurrency / memory-visibility bugs** are timing- and platform-dependent and easily masked — a
  clean run is weak evidence a fix is correct. See
  [`CODING.md` → Reproducing concurrency / memory-visibility bugs](../../../CODING.md#reproducing-concurrency--memory-visibility-bugs)
  (JIT warm-up rounds, interpreted-vs-compiled differences, multi-socket/NUMA hardware).

## Validation checklist

- [ ] The new/changed test runs green via a scoped `--tests` invocation with `-PtestRetryCount=0`.
- [ ] No reflection into private state; resources and buffers are released.
- [ ] For a bugfix: the test fails on the unpatched code and passes with the fix.
- [ ] For larger changes, the full suite is validated via **Personal CI**, not locally
      (see [`CONTRIBUTING.md`](../../../CONTRIBUTING.md#running-the-full-ci-pipeline-personal-ci)).
