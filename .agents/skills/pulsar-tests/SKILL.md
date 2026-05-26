---
name: pulsar-tests
description: AI guardrails for writing and running Apache Pulsar tests (TestNG/Mockito/AssertJ/Awaitility). Use when adding, modifying, running, or debugging unit or integration tests.
---

# Pulsar tests

AI-tooling layer over [`CODING.md` → Testing conventions](../../../CODING.md#testing-conventions) (how
tests are written) and [`CONTRIBUTING.md` → Running tests](../../../CONTRIBUTING.md#running-tests) (how
to run them). Load those for the full detail and examples — the points below are the ones agents most
often get wrong.

## Guardrails

- **Scope every run with `--tests`** (class / method / package) — never run a whole module's `test`
  task. Mind the module-name gotcha (`pulsar-client/` → `:pulsar-client-original`).
- **Run with `-PtestRetryCount=0`** so flakiness surfaces; a test in `flaky` / `quarantine` needs
  `-PexcludedTestGroups=''`.
- **AssertJ + Awaitility**, never `Thread.sleep` timing; tests must be deterministic and have timeouts.
- **No reflection into private state** — add a package-private `@VisibleForTesting` accessor instead;
  flag any new reflection.
- **A `ByteBuf` / buffer leak is a real bug** (fix the missing `release()`), but a **thread leak from
  `ThreadLeakDetectorListener` is unreliable** (false positives, notably with `SharedPulsarBaseTest`) —
  corroborate before treating it as real.
- **New integration-style test → extend `SharedPulsarBaseTest`**; get the per-method namespace from
  `getNamespace()` and topic names from `newTopicName()`. Never hardcode namespace/topic names (the
  cluster is shared across the test JVM).
- **A bug-fix test must fail on the unpatched code for the real reason** — not by mutating internal
  state to force it. Don't fabricate assertions or symbol names. Prefer a design that allows a true
  isolated unit test over piling on mocks.
- **Concurrency / memory-visibility fixes**: a clean local run is weak evidence (timing- and
  platform-dependent). See
  [`CODING.md` → Reproducing concurrency bugs](../../../CODING.md#reproducing-concurrency--memory-visibility-bugs).

## Before you finish

- [ ] The scoped `--tests` run passes with `-PtestRetryCount=0`; a bug-fix test fails without the fix.
- [ ] No reflection into private state; resources and buffers are released.
- [ ] Larger changes validated via **Personal CI**, not a local full run.
