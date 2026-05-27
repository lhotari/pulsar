# Building Apache Pulsar

Apache Pulsar uses a **Gradle** build (migrated from Maven via PIP-463; some older tooling and docs
elsewhere still reference Maven). Use the bundled wrapper `./gradlew` (Linux/macOS) or `gradlew.bat`
(Windows) — no separate Gradle install is needed. For local-environment setup see the
[build-tooling setup guide](https://pulsar.apache.org/contribute/setup-buildtools/) and the
[IDE setup guide](https://pulsar.apache.org/contribute/setup-ide/). For how to *run tests* see
[`CONTRIBUTING.md` → Running tests](CONTRIBUTING.md#running-tests); for the module map see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Prerequisites

- **JDK 21 or 25** is required to build `master`. Bytecode targets Java 17 (`options.release = 17`).
  `zip` is also needed. (`-PskipJavaVersionCheck` bypasses the JDK version check.)
- The Gradle wrapper `./gradlew` pins the Gradle version — don't install Gradle separately.

## Common build commands

```bash
# Compile and assemble everything (or a single module)
./gradlew assemble
./gradlew :pulsar-broker:assemble

# Lint / verify (license headers, formatting, checkstyle) — run before pushing
./gradlew rat spotlessCheck checkstyleMain checkstyleTest
./gradlew spotlessApply            # auto-fix license headers/formatting

# Verify bundled-dependency LICENSE/NOTICE coverage (run after changing a runtime dependency)
./gradlew checkBinaryLicense

# Start a standalone Pulsar service (broker + bookie + metadata in one JVM)
bin/pulsar standalone

# Build docker images apachepulsar/pulsar(-all):latest
./gradlew docker        # or docker-all
```

## Build infrastructure

The build is **Gradle**; the wrapper requires **JDK 21 or 25** to run (bytecode targets Java 17).

- `settings.gradle.kts` — all modules, organized in dependency tiers (Tier 0 has no internal deps,
  higher tiers build on lower ones).
- `build-logic/conventions/` — convention plugins (`pulsar.java-conventions`,
  `pulsar.code-quality-conventions`, `pulsar.shadow-conventions`, etc.) applied by modules. This is
  where shared compile/test/dependency config lives — edit conventions here rather than duplicating
  config across modules.
- `gradle/libs.versions.toml` — version catalog (single source of truth for dependency versions;
  referenced as `libs.*` in build scripts).
- `pulsar-dependencies` — enforced platform (BOM) pinning all dependency versions; applied to every
  module.

The build enables both the **configuration cache** (`org.gradle.configuration-cache=true`) and
**configure-on-demand** (`org.gradle.configureondemand=true`).

## Module name vs. directory name gotcha

Several Gradle project paths do **not** match their directory because the Maven artifactId is
preserved. Most importantly:

- Directory `pulsar-client/` → project **`:pulsar-client-original`**
- Directory `pulsar-client-admin/` → project **`:pulsar-client-admin-original`**
- Directory `pulsar-functions/localrun/` → project `:pulsar-functions:pulsar-functions-local-runner-original`

Always use the Gradle project path (left of any `--tests`), e.g. `./gradlew :pulsar-client-original:test`.
Check `settings.gradle.kts` when a path is ambiguous.

## Changing the build

When editing `build-logic/`, `settings.gradle.kts`, a module `build.gradle.kts`, `gradle.properties`,
`gradle/libs.versions.toml`, or the `pulsar-dependencies` platform:

- **Edit shared config in `build-logic/conventions/`**, not per-module.
- **Versions come from `gradle/libs.versions.toml`** (`libs.*` / `pulsar-dependencies`) — never
  hardcode a version in a build script.
- **Keep tasks configuration-cache and configure-on-demand compatible** (both are enabled): no
  reading of mutable state at execution time and no `Project` access in task actions — use
  `Provider` / value sources instead, and verify with `--configuration-cache`. Tasks reached by the
  common flows (`assemble`, `test`, `integrationTest`, `rat` / `spotlessCheck` / `checkstyle*`,
  `checkBinaryLicense`, `docker*`) must be compatible; one-off tooling tasks not part of those flows
  (e.g. `verifyTestGroups`, ad-hoc maintenance/report tasks) may be exempt. Follow the
  [Gradle best practices](https://docs.gradle.org/current/userguide/best_practices_index.html).
- **Published modules must not depend on internal modules** at compile/runtime scope — the artifact
  would be unresolvable from Maven Central. A module is published only when it applies
  `pulsar.public-java-library-conventions`.
- **After a dependency change**, run `./gradlew checkBinaryLicense` and update the distribution
  `LICENSE`/`NOTICE`; justify any genuinely new dependency (see
  [`CODING.md` → Dependencies](CODING.md#dependencies)).
- **Follow the [Gradle best practices](https://github.com/gradle/gradle/blob/master/platforms/documentation/docs/src/docs/userguide/best-practices/best_practices_index.adoc)**.

Before finishing a build change, confirm the affected task and `./gradlew help` run clean with
`--configuration-cache`, and that `assemble` and `rat spotlessCheck checkstyleMain checkstyleTest`
pass (plus `checkBinaryLicense` if a dependency changed).
