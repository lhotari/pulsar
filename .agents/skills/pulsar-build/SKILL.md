---
name: pulsar-build
description: AI-tooling guardrails for changes to the Apache Pulsar Gradle build. Points at the build conventions in ARCHITECTURE.md (tiered modules, convention plugins under build-logic/, the gradle/libs.versions.toml version catalog, the pulsar-dependencies enforced platform) and adds the AI-specific constraints on top — configuration-cache / configure-on-demand compatibility, no hard-coded versions, update LICENSE/NOTICE after dependency changes. Use when editing build.gradle.kts, settings.gradle.kts, build-logic/, gradle.properties, or gradle/libs.versions.toml.
license: Apache-2.0
compatibility: claude, codex, copilot, cursor, gemini, aider
metadata:
  audience: contributors to apache/pulsar
  scope: ai-tooling-build-guardrails
---

# Pulsar build

AI-tooling layer over the Gradle build. The conventions themselves live in
[`ARCHITECTURE.md`'s "Build infrastructure" section](../../../ARCHITECTURE.md#build-infrastructure);
this skill cites them and adds the guardrails below.

## When to use this skill

**Use it for:**

- Convention-plugin changes under `build-logic/conventions/` (`pulsar.java-conventions`,
  `pulsar.code-quality-conventions`, `pulsar.shadow-conventions`, …).
- `settings.gradle.kts` (module wiring/tiers) and root or module `build.gradle.kts` edits.
- `gradle.properties`, `gradle/libs.versions.toml` (version catalog), or the `pulsar-dependencies`
  enforced platform.
- Adding, removing, or upgrading a runtime or test dependency.
- Gradle wrapper bumps (`gradle/wrapper/gradle-wrapper.properties`).

**Don't use it for:** production/test source changes — those are
[`pulsar-tests`](../pulsar-tests/SKILL.md) or the relevant module.

## Read first

- [`ARCHITECTURE.md` → Build infrastructure](../../../ARCHITECTURE.md#build-infrastructure) — tiers,
  convention plugins, version catalog, enforced platform, the module-name gotcha.
- [`CODING.md` → Dependencies](../../../CODING.md#dependencies) — which libraries are preferred and
  the LICENSE/NOTICE obligation.

## Guardrails

- **Edit shared config in the convention plugins, not per-module.** Compile/test/dependency defaults
  belong in `build-logic/conventions/`; don't duplicate them across module build scripts.
- **Versions come from the catalog.** Reference `libs.*` / the `pulsar-dependencies` platform; never
  hard-code a version string in a build script. Add or change versions in
  `gradle/libs.versions.toml`.
- **Stay configuration-cache and configure-on-demand compatible.** Both are enabled
  (`org.gradle.configuration-cache=true`, `org.gradle.configureondemand=true`). Any task on a
  commonly-run path (`assemble`, `test`, `integrationTest`, `rat`/`spotlessCheck`/`checkstyle*`,
  `checkBinaryLicense`, `docker*`) must not read mutable state at execution time or access `Project`
  in task actions — use `Provider` / value sources. **Verify with `--configuration-cache`.**
  Tooling/one-off tasks (e.g. `verifyTestGroups`, dependency reports) may be exempt.
- **Follow [Gradle best practices](https://docs.gradle.org/current/userguide/best_practices_index.html).**
- **Use the real Gradle project path** (mind the module-name-vs-directory gotcha — directory
  `pulsar-client/` is project `:pulsar-client-original`).
- **After any dependency change, run `./gradlew checkBinaryLicense`** and update the binary
  distribution `LICENSE`/`NOTICE` accordingly. Justify a genuinely new dependency.
- **Don't fabricate plugin/DSL/task names** — verify they exist in the build.

## Validation checklist

- [ ] `./gradlew help --configuration-cache` (and the affected task with `--configuration-cache`)
      succeeds with no config-cache problems.
- [ ] `./gradlew assemble` (or the affected module's `assemble`) passes.
- [ ] `./gradlew rat spotlessCheck checkstyleMain checkstyleTest` passes.
- [ ] If dependencies changed: `./gradlew checkBinaryLicense` passes and LICENSE/NOTICE are updated.
