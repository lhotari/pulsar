---
name: pulsar-build
description: AI guardrails for Gradle build changes (convention plugins, settings, version catalog, dependencies). Use when editing build-logic/, *.gradle.kts, gradle.properties, or gradle/libs.versions.toml.
---

# Pulsar build

AI-tooling layer over [`ARCHITECTURE.md` → Build infrastructure](../../../ARCHITECTURE.md#build-infrastructure)
(tiers, convention plugins, version catalog, enforced platform, the module-name gotcha) and
[`CODING.md` → Dependencies](../../../CODING.md#dependencies). Load those for detail.

## When to use

Editing `build-logic/`, `settings.gradle.kts`, root/module `build.gradle.kts`, `gradle.properties`,
`gradle/libs.versions.toml`, the `pulsar-dependencies` platform, or the Gradle wrapper. (Source
changes belong to [`pulsar-tests`](../pulsar-tests/SKILL.md) or the module itself.)

## Guardrails

- **Edit shared config in `build-logic/conventions/`**, not per-module.
- **Versions come from `gradle/libs.versions.toml`** (`libs.*` / `pulsar-dependencies`) — never
  hardcode a version in a build script.
- **Keep tasks configuration-cache and configure-on-demand compatible** (both are enabled): no
  mutable-state reads or `Project` access in task actions — use `Provider` / value sources. Verify
  with `--configuration-cache`. One-off tooling tasks may be exempt. Follow
  [Gradle best practices](https://docs.gradle.org/current/userguide/best_practices_index.html).
- **Use the real Gradle project path** (`pulsar-client/` is `:pulsar-client-original`).
- **After a dependency change**, run `./gradlew checkBinaryLicense` and update the distribution
  LICENSE/NOTICE; justify any genuinely new dependency.
- **Published modules must not depend on internal modules** (compile/runtime) — the artifact would be
  unresolvable from Maven Central. Modules aren't published unless they apply
  `pulsar.public-java-library-conventions`.
- **Don't fabricate plugin / DSL / task names** — verify they exist.

## Before you finish

- [ ] The affected task and `./gradlew help` run clean with `--configuration-cache`.
- [ ] `assemble` and `rat spotlessCheck checkstyleMain checkstyleTest` pass.
- [ ] Dependency change → `checkBinaryLicense` passes and LICENSE/NOTICE are updated.
