# Agent skills

Task-specific guidance for AI coding assistants working in apache/pulsar. Each skill is a directory
with a `SKILL.md` that adds AI-tooling guardrails on top of the canonical docs
([`CONTRIBUTING.md`](../../CONTRIBUTING.md), [`ARCHITECTURE.md`](../../ARCHITECTURE.md),
[`CODING.md`](../../CODING.md)). Load the relevant skill **on demand** — before working in its area —
instead of pulling everything into context. The index of skills is in the repository-root
[`AGENTS.md`](../../AGENTS.md#skills).

| Skill | Use for |
|-------|---------|
| [`pulsar-build`](pulsar-build/SKILL.md) | Gradle build changes — convention plugins, `settings.gradle.kts`, version catalog, dependencies. |
| [`pulsar-tests`](pulsar-tests/SKILL.md) | Writing and running tests — `--tests` scoping, TestNG groups, leak detectors. |
| [`pulsar-pr-workflow`](pulsar-pr-workflow/SKILL.md) | Branching, semantic PR titles, descriptions, Personal CI, rebase-vs-merge. |
| [`pulsar-security`](pulsar-security/SKILL.md) | Reporting a vulnerability or checking exposure to a public CVE. |

To add a skill, create `<skill-name>/SKILL.md` with YAML frontmatter (`name`, `description` — phrased
so an agent knows *when* to use it), keep it focused on one task area, cite the canonical docs rather
than restating them, and add a row here and in [`AGENTS.md`](../../AGENTS.md#skills).
