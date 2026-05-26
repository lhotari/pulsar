# Agent guide for Apache Pulsar

Supplemental guidance for AI coding assistants (Claude Code, Copilot, Cursor, Gemini, Codex, Aider,
and similar tools) working in this repository. Keep this file short — it is a **router**. The detail
lives in the human-facing docs and the task skills, which you should load **on demand** rather than
pulling everything into context up front.

Apache Pulsar is a distributed pub-sub messaging and streaming platform. The codebase is
performance-critical, heavily asynchronous, and concurrency-sensitive. Prioritize **correctness,
thread safety, performance, maintainability, and backward compatibility**.

## Licensing and provenance (read first)

Apache Pulsar is licensed under the Apache License 2.0, and all contributions must meet the ASF's
[Generative Tooling guidance](https://www.apache.org/legal/generative-tooling.html):

- **A human is in the loop and is accountable.** Every pull request and every security report must be
  submitted by a human contributor who has reviewed and verified the change and takes responsibility
  for it. Fully autonomous agents (e.g. OpenClaw-style bots) opening PRs or filing reports on their own
  are not acceptable — the AI assists, the human is accountable.
- **Don't introduce code of incompatible or unknown provenance.** Never copy verbatim from
  GPL/AGPL/LGPL, proprietary, or unlicensed sources (including Stack Overflow / blog / forum snippets of
  unclear licensing), and don't use a tool whose terms restrict the output inconsistently with open
  source. Reimplement from specifications or Apache-compatible sources, and follow the
  [ASF 3rd Party Licensing Policy](https://www.apache.org/legal/resolved.html).
- **Every new source file needs the ASF license header** (Spotless enforces this — copy the form from
  an existing `.java` file).
- **Consider attributing AI assistance.** When AI tooling assisted on a change, consider adding an
  `Assisted-by: <tool/version>` commit trailer (the default, since a human still does the final
  review); `Generated-by: <tool/version>` is for minimally-modified generated output.

## Canonical docs (read the one that fits the task)

| Doc | Use for |
|-----|---------|
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Local dev workflow: build, lint, running tests & test groups, integration tests, Personal CI, PR conventions, security reporting. |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | Big-picture module map, the Gradle build infrastructure, the module-name-vs-directory gotcha, and the `pip/` proposals. |
| [`CODING.md`](CODING.md) | Coding conventions: style, async/`CompletableFuture`, concurrency, logging ([slog](https://github.com/merlimat/slog)), dependencies, backward compatibility, testing, and the review checklist. |
| [`SECURITY.md`](SECURITY.md) | Reporting a vulnerability, disclosure hygiene, and checking exposure to an already-public CVE. |

The authoritative project documentation is at <https://pulsar.apache.org>; the files above and the
website remain the source of truth — this guide just layers AI-specific pointers on top.

## Skills

> **Before writing or modifying code in an area below, read the matching skill file first.** Each
> skill is more focused than the canonical docs and adds AI-specific guardrails; it tells you what to
> verify and points back into the docs above. Load it on demand to keep context small.

Task-specific guidance lives under [`.agents/skills/`](.agents/skills/), each in its own directory
with a `SKILL.md`:

| Skill | Use for |
|-------|---------|
| [`pulsar-build`](.agents/skills/pulsar-build/SKILL.md) | Editing the Gradle build — convention plugins under `build-logic/`, `settings.gradle.kts`, `gradle/libs.versions.toml`, dependency changes. Enforces configuration-cache / configure-on-demand compatibility and LICENSE/NOTICE upkeep. |
| [`pulsar-tests`](.agents/skills/pulsar-tests/SKILL.md) | Writing or running tests — TestNG groups, `--tests` scoping, AssertJ/Awaitility, no-reflection rule, buffer/thread leak detectors, retry count. |
| [`pulsar-pr-workflow`](.agents/skills/pulsar-pr-workflow/SKILL.md) | Branching, commits, semantic PR titles, PR descriptions, the Personal CI loop, and the never-rebase-after-open / merge-instead rule. |
| [`pulsar-security`](.agents/skills/pulsar-security/SKILL.md) | Reporting a vulnerability or checking Pulsar's exposure to an already-public CVE. |

## Critical rules

1. **Don't break backward compatibility** — public APIs, client compatibility, wire protocol, and
   serialized/metadata formats. Servers must interoperate with older and newer clients.
2. **Never block on async/event-loop threads;** methods returning `CompletableFuture` must not throw
   synchronously. See [`CODING.md`](CODING.md#asynchronous-programming).
3. **Logging:** prefer [slog](https://github.com/merlimat/slog) via `@CustomLog`; default new logs to
   `TRACE`/`DEBUG`, not `INFO`.
4. **Tests:** scope runs with `--tests`; no reflection into private state (use a `@VisibleForTesting`
   package-private accessor); release buffers and resources.
5. **PRs:** semantic `[type][scope]` title; describe **motivation** and **modifications**; do not
   rebase once the PR is open in `apache/pulsar` — merge upstream `master` instead.
6. **Security:** never disclose a vulnerability — or the security nature of a change — in a public
   issue, PR, or commit. See [`SECURITY.md`](SECURITY.md) /
   [`pulsar-security`](.agents/skills/pulsar-security/SKILL.md).
7. **Stay in scope.** Keep a change focused on its task; don't bundle unrelated drive-by refactors or
   generate broad mass-refactoring PRs. Discuss large refactorings on `dev@pulsar.apache.org` first.
   See [`CONTRIBUTING.md`](CONTRIBUTING.md#pull-requests).

## Where to ask

- Dev mailing list: <dev@pulsar.apache.org> · Slack: <https://apache-pulsar.slack.com/>
- Issues: <https://github.com/apache/pulsar/issues> · Discussions: <https://github.com/apache/pulsar/discussions>
