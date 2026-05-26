---
name: pulsar-pr-workflow
description: AI-tooling guardrails for branching, commits, and pull requests against apache/pulsar. Points at the PR conventions and Personal CI loop in CONTRIBUTING.md, then adds the AI-specific constraints — semantic [type][scope] titles validated against ci-semantic-pull-request.yml, PR descriptions that cover motivation and modifications, never rebase after the PR is opened (merge upstream master instead), and per-action user confirmation before pushing or opening a PR. Use when creating a branch, committing, opening or updating a PR, or running the Personal CI loop.
license: Apache-2.0
compatibility: claude, codex, copilot, cursor, gemini, aider
metadata:
  audience: contributors to apache/pulsar
  scope: ai-tooling-pr-guardrails
---

# Pulsar PR workflow

AI-tooling layer over the contribution workflow in
[`CONTRIBUTING.md` → Pull requests](../../../CONTRIBUTING.md#pull-requests) and
[Personal CI](../../../CONTRIBUTING.md#running-the-full-ci-pipeline-personal-ci).

## When to use this skill

Use it when creating a branch, committing, opening or updating a pull request, or driving the
Personal CI loop on a fork.

## Read first

- [`CONTRIBUTING.md` → Pull requests](../../../CONTRIBUTING.md#pull-requests) — title convention,
  description requirement, rebase-vs-merge rule.
- [`CONTRIBUTING.md` → Personal CI](../../../CONTRIBUTING.md#running-the-full-ci-pipeline-personal-ci)
  — the fork-based full-pipeline loop.
- `.github/PULL_REQUEST_TEMPLATE.md` and `.github/workflows/ci-semantic-pull-request.yml`.

## Guardrails

- **Each state-changing action needs its own explicit user confirmation.** Pushing, opening a PR, and
  posting comments are not authorised just because the task was started. Confirm before each.
- **Before pushing, ensure lint passes:** `./gradlew rat spotlessCheck checkstyleMain checkstyleTest`
  (`spotlessApply` to auto-fix). Scope test runs with `--tests` (see [`pulsar-tests`](../pulsar-tests/SKILL.md)).
- **Semantic PR title** `[<type>][<optional scope>] <description>`: valid `type`/`scope` values are
  enforced by `.github/workflows/ci-semantic-pull-request.yml` — read it rather than guessing.
  `<description>` is imperative, like a commit subject.
- **PR description must cover Motivation (why?) and Modifications (what/how?)** per the template — a
  title alone, or a description restating the title, is not enough. Link the issue when applicable:
  `Fixes #N` (or equivalently `Closes #N`) for an issue the PR resolves, `Main Issue: #N` for one task
  of a larger issue, and `PIP: #N` for a proposal.
- **Never rebase once the PR is open in `apache/pulsar`** — it invalidates review comments and
  incremental diffs. Bring in upstream changes by merging:

  ```bash
  git fetch <apache-pulsar-remote>          # e.g. upstream / apache
  git merge <apache-pulsar-remote>/master
  ```

  Rebasing is fine *before* the PR is opened (e.g. during the Personal CI loop).
- **Personal CI loop (pre-PR):** keep local `master` current, rebase the feature branch, push to the
  **fork** to trigger CI, fix failures, and open the PR to `apache/pulsar` only when the fork's CI is
  green. A PR can be open in both the fork and `apache/pulsar` at once. After the upstream PR is open,
  switch from rebase to merge (above).
- **Flaky CI:** Pulsar has many flaky tests, so GitHub Actions jobs on a PR can fail unrelated to the
  change. When a failure looks like flakiness, comment **`/pulsarbot rerun`** to re-run the failed
  workflows — only **after all jobs from the previous run have completed** (it has no effect while
  jobs are still running or queued). Note that for fork PRs a maintainer must approve each run (again
  after new pushes), which is why Personal CI is the better loop for iterating on genuine failures.
  Don't dismiss a failure as "flaky" without checking it isn't caused by the change.
- **Keep commits and PRs focused** — a fix, a refactor, and a formatting pass are separate
  commits/PRs. Don't bundle unrelated drive-by refactoring into a feature/bug-fix PR, and **don't
  reformat unrelated files or lines** (whitespace, import reordering, re-wrapping) that your change
  doesn't touch — it hides the real diff and pollutes `git blame`. Raise large refactorings on
  `dev@pulsar.apache.org` first rather than opening (or auto-generating) sweeping refactor PRs.
- **Security:** never describe the security nature of a change in a commit/PR — see
  [`pulsar-security`](../pulsar-security/SKILL.md).

## Validation checklist

- [ ] Lint (`rat spotlessCheck checkstyleMain checkstyleTest`) passes locally.
- [ ] Title matches the semantic `[type][scope]` rules in `ci-semantic-pull-request.yml`.
- [ ] Description has Motivation + Modifications and links any issue/PIP.
- [ ] No rebase after the upstream PR was opened; upstream changes pulled in via merge.
- [ ] Each push / PR-open / comment was explicitly confirmed by the user.
