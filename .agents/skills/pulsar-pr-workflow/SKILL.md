---
name: pulsar-pr-workflow
description: AI guardrails for branching, commits, and PRs to apache/pulsar — semantic titles, PR descriptions, Personal CI, never-rebase-after-open, per-action confirmation. Use when committing, opening/updating a PR, or running CI.
---

# Pulsar PR workflow

AI-tooling layer over [`CONTRIBUTING.md` → Pull requests](../../../CONTRIBUTING.md#pull-requests) and
[Personal CI](../../../CONTRIBUTING.md#running-the-full-ci-pipeline-personal-ci); the title rules are
enforced by `.github/workflows/ci-semantic-pull-request.yml` and the body by
`.github/PULL_REQUEST_TEMPLATE.md`. Load those for detail.

## Guardrails

- **Confirm each state-changing action with the user** — pushing, opening a PR, posting a comment.
  Starting the task is not standing authorization.
- **Before pushing**, run lint: `./gradlew rat spotlessCheck checkstyleMain checkstyleTest`
  (`spotlessApply` to fix); scope tests with `--tests`.
- **Semantic title** `[<type>][<scope>] <imperative description>` — valid prefixes are CI-enforced
  (read `ci-semantic-pull-request.yml`, don't guess); no issue numbers in the title.
- **Body must cover Motivation (why) + Modifications (what/how)**, not just restate the title. Link
  the issue: `Fixes #N` / `Closes #N`, `Main Issue: #N`, or `PIP: #N`.
- **Never rebase once the PR is open in `apache/pulsar`** — merge `<remote>/master` instead. (Rebasing
  is fine pre-PR, e.g. during the Personal CI loop.)
- **Iterate via Personal CI** (full pipeline in your own fork) instead of waiting on maintainer CI
  approval; open the upstream PR once the fork's CI is green.
- **Flaky CI**: comment `/pulsarbot rerun` (or `rerun-failure-checks`) only after all jobs finish — but
  verify the failure isn't caused by your change first.
- **Keep PRs focused** — no unrelated drive-by refactoring or reformatting; raise large refactorings on
  `dev@pulsar.apache.org` first.
- **Security**: never state the security nature of a change in a commit/PR — see
  [`pulsar-security`](../pulsar-security/SKILL.md).

## Before you finish

- [ ] Lint passes; title matches the CI semantic rules.
- [ ] Body has Motivation + Modifications and links the issue/PIP.
- [ ] No rebase after the upstream PR opened.
- [ ] Each push / PR-open / comment was user-confirmed.
