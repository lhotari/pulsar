---
name: pulsar-security
description: Guardrails for handling security topics in Apache Pulsar — reporting a vulnerability through the private ASF channels (never a public issue, PR, or commit), and checking whether Pulsar is exposed to an already-public CVE. Use when a task involves a suspected vulnerability, a CVE, security advisories, or questions about supported/patched versions.
license: Apache-2.0
compatibility: claude, codex, copilot, cursor, gemini, aider
metadata:
  audience: contributors to apache/pulsar
  scope: ai-tooling-security-guardrails
---

# Pulsar security

Guardrails for security-related work. Canonical policy: [`SECURITY.md`](../../../SECURITY.md),
<https://pulsar.apache.org/security/>, and <https://www.apache.org/security/>.

## When to use this skill

Use it when a task touches a suspected or reported vulnerability, a CVE, a security advisory, or
questions about which Pulsar versions are supported/patched.

## Reporting a (non-public) vulnerability

- **Never disclose a vulnerability in a public channel** — not in a GitHub issue, PR title/body,
  commit message, or mailing list. Public disclosure before a fix is released puts users at risk.
- Report privately to the ASF Security Team (`security@apache.org`), optionally copying
  `private@pulsar.apache.org`. Follow <https://pulsar.apache.org/security/#security-policy> and
  <https://github.com/apache/pulsar/security/policy>.
- When a change fixes a non-public security issue, **do not state its security nature** in the commit
  message, PR title, or PR body.

## Checking exposure to an already-public CVE

For a CVE that is **already public** (e.g. in a dependency) and you want to know Pulsar's exposure or
whether it is already fixed:

1. **Search existing PRs and issues by the CVE id** at <https://github.com/apache/pulsar/pulls> —
   also check issues and **closed** PRs/issues, since the fix may already be merged or the question
   already answered.
2. If not found, ask via a **GitHub issue on apache/pulsar** or on **dev@pulsar.apache.org** (this is
   the right venue for already-public CVEs — it is not a private disclosure).
3. Check the dependency version against `gradle/libs.versions.toml` / the `pulsar-dependencies`
   platform to determine whether the affected version is actually shipped.
4. Supported versions are listed at <https://pulsar.apache.org/contribute/release-policy/>; users
   should upgrade to a supported version to receive security updates.

## Guardrails

- Treat the public-vs-private distinction as the deciding factor for *where* to communicate: public
  CVE → public issue/list; suspected new vulnerability → private ASF channels only.
- Don't fabricate CVE status — verify against merged PRs and the shipped dependency versions before
  concluding "fixed" or "not affected".
