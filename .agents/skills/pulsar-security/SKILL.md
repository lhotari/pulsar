---
name: pulsar-security
description: Guardrails for handling security topics in Apache Pulsar — Pulsar's (informal) security model and threat scope, reporting a vulnerability through the private ASF channels (never a public issue, PR, or commit), and checking whether Pulsar is exposed to an already-public CVE. Use when a task involves a suspected vulnerability, a CVE, security advisories, deciding whether some behaviour (e.g. functions running code, or DoS) is in scope, or questions about supported/patched versions.
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

Use it when a task touches a suspected or reported vulnerability, a CVE, a security advisory,
deciding whether some behaviour is in scope, or questions about which Pulsar versions are
supported/patched.

## Security model (scope)

Pulsar's security model is not formally defined. Two assumptions decide whether a report is in scope
(full text in [`SECURITY.md` → Security model and scope](../../../SECURITY.md#security-model-and-scope)):

- **Functions and connectors run fully trusted code.** The function instance runtime's purpose is to
  execute user-provided code — **RCE is the feature, not a bug** (the PMC repeatedly receives reports
  to the contrary). The thread/process runtimes can touch any files/state they can access; the
  Kubernetes runtime alone does not restrict cluster resource access; hardening hooks exist but the
  hardening itself is not shipped.
- **Clusters assume network-perimeter security.** Only trusted users should reach the cluster. There
  is **no explicit protection against malicious DoS**; rate limits address only *unintentional* DoS
  (misconfiguration, thundering herd).

When triaging, do **not** classify "a trusted function executes/modifies its environment" or "a
perimeter-trusted client can cause a denial-of-service" as a vulnerability by default — these are
outside the model. Surface uncertainty to a human (or the channels above) rather than asserting a CVE.

## Reporting a (non-public) vulnerability

- **Never disclose a vulnerability in a public channel** — not in a GitHub issue, PR title/body,
  commit message, or mailing list. Public disclosure before a fix is released puts users at risk.
- Report privately to the ASF Security Team (`security@apache.org`), optionally copying
  `private@pulsar.apache.org`. Follow <https://pulsar.apache.org/security/#security-policy> and
  <https://github.com/apache/pulsar/security/policy>.
- **Do not push, commit publicly, or open a PR for the fix yourself.** You may share a suggested fix
  patch privately by including it in the report to `private@pulsar.apache.org` — never via a public
  commit or PR.

The **project team** commits the fix, coordinated through the ASF security vulnerability handling
process
([apache.org/security/committers.html#possible](https://apache.org/security/committers.html#possible)).
The team may commit the fix to the public repository **before** the release, using a neutral commit
message that does not state its security nature. In severe cases, the commit and release are made in a
private repository, and the fix is made public only at the time of the release.

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
