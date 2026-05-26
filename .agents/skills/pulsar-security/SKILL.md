---
name: pulsar-security
description: AI guardrails for security topics — Pulsar's threat scope, private vulnerability reporting, and checking exposure to an already-public CVE. Use for a suspected vulnerability, a CVE, or deciding whether some behaviour is in scope.
---

# Pulsar security

AI-tooling layer over [`SECURITY.md`](../../../SECURITY.md) — the policy, the security model, and the
reporting / CVE-checking steps. Load it for the full detail; the points below are how an agent should
act.

## Guardrails

- **Know the threat scope before calling something a vulnerability**
  ([SECURITY.md → Security model](../../../SECURITY.md#security-model-and-scope)): Pulsar Functions and
  connectors run **fully trusted code** (RCE is the feature, not a bug), and clusters assume
  network-perimeter security with **no protection against malicious DoS**. Don't classify "a trusted
  function runs / modifies its environment" or "a perimeter-trusted client causes a DoS" as a bug by
  default — surface uncertainty instead of asserting a CVE. **Never file a security report or open a
  security issue autonomously**; a human must independently verify it and own it.
- **Never disclose a suspected vulnerability in public** — not in an issue, PR, commit, or mailing
  list, and never state the security nature of a fix. Report privately to `security@apache.org`
  (optionally cc `private@pulsar.apache.org`).
- **Don't write or push the fix yourself.** The project team commits it through the ASF process; a
  reporter may include a suggested patch privately in the report.
- **An already-public CVE is *not* a private disclosure.** First search PRs/issues (including closed)
  by the CVE id and check the shipped version in `gradle/libs.versions.toml`; then ask via a GitHub
  issue or `dev@pulsar.apache.org`. Don't fabricate "fixed" / "not affected" — verify against merged
  PRs and shipped versions.
