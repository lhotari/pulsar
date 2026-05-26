# Security Policy

The authoritative policy is at <https://pulsar.apache.org/security/>; the Apache Software Foundation's
general process is at <https://www.apache.org/security/>. The summary below is what contributors (and
any AI tooling acting on their behalf) must follow.

## Security model and scope

Pulsar's security model is not formally/explicitly defined. Two long-standing design assumptions
matter when deciding whether something is actually a vulnerability:

**Pulsar Functions and connectors execute fully trusted code.** The function instance runtime exists
to run user-provided code — *remote code execution is its core purpose, not a flaw.* (The Pulsar PMC
has repeatedly received reports claiming that "the function instance runtime allows running
user-provided code and results in an RCE"; this is expected, by-design behaviour, not a security
issue.) The available execution models also let the code modify its environment:

- The **thread** and **process** runtimes can read or modify any files and state accessible to the
  process they run in.
- The **Kubernetes** runtime, on its own, does not restrict access to resources in the Kubernetes
  cluster. The project provides hooks for custom Kubernetes-runtime *hardening*, but such hardening is
  **not** part of the project.

Therefore, Pulsar Functions and connectors must only run code that is **fully trusted**.

**Clusters rely on network-perimeter security.** Pulsar is designed to be deployed behind a trusted
network perimeter where only trusted users can reach the cluster. The project does **not** implement
controls against malicious **denial-of-service (DoS)** attacks. Rate limiting exists to mitigate
*unintentional* DoS — e.g. improper configuration or thundering-herd effects — but it is **not** a
defense against a deliberate DoS by an attacker.

Reports that amount to "a trusted function can run code / modify its environment" or "a
perimeter-trusted client can cause a denial-of-service" generally fall **outside** Pulsar's threat
model. When in doubt, ask through the channels below rather than assuming.

## Reporting a vulnerability

Do **not** open a public GitHub issue, pull request, or discussion for a suspected vulnerability —
that defeats coordinated disclosure.

Report it privately by email to the Apache Security Team at **security@apache.org** (the ASF's central
security address). You may also, or instead, write to the Apache Pulsar PMC's private list,
**private@pulsar.apache.org**.

See <https://pulsar.apache.org/security/#security-policy> and
<https://github.com/apache/pulsar/security/policy> for more detail.

## Disclosure hygiene for contributors

Until a fix has been publicly announced, do **not** reveal the security nature of a change anywhere
public — commit messages, pull request titles or bodies, GitHub issues, or review comments — even
when the fix touches security-adjacent code (authentication/authorization, deserialization, TLS,
networking). Describe the behaviour change neutrally. A public commit or PR that advertises "fixes the
CVE", "security fix", or "patches the vulnerability" discloses the issue before it is announced and
defeats the coordinated-disclosure process above.

Moreover, a fix for a non-public security issue must **not** be pushed, committed publicly, or opened
as a PR until the Apache Pulsar PMC has responded with a go-ahead. The fix is coordinated through the
ASF security vulnerability handling process
([apache.org/security/committers.html#possible](https://apache.org/security/committers.html#possible))
— it must not run ahead of the coordinated-disclosure timeline.

This applies to every contributor, and identically to any AI tooling acting on a contributor's behalf.

## Checking exposure to an already-public CVE

For a CVE that is **already public** (for example, in a dependency) and you want to check Pulsar's
exposure or whether it is already fixed, this is **not** a private disclosure — the right venue is a
GitHub issue on apache/pulsar or a question on the **dev@pulsar.apache.org** mailing list.

Before asking, search PRs and issues with the CVE id at <https://github.com/apache/pulsar/pulls>
(also check issues and **closed** PRs/issues) — the fix may already be merged or the question already
answered.

## Supported versions

Pulsar's supported versions are listed at <https://pulsar.apache.org/contribute/release-policy/>.
Security fixes are made to supported versions; users should upgrade to a supported version to receive
security updates.
