# Security Policy

The authoritative policy is at <https://pulsar.apache.org/security/>; the Apache Software Foundation's
general process is at <https://www.apache.org/security/>. The summary below is what contributors (and
any AI tooling acting on their behalf) must follow.

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
