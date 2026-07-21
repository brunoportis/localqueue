# Security Policy

## Supported versions

Security fixes are provided for the latest released version of `localqueue`.

| Version | Supported |
| --- | --- |
| Latest release | ✅ |
| Older releases | ❌ |

## Reporting a vulnerability

Please do not report security vulnerabilities through public GitHub issues.

Report vulnerabilities privately using GitHub Security Advisories through the repository's **Security** tab.

Include, when possible:

- the affected version;
- a minimal reproduction;
- the expected impact;
- any suggested mitigation.

You should receive an initial response within 72 hours. Please allow time for investigation, remediation, and coordinated disclosure before publishing details.

## Scope

Security reports may include issues affecting:

- SQLite database integrity;
- queue isolation or job deduplication;
- unsafe serialization or deserialization;
- Rust/Python boundary safety;
- release artifacts and supply-chain integrity.

This project currently does not offer a bug bounty program.
