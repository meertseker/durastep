# Security policy

## Supported versions

| version | supported |
| ------- | --------- |
| 0.1.x   | yes       |

## Reporting a vulnerability

Please do not open a public issue. Email **meertseker@gmail.com** with a
description, affected versions and, if possible, a reproduction. You should
get an acknowledgement within 72 hours and a fix or mitigation plan within 14
days for confirmed issues. Coordinated disclosure is appreciated; you will be
credited unless you prefer otherwise.

## Scope notes

- durastep stores worker inputs, outputs and error messages in your database
  as-is. Do not put secrets in step inputs; pass references and resolve them
  inside the worker.
- `tenantId` scopes idempotency keys, quotas and filters. It is not an access
  control mechanism; authorise callers in your application before calling
  `startSession` or reading runs.
- Error messages are truncated to 2000 characters before being stored, but
  are otherwise not redacted.
