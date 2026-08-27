# Security Policy

## Reporting a vulnerability

Please do not open a public issue for security vulnerabilities.

Report them privately through GitHub's [security advisory](https://github.com/woven-by-toyota/neco-dq-framework/security/advisories/new) feature instead.

When you report, include what you found, how to reproduce it, and the impact you expect. A suggested fix helps but isn't required.

We aim to acknowledge a report within a few business days and will keep you posted as we work on a fix.

## How the framework handles data

A couple of design choices that matter for security:

- It does not store credentials. Connection and secret handling is left to your Spark or Databricks environment.
- It reads source data and writes only to its own metadata tables. It never writes back to the tables it validates.
- Results contain statistics, counts, and query templates, never raw row values. No data values leak into logs or stored results.
