# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in barrel_docdb, please report it responsibly.

**DO NOT** open a public issue for security vulnerabilities.

### How to Report

1. **Email**: Send details to security@barrel-db.eu
2. **Include**:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Any suggested fixes (optional)

### What to Expect

- **Acknowledgment**: Within 48 hours
- **Initial Assessment**: Within 5 business days
- **Resolution Timeline**: Depends on severity
  - Critical: 7 days
  - High: 14 days
  - Medium: 30 days
  - Low: 90 days

### Scope

This policy applies to:
- barrel_docdb core library
- HTTP API endpoints
- Replication protocols
- Storage layer

### Out of Scope

- Issues in dependencies (report to upstream)
- Self-hosted deployment misconfigurations
- Social engineering attacks

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.3.x   | Yes       |
| < 0.3   | No        |

## Security Best Practices

When deploying barrel_docdb:

1. **Network Security**: Use TLS for HTTP endpoints
2. **Authentication**: Enable authentication for production
3. **Access Control**: Restrict database access appropriately
4. **Updates**: Keep dependencies updated
5. **Monitoring**: Enable metrics and alerting

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who report valid vulnerabilities (unless they prefer to remain anonymous).
