# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in barrel_embed, please report it responsibly.

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
- barrel_embed core library
- Provider integrations (Ollama, OpenAI, local, etc.)
- Python execution layer

### Out of Scope

- Issues in dependencies (report to upstream)
- Issues in third-party embedding providers
- Social engineering attacks

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | Yes       |
| < 1.0   | No        |

## Security Best Practices

When using barrel_embed:

1. **API Keys**: Store API keys securely (environment variables, secrets manager)
2. **Python Isolation**: Consider using virtual environments
3. **Rate Limiting**: Respect provider rate limits
4. **Input Validation**: Sanitize user-provided text before embedding
5. **Updates**: Keep dependencies updated

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who report valid vulnerabilities (unless they prefer to remain anonymous).
