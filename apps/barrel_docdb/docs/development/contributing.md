# Contributing to Barrel DocDB

Thank you for your interest in contributing to Barrel DocDB!

## Getting Started

### Prerequisites

- Erlang/OTP 28+
- Git
- RocksDB development libraries

### Building from Source

```bash
# Clone the repository
git clone https://github.com/barrel-db/barrel_docdb.git
cd barrel_docdb

# Compile
rebar3 compile

# Run tests
rebar3 ct
```

## Development Workflow

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates

### Running Tests

```bash
# All tests
rebar3 ct

# Specific suite
rebar3 ct --suite=barrel_docdb_SUITE

# With coverage
rebar3 ct --cover
rebar3 cover
```

### Code Style

- Follow existing code conventions
- Use meaningful variable names
- Add `@doc` comments for public functions
- Keep functions small and focused

## Submitting Changes

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests to ensure they pass
5. Submit a merge request

### Commit Messages

Use clear, descriptive commit messages:

```
Add support for regex queries

- Implement regex condition in barrel_query
- Add tests for regex pattern matching
- Update documentation
```

## Reporting Issues

When reporting issues, please include:

- Barrel DocDB version
- Erlang/OTP version
- Steps to reproduce
- Expected vs actual behavior
- Relevant logs or error messages

## Questions?

- Open an issue on [GitHub](https://github.com/barrel-db/barrel_docdb/issues)
- Check existing documentation

## License

By contributing, you agree that your contributions will be licensed under Apache License 2.0.
