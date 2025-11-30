# Contributing to dynamo_table

Thank you for your interest in contributing to `dynamo_table`! This document provides guidelines and instructions for contributing.

## Getting Started

1. **Fork the repository** (once it's published on GitHub)
2. **Clone your fork**
3. **Create a feature branch**: `git checkout -b feature/my-feature`
4. **Make your changes**
5. **Run tests**: `cargo test`
6. **Run linter**: `cargo clippy`
7. **Format code**: `cargo fmt`
8. **Commit your changes**
9. **Push to your fork**
10. **Create a Pull Request**

## Development Setup

### Prerequisites

- Rust 1.75+ (uses `impl Trait` in trait methods)
- AWS SDK dependencies
- Tokio runtime

### Building

```bash
cargo build
```

### Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_name
```

### Linting

```bash
# Check for common mistakes
cargo clippy

# Apply automatic fixes
cargo clippy --fix
```

### Documentation

```bash
# Build and open documentation
cargo doc --open

# Check documentation
cargo doc --no-deps
```

## Priority Contributions

The project would especially benefit from:

### 1. API Refactoring (HIGH PRIORITY)

The most impactful contribution would be implementing the client parameter pattern. See `REFACTORING_GUIDE.md` for detailed instructions.

**Skills needed**: Rust, AWS SDK, async programming

**Estimated effort**: 15-20 hours

**Impact**: Makes the crate publishable

### 2. Testing

Add comprehensive tests:

- Unit tests with mocked DynamoDB client
- Integration tests (optional, requires localstack)
- Property-based tests for serialization/deserialization
- Edge case testing

**Skills needed**: Rust testing, mockall or similar mocking framework

### 3. Documentation

Improve documentation:

- Add more usage examples
- Document error handling patterns
- Add troubleshooting guide
- Create migration guide from other DynamoDB libraries

**Skills needed**: Technical writing, Rust

### 4. Features

Potential new features:

- TransactGetItems/TransactWriteItems support
- PartiQL query support
- Time To Live (TTL) support
- DynamoDB Streams integration
- Caching layer
- Metrics/observability integration

**Skills needed**: Varies by feature

## Code Style

### Formatting

- Use `rustfmt` with default settings
- Maximum line length: 100 characters
- Use 4 spaces for indentation

### Naming

- Use descriptive variable names
- Follow Rust naming conventions (snake_case for functions/variables, PascalCase for types)
- Prefix internal functions with `_` if not part of public API

### Documentation

- All public items must have doc comments
- Use `///` for outer doc comments
- Use `//!` for module-level documentation
- Include examples in doc comments where helpful
- Link to related items with `[OtherType]`

Example:
```rust
/// Retrieves an item from the DynamoDB table.
///
/// This method queries the table using the partition key and optional sort key,
/// returning `None` if the item doesn't exist.
///
/// # Arguments
///
/// * `client` - AWS DynamoDB client instance
/// * `partition_key` - The partition key value
/// * `sort_key` - Optional sort key value
///
/// # Errors
///
/// Returns [`Error::DynamoGetError`] if the DynamoDB operation fails.
///
/// # Example
///
/// ```no_run
/// # use dynamo_table::{DynamoTable, Error};
/// # use aws_sdk_dynamodb::Client;
/// # async fn example(client: &Client) -> Result<(), Error> {
/// # struct User { id: String }
/// # impl DynamoTable for User {
/// #     type PK = String;
/// #     type SK = ();
/// #     const TABLE: &'static str = "users";
/// #     const PARTITION_KEY: &'static str = "id";
/// #     fn primary_key(&self) -> Self::PK { self.id.clone() }
/// #     fn sort_key(&self) -> Self::SK { () }
/// # }
/// let user = User::get_item(&client, &"user123".to_string(), None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn get_item(/* ... */) -> Result<Option<Self>, Error> {
    // Implementation
}
```

### Error Handling

- Use `Result<T, Error>` for all fallible operations
- Provide context in error messages
- Use `?` operator for error propagation
- Add error helper methods when appropriate (e.g., `is_not_found()`)

### Testing

- Write tests for all public functions
- Use descriptive test names: `test_get_item_returns_none_when_not_found`
- Group related tests in modules
- Use `#[tokio::test]` for async tests

Example:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_item_succeeds() {
        let client = mock_client();
        let item = TestItem::new();

        let result = item.add_item(&client).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_item_returns_none_when_missing() {
        let client = mock_client_empty();

        let result = TestItem::get_item(&client, &"missing", None).await;

        assert!(result.unwrap().is_none());
    }
}
```

## Pull Request Process

1. **Update documentation**: Ensure README, CHANGELOG, and code docs are updated
2. **Add tests**: New features should include tests
3. **Update CHANGELOG.md**: Add entry under "Unreleased"
4. **Ensure CI passes**: All checks must pass (once CI is set up)
5. **Request review**: Tag maintainers for review
6. **Address feedback**: Respond to review comments
7. **Squash commits** (optional): Clean up commit history before merge

### PR Title Format

Use conventional commit format:

- `feat: add TransactWriteItems support`
- `fix: correct pagination cursor encoding`
- `docs: improve error handling examples`
- `refactor: extract retry logic to module`
- `test: add integration tests for batch operations`
- `chore: update dependencies`

### PR Description Template

```markdown
## Description
Brief description of changes

## Motivation
Why is this change needed?

## Changes
- Change 1
- Change 2

## Testing
How was this tested?

## Checklist
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Code formatted with rustfmt
- [ ] Clippy passes
- [ ] Builds successfully
```

## Release Process

(For maintainers)

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md` (move Unreleased to version number)
3. Commit: `git commit -m "chore: release v0.x.0"`
4. Tag: `git tag -a v0.x.0 -m "Release v0.x.0"`
5. Push: `git push && git push --tags`
6. Publish: `cargo publish`
7. Create GitHub release with changelog

## Code of Conduct

### Our Pledge

We are committed to providing a friendly, safe, and welcoming environment for all contributors.

### Our Standards

- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on what is best for the community
- Show empathy towards other community members
- Accept constructive criticism gracefully

### Unacceptable Behavior

- Harassment, trolling, or discriminatory behavior
- Personal attacks
- Publishing others' private information
- Other conduct which could reasonably be considered inappropriate

## Questions?

Feel free to:

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Reach out to maintainers directly

## License

By contributing, you agree that your contributions will be licensed under both MIT and Apache-2.0 licenses.
