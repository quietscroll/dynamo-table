# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-30

Initial release of `dynamo_table` - a high-level DynamoDB table abstraction for Rust.

### Added

- **DynamoTable trait** for type-safe table abstraction
  - Partition key and optional sort key support
  - Generic key types (PK, SK) for type safety
  - Configurable table names and key field names

- **GSITable trait** for Global Secondary Index support
  - Query and scan operations on GSIs
  - Filter expression support

- **Batch operations** with automatic retry and exponential backoff
  - `batch_get` - Retrieve up to 100 items per request
  - `batch_write` - Write up to 25 items per request
  - `batch_delete` - Delete up to 25 items per request
  - Automatic chunking and parallel processing

- **Pagination support**
  - Cursor-based pagination with exclusive start keys
  - Configurable page sizes
  - Automatic pagination through large result sets

- **Streaming support**
  - Async streams for memory-efficient processing
  - `query_stream` and `reverse_query_stream` methods
  - Compatible with `futures` ecosystem

- **Query operations**
  - Basic query by partition key
  - Query with sort key
  - Query with filter expressions
  - Range queries (begins_with, between)
  - Reverse queries (descending order)

- **CRUD operations**
  - `add_item` / `put_item` - Insert or replace items
  - `get_item` - Retrieve single items
  - `update_item` - Update item attributes
  - `update_item_with_condition` - Conditional updates for optimistic locking
  - `delete_item` / `destroy_item` - Remove items

- **Scan operations**
  - Full table scans
  - Scans with filter expressions
  - Paginated scanning

- **Error handling**
  - Comprehensive `Error` enum
  - Helper methods: `is_conditional_check_failed()`, `is_serialization_error()`, `is_dynamodb_error()`
  - Proper error conversion from AWS SDK errors

- **Reserved word validation** (debug mode)
  - Automatic validation against DynamoDB reserved words
  - Prevents runtime errors from using reserved field names
  - Only active in debug builds for zero runtime cost

- **Global client initialization**
  - Simple one-time initialization with `dynamo_table::init(&config)`
  - Custom client support with `init_with_client(client)`
  - Easy testing with mock clients

- **Type safety**
  - Automatic serialization/deserialization via `serde`
  - Compile-time validation of table schemas
  - Generic key types prevent type mismatches

### Technical Details

- Built on `aws-sdk-dynamodb` v1.x
- Async-first design with `tokio`
- Uses `serde_dynamo` for attribute value conversion
- Supports Rust 1.75+ (uses `impl Trait` in trait methods)
- Zero-cost abstractions where possible

### Dependencies

- `aws-config` ^1.0
- `aws-sdk-dynamodb` ^1.0
- `aws-smithy-runtime-api` ^1.0
- `aws-smithy-types-convert` ^0.60
- `futures-util` ^0.3
- `serde` ^1.0
- `serde_dynamo` ^4.2
- `tokio` ^1.0 (with `rt-multi-thread`, `time`, `sync` features)
- `tokio-stream` ^0.1

[0.1.0]: https://github.com/yourusername/dynamo_table/releases/tag/v0.1.0
