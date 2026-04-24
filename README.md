# DynamoDB Table Abstraction

[![CI](https://github.com/quietscroll/dynamo-table/actions/workflows/ci.yml/badge.svg)](https://github.com/quietscroll/dynamo-table/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/dynamo_table)](https://docs.rs/dynamo_table)

A high-level, type-safe DynamoDB table abstraction for Rust with support for batch operations, pagination, Global Secondary Indexes, and more.

## Features

- **Type-safe**: Leverage Rust's type system with `serde` for automatic serialization
- **Async-first**: Built on `tokio` and `aws-sdk-dynamodb`
- **Auto-initialization**: Client automatically initializes with sensible defaults on first use
- **Batch operations**: Efficiently process multiple items with automatic batching and retry logic
- **Streaming**: Handle large result sets with async streams
- **Pagination**: Built-in cursor-based pagination support
- **Reserved word validation**: Debug-mode checks for DynamoDB reserved words
- **GSI support**: Query and scan Global Secondary Indexes
- **Optimistic locking**: Conditional expression support for safe concurrent updates
- **Automatic retries**: Exponential backoff with adaptive retry mode
- **Simple initialization**: Global client pattern with easy setup or auto-initialization
- **Consumed capacity tracking**: Global atomic counters for read/write capacity units across every operation, with `tracing`-based logging and shutdown reporting via `Drop`

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
dynamo_table = "0.3"
aws-config = "1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Quick Start

### Option 1: Auto-Initialization (Recommended for Getting Started)

The client automatically initializes with sensible defaults on first use:

```rust
use dynamo_table::DynamoTable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    user_id: String,
    email: String,
    name: String,
}

impl DynamoTable for User {
    type PK = String;
    type SK = String;

    const TABLE: &'static str = "users";
    const PARTITION_KEY: &'static str = "user_id";
    const SORT_KEY: Option<&'static str> = None;

    fn partition_key(&self) -> Self::PK {
        self.user_id.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), dynamo_table::Error> {
    // No initialization needed! Just use the table methods directly
    let user = User {
        user_id: "user123".to_string(),
        email: "user@example.com".to_string(),
        name: "John Doe".to_string(),
    };

    user.add_item().await?;

    let retrieved = User::get_item(&"user123".to_string(), None).await?;
    println!("Retrieved: {:?}", retrieved);

    Ok(())
}
```

Auto-initialization provides:
- Adaptive retry mode with 3 max attempts
- Exponential backoff starting at 1 second
- Connect timeout: 3 seconds
- Read timeout: 20 seconds
- Operation timeout: 60 seconds
- LocalStack support via `AWS_PROFILE=localstack` environment variable

### Option 2: Custom Initialization

For production applications, customize the client configuration:

```rust
use dynamo_table::{init, defaults, BehaviorVersion, Region};

#[tokio::main]
async fn main() {
    // Initialize once at application startup with custom config
    let config = defaults(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .load()
        .await;

    init(&config).await;

    // Now use your tables with the custom configuration!
}
```

Or with a custom client:

```rust
use dynamo_table::init_with_client;

#[tokio::main]
async fn main() {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_dynamodb::Client::new(&config);

    init_with_client(client).await;
}
```

## Core Concepts

### 1. Define Your Table

```rust
use dynamo_table::DynamoTable;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    user_id: String,
    email: String,
    name: String,
    created_at: i64,
}

impl DynamoTable for User {
    type PK = String;      // Partition key type
    type SK = String;      // Sort key type (use String for no sort key)

    const TABLE: &'static str = "users";
    const PARTITION_KEY: &'static str = "user_id";
    const SORT_KEY: Option<&'static str> = None;

    fn partition_key(&self) -> Self::PK {
        self.user_id.clone()
    }
}
```

### 2. Basic CRUD Operations

```rust
use dynamo_table::Error;

async fn crud_examples() -> Result<(), Error> {
    // CREATE - Add an item
    let user = User {
        user_id: "user123".to_string(),
        email: "user@example.com".to_string(),
        name: "John Doe".to_string(),
        created_at: 1234567890,
    };
    user.add_item().await?;

    // READ - Get a single item
    let retrieved = User::get_item(&"user123".to_string(), None).await?;
    if let Some(user) = retrieved {
        println!("Found user: {:?}", user);
    }

    // UPDATE - Update item fields
    use serde_json::json;

    let updates = json!({
        "name": "Jane Doe",
        "email": "jane@example.com"
    });

    user.update_item(updates).await?;

    // DELETE - Remove an item
    User::delete_item("user123".to_string(), None).await?;

    // Or delete using the item itself
    user.destroy_item().await?;

    Ok(())
}
```

### 3. Querying Data

```rust
async fn query_examples() -> Result<(), Error> {
    // Query all items with a partition key
    let result = User::query_items(
        &"user123".to_string(),
        None,    // No sort key filter
        Some(10), // Limit to 10 items
        None,    // No pagination cursor
    ).await?;

    for user in result.items {
        println!("User: {:?}", user);
    }

    // Check if there are more results
    if let Some(cursor) = result.start_cursor() {
        println!("More results available, cursor: {:?}", cursor);
    }

    // Query a single item by partition key
    let single = User::query_item(&"user123".to_string()).await?;

    // Count items for a partition key
    let count = User::count_items(&"user123".to_string()).await?;
    println!("Found {} users", count);

    Ok(())
}
```

### 4. Composite Keys (Partition + Sort Key)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    user_id: String,
    order_id: String,
    total: f64,
    status: String,
    created_at: String,
}

impl DynamoTable for Order {
    type PK = String;
    type SK = String;

    const TABLE: &'static str = "orders";
    const PARTITION_KEY: &'static str = "user_id";
    const SORT_KEY: Option<&'static str> = Some("order_id");

    fn partition_key(&self) -> Self::PK {
        self.user_id.clone()
    }

    fn sort_key(&self) -> Option<Self::SK> {
        Some(self.order_id.clone())
    }
}

async fn composite_key_examples() -> Result<(), Error> {
    // Get all orders for a user
    let orders = Order::query_items(
        &"user123".to_string(),
        None,
        Some(20),
        None,
    ).await?;

    // Get specific order
    let order = Order::get_item(
        &"user123".to_string(),
        Some(&"order456".to_string()),
    ).await?;

    // Query with sort key pattern - orders beginning with "2024-"
    let orders_2024 = Order::query_begins_with(
        &"user123".to_string(),
        "2024-",
        Some(10),
        None,
        true, // ascending order
    ).await?;

    // Query range - orders between two IDs
    let orders_range = Order::query_between(
        &"user123".to_string(),
        "order100",
        "order200",
        Some(50),
        None,
        true,
    ).await?;

    // Reverse query (newest first if sort key is timestamp)
    let recent_orders = Order::reverse_query_items(
        &"user123".to_string(),
        None,
        Some(5), // Last 5 orders
        None,
    ).await?;

    Ok(())
}
```

## Advanced Features

### Global Secondary Indexes (GSI)

Use GSI to query your data by alternate keys. For paginated GSI queries, pass `result.start_cursor()` back into the next call; DynamoDB requires the base-table PK in addition to the index key, so a raw string cursor is not sufficient:

```rust
use dynamo_table::GSITable;

impl GSITable for User {
    const GSI_PARTITION_KEY: &'static str = "email";
    const GSI_SORT_KEY: Option<&'static str> = None;

    fn gsi_partition_key(&self) -> String {
        self.email.clone()
    }

    fn gsi_sort_key(&self) -> Option<String> {
        None
    }
}

async fn gsi_examples() -> Result<(), Error> {
    // Query by email using GSI
    let result = User::query_gsi_items(
        "user@example.com".to_string(),
        None,
        Some(10),
        None,
    ).await?;

    // Request the next page by passing the typed cursor back in.
    let next_page = User::query_gsi_items(
        "user@example.com".to_string(),
        None,
        Some(10),
        result.start_cursor(),
    ).await?;

    // Query single item by GSI
    let user = User::query_gsi_item(
        "user@example.com".to_string(),
        None,
    ).await?;

    // Count items by GSI key
    let count = User::count_gsi_items("user@example.com".to_string()).await?;

    // Reverse query on GSI
    let reverse_page = User::reverse_query_gsi_items(
        "user@example.com".to_string(),
        None,
        Some(10),
        None,
    ).await?;

    Ok(())
}
```

### Batch Operations

Efficiently process multiple items in a single request:

```rust
async fn batch_examples() -> Result<(), Error> {
    // Batch Get - Retrieve multiple items
    let keys = vec![
        ("user1".to_string(), None),
        ("user2".to_string(), None),
        ("user3".to_string(), None),
    ];

    let result = User::batch_get(keys).await?;
    println!("Retrieved {} items", result.items.len());

    if !result.failed_keys.is_empty() {
        println!("Failed to retrieve: {:?}", result.failed_keys);
    }

    // Batch Write - Insert/update multiple items
    let new_users = vec![user1, user2, user3];
    let write_result = User::batch_upsert(new_users).await?;

    println!("Wrote {} items in {:?}",
        write_result.items.len(),
        write_result.execution_time
    );

    // Batch Delete - Remove multiple items
    let users_to_delete = vec![user1, user2];
    let delete_result = User::batch_delete(users_to_delete).await?;

    // Mixed batch - Write and delete in same call
    let batch_result = batch_write(
        vec![user_to_write1, user_to_write2],  // Items to put
        vec![user_to_delete1, user_to_delete2], // Items to delete
    ).await?;

    Ok(())
}
```

### Streaming Large Result Sets

Use streams to process large datasets without loading everything into memory:

```rust
use futures_util::StreamExt;

async fn streaming_examples() -> Result<(), Error> {
    // Stream query results
    let stream = User::query_stream(
        &"user123".to_string(),
        None,
        Some(100), // Page size
    );

    tokio::pin!(stream);

    let mut count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(user) => {
                println!("Processing user: {:?}", user);
                count += 1;
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }
    println!("Processed {} users total", count);

    // Stream with filter
    let stream_with_filter = User::query_stream_with_filter(
        &"user123".to_string(),
        None,
        Some(50),
        "status = :active".to_string(),
        json!({ ":active": "active" }),
    );

    Ok(())
}
```

### Filter Expressions

Apply filters to query results:

```rust
use serde_json::json;

async fn filter_examples() -> Result<(), Error> {
    // Query with filter
    let active_users = User::query_items_with_filter(
        &"user123".to_string(),
        None,
        Some(50),
        None,
        "status = :status AND created_at > :timestamp".to_string(),
        json!({
            ":status": "active",
            ":timestamp": 1700000000
        }),
    ).await?;

    // Scan with filter
    let premium_users = User::scan_items_with_filter(
        Some(100),
        None,
        "subscription_tier = :tier".to_string(),
        json!({ ":tier": "premium" }),
    ).await?;

    // GSI query with filter
    let filtered_gsi = User::query_gsi_items_with_filter(
        "user@example.com".to_string(),
        None,
        None,
        Some(20),
        true,
        "account_status = :status".to_string(),
        json!({ ":status": "verified" }),
    ).await?;

    let filtered_gsi_next_page = User::query_gsi_items_with_filter(
        "user@example.com".to_string(),
        None,
        filtered_gsi.start_cursor(),
        Some(20),
        true,
        "account_status = :status".to_string(),
        json!({ ":status": "verified" }),
    ).await?;

    Ok(())
}
```

### Conditional Updates (Optimistic Locking)

Prevent race conditions with conditional expressions:

```rust
use serde_json::json;

async fn conditional_update_examples() -> Result<(), Error> {
    // Only update if item exists and status is pending
    let updates = json!({
        "status": "active",
        "updated_at": 1234567890
    });

    let result = user.update_item_with_condition(
        updates,
        Some("attribute_exists(user_id) AND #status = :old_status".to_string()),
        Some(json!({ ":old_status": "pending" })),
    ).await;

    match result {
        Ok(_) => println!("Updated successfully"),
        Err(e) if e.is_conditional_check_failed() => {
            println!("Condition failed - concurrent modification or wrong state");
        }
        Err(e) => return Err(e),
    }

    // Prevent overwriting existing items
    let new_user = User {
        user_id: "newuser".to_string(),
        email: "new@example.com".to_string(),
        name: "New User".to_string(),
    };

    match new_user.add_item_with_condition(
        Some("attribute_not_exists(user_id)".to_string()),
        None,
    ).await {
        Ok(_) => println!("Created new user"),
        Err(e) if e.is_conditional_check_failed() => {
            println!("User already exists");
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
```

### Atomic Counters

Increment numeric fields atomically:

```rust
async fn counter_examples() -> Result<(), Error> {
    // Increment single field
    User::increment_multiple(
        &"user123".to_string(),
        None,
        &[("login_count", 1)],
    ).await?;

    // Increment multiple fields atomically
    User::increment_multiple(
        &"user123".to_string(),
        None,
        &[
            ("login_count", 1),
            ("points", 10),
            ("streak", 1),
        ],
    ).await?;

    Ok(())
}
```

### Scanning Tables

Use scans sparingly for full table operations:

```rust
async fn scan_examples() -> Result<(), Error> {
    // Basic scan
    let result = User::scan_items(Some(100), None).await?;

    for user in result.items {
        println!("User: {:?}", user);
    }

    // Paginated scan
    let mut cursor = None;
    loop {
        let result = User::scan_items(Some(50), cursor).await?;

        // Process items
        for user in result.items {
            println!("Processing: {:?}", user);
        }

        // Check for more pages
        match result.start_cursor() {
            Some(next_cursor) => cursor = Some(next_cursor),
            None => break,
        }
    }

    Ok(())
}
```

## Configuration

### Table-Specific Configuration

Override defaults for individual tables:

```rust
use std::time::Duration;
use dynamo_table::RetryConfig;

impl DynamoTable for User {
    // Required fields...
    type PK = String;
    type SK = String;
    const TABLE: &'static str = "users";
    const PARTITION_KEY: &'static str = "user_id";

    fn partition_key(&self) -> Self::PK {
        self.user_id.clone()
    }

    // Optional: Custom page size (default: 10)
    const DEFAULT_PAGE_SIZE: u16 = 50;

    // Optional: Custom retry configuration (default: 2 retries)
    const BATCH_RETRIES_CONFIG: RetryConfig = RetryConfig {
        max_retries: 3,
        initial_delay: Duration::from_millis(200),
        max_delay: Duration::from_secs(5),
    };
}
```

### Custom Client Configuration

```rust
use dynamo_table::{RetryConfig, RetryMode, TimeoutConfig};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let timeout_config = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(30))
        .operation_timeout(Duration::from_secs(90))
        .build();

    let retry_config = RetryConfig::standard()
        .with_max_attempts(5)
        .with_initial_backoff(Duration::from_millis(500));

    let config = dynamo_table::defaults(dynamo_table::BehaviorVersion::latest())
        .region(dynamo_table::Region::new("us-east-1"))
        .retry_config(retry_config)
        .timeout_config(timeout_config)
        .load()
        .await;

    dynamo_table::init(&config).await;
}
```

## Testing

### LocalStack Support

The library automatically detects LocalStack when `AWS_PROFILE=localstack`.
Start LocalStack with DynamoDB enabled, then run the normal Rust test commands:

```bash
docker run --rm -d \
  --name dynamo-table-localstack \
  -p 4566:4566 \
  -e SERVICES=dynamodb \
  -e AWS_DEFAULT_REGION=us-east-1 \
  localstack/localstack:3

export AWS_PROFILE=localstack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

cargo check
cargo test
```

Stop the container when you are done:

```bash
docker stop dynamo-table-localstack
```

### GitHub Actions CI

This repository includes a GitHub Actions workflow at `.github/workflows/ci.yml`.
It starts a LocalStack service with DynamoDB enabled and runs:

```bash
cargo check
cargo test
```

The workflow sets the same environment variables used for local development:

```bash
AWS_PROFILE=localstack
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
AWS_REGION=us-east-1
```

### Custom Test Client

```rust
#[cfg(test)]
mod tests {
    use super::*;

    async fn test_client() {
        let config = aws_config::from_env()
            .endpoint_url("http://localhost:4566")
            .load()
            .await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        dynamo_table::init_with_client(client).await;
    }

    #[tokio::test]
    async fn test_user_operations() {
        test_client().await;

        let user = User {
            user_id: "test_user".to_string(),
            email: "test@example.com".to_string(),
            name: "Test User".to_string(),
        };

        user.add_item().await.unwrap();
        let retrieved = User::get_item(&user.user_id, None).await.unwrap();
        assert!(retrieved.is_some());
    }
}
```

## Error Handling

The library provides a comprehensive `Error` type with helper methods:

```rust
use dynamo_table::Error;

async fn error_handling_examples() -> Result<(), Error> {
    match User::update_item(updates).await {
        Ok(output) => println!("Success: {:?}", output),
        Err(e) if e.is_conditional_check_failed() => {
            // Handle optimistic locking failure
            println!("Concurrent modification detected");
        }
        Err(e) if e.is_serialization_error() => {
            // Handle serialization issues
            eprintln!("Data serialization failed: {}", e);
        }
        Err(e) if e.is_dynamodb_error() => {
            // Handle DynamoDB service errors
            eprintln!("DynamoDB error: {}", e);
        }
        Err(e) => return Err(e),
    }

    Ok(())
}
```

## Consumed Capacity Tracking

Enabled by default via the `consumed_capacity_stats` feature, the library records read and write capacity units consumed by every DynamoDB operation into global lock-free atomic counters.

### Querying Counters

```rust
use dynamo_table::{read_capacity_units, write_capacity_units, total_capacity_units, operation_count};

// After running some operations:
println!("Read CU:    {:.2}", read_capacity_units());
println!("Write CU:   {:.2}", write_capacity_units());
println!("Total CU:   {:.2}", total_capacity_units());
println!("Operations: {}", operation_count());
```

### Snapshot Struct

```rust
use dynamo_table::consumed_capacity_stats;

let stats = consumed_capacity_stats();
// stats.read_capacity_units  -> f64
// stats.write_capacity_units -> f64
// stats.total_capacity_units -> f64
// stats.operation_count      -> u64
```

### Logging with `tracing`

```rust
use dynamo_table::log_stats;

// Emit a structured tracing::info! event:
//   read_cu=1.5 write_cu=3.0 total_cu=4.5 operations=7 message="DynamoDB consumed capacity"
log_stats();
```

### Automatic Shutdown Logging via `Drop`

Hold a `CapacityStatsGuard` for the lifetime of your program. When it drops at program exit, it automatically calls `log_stats()`:

```rust
use dynamo_table::CapacityStatsGuard;

#[tokio::main]
async fn main() -> Result<(), dynamo_table::Error> {
    // Stats are emitted to tracing when this guard drops at the end of main
    let _stats_guard = CapacityStatsGuard::new();

    // ... run your application ...

    Ok(())
}
```

### Disabling the Feature

To opt out entirely and avoid the `tracing` dependency:

```toml
[dependencies]
dynamo_table = { version = "0.3", default-features = false }
```

## Performance Tips

1. **Use batch operations** for multiple items to reduce API calls and costs
2. **Enable streams** for large result sets to avoid memory issues
3. **Configure appropriate page sizes** based on your item size (default is 10)
4. **Use GSIs** for alternative query patterns instead of scans
5. **Leverage conditional expressions** to avoid race conditions
6. **Use projection expressions** to retrieve only needed attributes
7. **Monitor consumed capacity** during development to optimize costs
8. **Avoid scans** when possible - prefer query or batch get operations

## API Reference

### DynamoTable Trait Methods

**Basic Operations:**
- `add_item()` - Put an item into the table
- `get_item(pk, sk)` - Retrieve a single item
- `delete_item(pk, sk)` - Delete an item by key
- `destroy_item(self)` - Delete using the item itself
- `update_item(updates)` - Update an item
- `update_item_with_condition(...)` - Conditional update

**Query Operations:**
- `query_items(pk, sk, limit, cursor)` - Query items by partition key
- `query_item(pk)` - Query single item
- `reverse_query_items(...)` - Query in descending order
- `query_items_with_filter(...)` - Query with filter expression
- `query_stream(...)` - Stream query results
- `query_begins_with(...)` - Query with sort key prefix
- `query_between(...)` - Query sort key range
- `count_items(pk)` - Count items for partition key

**Batch Operations:**
- `batch_get(keys)` - Batch get multiple items
- `batch_upsert(items)` - Batch write multiple items
- `batch_delete(items)` - Batch delete multiple items

**Scan Operations:**
- `scan_items(limit, cursor)` - Scan the table
- `scan_items_with_filter(...)` - Scan with filter

**Utility:**
- `increment_multiple(pk, sk, fields)` - Atomic counter operations
- `remove_attributes(attributes)` - Remove one or more attributes from an item
- `dynamodb_client()` - Get client for this table (can be overridden)

### Consumed Capacity (`consumed_capacity_stats` feature)

**Global functions:**
- `read_capacity_units() -> f64` - Total read CU consumed since startup
- `write_capacity_units() -> f64` - Total write CU consumed since startup
- `total_capacity_units() -> f64` - Combined read + write CU
- `operation_count() -> u64` - Number of tracked operations
- `consumed_capacity_stats() -> CapacityStats` - Point-in-time snapshot
- `log_stats()` - Emit a `tracing::info!` event with all counters
- `record(&ConsumedCapacity)` - Manually record a capacity value
- `record_read(f64)` / `record_write(f64)` - Record capacity by type

**Types:**
- `CapacityStats` - Snapshot struct (`read_capacity_units`, `write_capacity_units`, `total_capacity_units`, `operation_count`)
- `CapacityStatsGuard` - Calls `log_stats()` when dropped; hold for program lifetime

### GSITable Trait Methods

- `query_gsi_items(...)` - Query using Global Secondary Index with `Option<Cursor<T>>` pagination
- `query_gsi_item(...)` - Query single item by GSI
- `reverse_query_gsi_items(...)` - Reverse query on GSI with `Option<Cursor<T>>` pagination
- `query_gsi_items_with_filter(...)` - Query GSI with filter and `Option<Cursor<T>>` pagination
- `count_gsi_items(...)` - Count items by GSI key

## Examples

See the `examples/` directory for more comprehensive examples:
- `batch_write_with_retry.rs` - Advanced batch operation patterns

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

This library was extracted from a production application's storage layer and represents battle-tested patterns for DynamoDB access in Rust. It emphasizes type safety, ergonomics, and production-ready defaults while maintaining flexibility for custom configurations.
