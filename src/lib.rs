//! # DynamoDB Table Abstraction
//!
//! A high-level, type-safe DynamoDB table abstraction for Rust with support for:
//! - Batch operations (get, write, delete)
//! - Pagination and streaming
//! - Global Secondary Indexes (GSI)
//! - Conditional expressions
//! - Optimistic locking
//! - Automatic retry with exponential backoff
//!
//! ## Features
//!
//! - **Type-safe**: Leverage Rust's type system with `serde` for automatic serialization
//! - **Async-first**: Built on `tokio` and `aws-sdk-dynamodb`
//! - **Batch operations**: Efficiently process multiple items with automatic batching
//! - **Streaming**: Handle large result sets with async streams
//! - **Reserved word validation**: Debug-mode checks for DynamoDB reserved words
//! - **GSI support**: Query and scan Global Secondary Indexes
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use dynamo_table::{DynamoTable, Error};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct User {
//!     user_id: String,
//!     email: String,
//!     name: String,
//! }
//!
//! impl DynamoTable for User {
//!     type PK = String;
//!     type SK = String;
//!
//!     const TABLE: &'static str = "users";
//!     const PARTITION_KEY: &'static str = "user_id";
//!     const SORT_KEY: Option<&'static str> = None;
//!
//!     fn partition_key(&self) -> Self::PK {
//!         self.user_id.clone()
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     // Initialize the global DynamoDB client
//!     let config = aws_config::defaults(aws_config::BehaviorVersion::latest()).load().await;
//!     dynamo_table::init(&config).await;
//!
//!     // Put an item
//!     let user = User {
//!         user_id: "123".to_string(),
//!         email: "user@example.com".to_string(),
//!         name: "John Doe".to_string(),
//!     };
//!     user.add_item().await?;
//!
//!     // Get an item
//!     let retrieved = User::get_item(&"123".to_string(), None).await?;
//!
//!     // Query items
//!     let result = User::query_items(&"123".to_string(), None, None, None).await?;
//!
//!     Ok(())
//! }
//! ```
#![deny(
    warnings,
    bad_style,
    dead_code,
    improper_ctypes,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true,
    missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    deprecated,
    unknown_lints,
    unreachable_code,
    unused_mut
)]

mod error;
pub use error::Error;

/// Generic table module
pub mod table;

/// Methods of Generic table
pub mod methods;

/// Table setup utilities for testing
pub mod setup;

// Re-export main types for convenience
pub use methods::DynamoTableMethods;
pub use table::{CompositeKey, DynamoTable, GSITable};

// Re-export aws-config types for configuration
pub use aws_config::{
    BehaviorVersion, Region, SdkConfig, defaults,
    meta::region::{ProvideRegion, RegionProviderChain},
    retry::{RetryConfig, RetryMode},
    timeout::TimeoutConfig,
};

// Re-export aws-types for advanced configuration
pub use aws_types::sdk_config::Builder as SdkConfigBuilder;

use aws_sdk_dynamodb::Client as DynamoDbClient;
use tokio::sync::OnceCell;

/// Global DynamoDB client instance
static GLOBAL_CLIENT: OnceCell<DynamoDbClient> = OnceCell::const_new();

/// Initialize the global DynamoDB client with default sensible settings
///
/// This is called automatically by `dynamodb_client()` if not already initialized.
/// It configures:
/// - Adaptive retry mode with 3 max attempts
/// - Exponential backoff starting at 1 second
/// - Connect timeout: 3 seconds
/// - Read timeout: 20 seconds
/// - Operation timeout: 60 seconds
/// - LocalStack support via AWS_PROFILE=localstack
///
/// Note: This function is internal. Use `init()` or `init_with_client()` for
/// custom configuration, or let `dynamodb_client()` auto-initialize with defaults.
async fn aws_config_defaults() -> SdkConfig {
    use aws_config::BehaviorVersion;
    use aws_types::sdk_config::{RetryConfig, TimeoutConfig};
    use std::time::Duration;

    let timeout_config = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(3))
        .read_timeout(Duration::from_secs(20))
        .operation_timeout(Duration::from_secs(60))
        .build();

    let mut loader = defaults(BehaviorVersion::latest())
        .retry_config(
            RetryConfig::adaptive()
                .with_max_attempts(3)
                .with_initial_backoff(Duration::from_secs(1)),
        )
        .timeout_config(timeout_config);

    // Support LocalStack via AWS_PROFILE=localstack
    if std::env::var("AWS_PROFILE").unwrap_or_default() == "localstack" {
        loader = loader.endpoint_url("http://127.0.0.1:4566");
    }

    loader.load().await
}

/// Initialize the global DynamoDB client with a custom AWS config
///
/// Use this when you need custom AWS configuration beyond the defaults.
///
/// # Example
///
/// ```rust,no_run
/// #[tokio::main]
/// async fn main() {
///     let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
///         .region(aws_config::Region::new("us-west-2"))
///         .load()
///         .await;
///     dynamo_table::init(&config).await;
///
///     // Now you can use table operations
/// }
/// ```
pub async fn init(config: &SdkConfig) {
    let _ = GLOBAL_CLIENT
        .get_or_init(|| async { DynamoDbClient::new(config) })
        .await;
}

/// Initialize the global DynamoDB client with a custom client instance
///
/// Useful for testing or when you need fine-grained control over client configuration.
///
/// # Example
///
/// ```rust,no_run
/// use aws_sdk_dynamodb::Client;
///
/// #[tokio::main]
/// async fn main() {
///     let config = aws_config::load_from_env().await;
///     let client = Client::new(&config);
///     dynamo_table::init_with_client(client).await;
/// }
/// ```
pub async fn init_with_client(client: DynamoDbClient) {
    let _ = GLOBAL_CLIENT.get_or_init(|| async { client }).await;
}

/// Get a reference to the global DynamoDB client
///
/// Automatically initializes the client with sensible defaults if not already initialized.
/// For custom configuration, call [`init`] or [`init_with_client`] before using this function.
///
/// # Auto-Initialization
///
/// If not explicitly initialized, this function will automatically configure:
/// - Adaptive retry mode with 3 max attempts
/// - Exponential backoff starting at 1 second
/// - Connect timeout: 3 seconds
/// - Read timeout: 20 seconds
/// - Operation timeout: 60 seconds
/// - LocalStack support via AWS_PROFILE=localstack
///
/// # Example
///
/// ```rust,no_run
/// # async fn example() {
/// // Client auto-initializes with defaults on first use
/// let client = dynamo_table::dynamodb_client().await;
/// // Use client for custom operations
/// # }
/// ```
///
/// # Custom Configuration Example
///
/// ```rust,no_run
/// # async fn example() {
/// // Initialize with custom config before first use
/// let config = dynamo_table::defaults(dynamo_table::BehaviorVersion::latest())
///     .region(dynamo_table::Region::new("us-west-2"))
///     .load()
///     .await;
/// dynamo_table::init(&config).await;
///
/// // Now uses custom configuration
/// let client = dynamo_table::dynamodb_client().await;
/// # }
/// ```
pub async fn dynamodb_client() -> &'static DynamoDbClient {
    // Safe to unwrap because init_defaults() always sets it
    GLOBAL_CLIENT
        .get_or_init(|| async {
            let config = aws_config_defaults().await;
            DynamoDbClient::new(&config)
        })
        .await
}

#[cfg(debug_assertions)]
pub(crate) fn assert_not_reserved_key(key: &str) {
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
    #[rustfmt::skip]
    const KEYS: [&str; 573] = [
"abort", "absolute", "action", "add", "after", "agent", "aggregate", "all", "allocate", "alter", "analyze", "and", "any", "archive", "are", "array", "as", "asc", "ascii", "asensitive", "assertion", "asymmetric", "at", "atomic", "attach", "attribute", "auth", "authorization", "authorize", "auto", "avg", "back", "backup", "base", "batch", "before", "begin", "between", "bigint", "binary", "bit", "blob", "block", "boolean", "both", "breadth", "bucket", "bulk", "by", "byte", "call", "called", "calling", "capacity", "cascade", "cascaded", "case", "cast", "catalog", "char", "character", "check", "class", "clob", "close", "cluster", "clustered", "clustering", "clusters", "coalesce", "collate", "collation", "collection", "column", "columns", "combine", "comment", "commit", "compact", "compile", "compress", "condition", "conflict", "connect", "connection", "consistency", "consistent", "constraint", "constraints", "constructor", "consumed", "continue", "convert", "copy", "corresponding", "count", "counter", "create", "cross", "cube", "current", "cursor", "cycle", "data", "database", "date", "datetime", "day", "deallocate", "dec", "decimal", "declare", "default", "deferrable", "deferred", "define", "defined", "definition", "delete", "delimited", "depth", "deref", "desc", "describe", "descriptor", "detach", "deterministic", "diagnostics", "directories", "disable", "disconnect", "distinct", "distribute", "do", "domain", "double", "drop", "dump", "duration", "dynamic", "each", "element", "else", "elseif", "empty", "enable", "end", "equal", "equals", "error", "escape", "escaped", "eval", "evaluate", "exceeded", "except", "exception", "exceptions", "exclusive", "exec", "execute", "exists", "exit", "explain", "explode", "export", "expression", "extended", "external", "extract", "fail", "false", "family", "fetch", "fields", "file", "filter", "filtering", "final", "finish", "first", "fixed", "flattern", "float", "for", "force", "foreign", "format", "forward", "found", "free", "from", "full", "function", "functions", "general", "generate", "get", "glob", "global", "go", "goto", "grant", "greater", "group", "grouping", "handler", "hash", "have", "having", "heap", "hidden", "hold", "hour", "identified", "identity", "if", "ignore", "immediate", "import", "in", "including", "inclusive", "increment", "incremental", "index", "indexed", "indexes", "indicator", "infinite", "initially", "inline", "inner", "innter", "inout", "input", "insensitive", "insert", "instead", "int", "integer", "intersect", "interval", "into", "invalidate", "is", "isolation", "item", "items", "iterate", "join", "key", "keys", "lag", "language", "large", "last", "lateral", "lead", "leading", "leave", "left", "length", "less", "level", "like", "limit", "limited", "lines", "list", "load", "local", "localtime", "localtimestamp", "location", "locator", "lock", "locks", "log", "loged", "long", "loop", "lower", "map", "match", "materialized", "max", "maxlen", "member", "merge", "method", "metrics", "min", "minus", "minute", "missing", "mod", "mode", "modifies", "modify", "module", "month", "multi", "multiset", "name", "names", "national", "natural", "nchar", "nclob", "new", "next", "no", "none", "not", "null", "nullif", "number", "numeric", "object", "of", "offline", "offset", "old", "on", "online", "only", "opaque", "open", "operator", "option", "or", "order", "ordinality", "other", "others", "out", "outer", "output", "over", "overlaps", "override", "owner", "pad", "parallel", "parameter", "parameters", "partial", "partition", "partitioned", "partitions", "path", "percent", "percentile", "permission", "permissions", "pipe", "pipelined", "plan", "pool", "position", "precision", "prepare", "preserve", "primary", "prior", "private", "privileges", "procedure", "processed", "project", "projection", "property", "provisioning", "public", "put", "query", "quit", "quorum", "raise", "random", "range", "rank", "raw", "read", "reads", "real", "rebuild", "record", "recursive", "reduce", "ref", "reference", "references", "referencing", "regexp", "region", "reindex", "relative", "release", "remainder", "rename", "repeat", "replace", "request", "reset", "resignal", "resource", "response", "restore", "restrict", "result", "return", "returning", "returns", "reverse", "revoke", "right", "role", "roles", "rollback", "rollup", "routine", "row", "rows", "rule", "rules", "sample", "satisfies", "save", "savepoint", "scan", "schema", "scope", "scroll", "search", "second", "section", "segment", "segments", "select", "self", "semi", "sensitive", "separate", "sequence", "serializable", "session", "set", "sets", "shard", "share", "shared", "short", "show", "signal", "similar", "size", "skewed", "smallint", "snapshot", "some", "source", "space", "spaces", "sparse", "specific", "specifictype", "split", "sql", "sqlcode", "sqlerror", "sqlexception", "sqlstate", "sqlwarning", "start", "state", "static", "status", "storage", "store", "stored", "stream", "string", "struct", "style", "sub", "submultiset", "subpartition", "substring", "subtype", "sum", "super", "symmetric", "synonym", "system", "table", "tablesample", "temp", "temporary", "terminated", "text", "than", "then", "throughput", "time", "timestamp", "timezone", "tinyint", "to", "token", "total", "touch", "trailing", "transaction", "transform", "translate", "translation", "treat", "trigger", "trim", "true", "truncate", "ttl", "tuple", "type", "under", "undo", "union", "unique", "unit", "unknown", "unlogged", "unnest", "unprocessed", "unsigned", "until", "update", "upper", "url", "usage", "use", "user", "users", "using", "uuid", "vacuum", "value", "valued", "values", "varchar", "variable", "variance", "varint", "varying", "view", "views", "virtual", "void", "wait", "when", "whenever", "where", "while", "window", "with", "within", "without", "work", "wrapped", "write", "year", "zone "
];

    debug_assert!(!KEYS.contains(&key), "Reserved key: {key}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_key_tuple() {
        let key: CompositeKey<String, String> =
            ("user123".to_string(), Some("order456".to_string()));
        assert_eq!(key.0, "user123");
        assert_eq!(key.1, Some("order456".to_string()));
    }

    #[test]
    fn test_composite_key_no_sort_key() {
        let key: CompositeKey<String, String> = ("user123".to_string(), None);
        assert_eq!(key.0, "user123");
        assert_eq!(key.1, None);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "Reserved key: user")]
    fn test_assert_reserved_key_panics() {
        assert_not_reserved_key("user");
    }

    #[cfg(debug_assertions)]
    #[test]
    fn test_assert_not_reserved_key_ok() {
        assert_not_reserved_key("user_id");
        assert_not_reserved_key("custom_field");
    }
}
