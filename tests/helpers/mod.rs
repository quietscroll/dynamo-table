/// Test helpers and fixtures for storage integration tests
///
/// This module provides common test utilities, fixtures, and helper functions
/// used across all integration tests.
pub mod fixtures;

pub use dynamo_table::setup;
pub use dynamo_table::table::DynamoTable;
pub use serde::{Deserialize, Serialize};

// Re-export common fixtures
pub use fixtures::{TestCounters, TestObject};

#[allow(unused_imports)]
pub use fixtures::TestGSIObject;

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

/// Ensure DynamoDB client is initialized for tests
static TEST_INIT: OnceCell<()> = OnceCell::const_new();

/// Initialize DynamoDB client for tests (idempotent)
pub async fn init_test_client() {
    TEST_INIT
        .get_or_init(|| async {
            // Trigger auto-initialization
            let _ = dynamo_table::dynamodb_client().await;
        })
        .await;
}

/// Generate a unique test prefix for isolation
///
/// Returns a timestamp-based prefix to avoid test data conflicts
pub fn unique_test_prefix(name: &str) -> String {
    format!(
        "{}_{}_",
        name,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

/// Wait for eventual consistency
///
/// DynamoDB scan operations may not immediately reflect recent writes.
/// This helper adds a small delay for eventual consistency.
#[allow(dead_code)]
pub async fn wait_for_consistency() {
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
}

/// Setup a DynamoDB table for testing
///
/// Creates the table if it doesn't exist. Safe to call multiple times.
/// Automatically initializes the DynamoDB client if not already initialized.
#[allow(dead_code)]
pub async fn setup_table<T>() -> Result<(), dynamo_table::Error>
where
    T: DynamoTable,
    T::PK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
    T::SK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
{
    init_test_client().await;
    let _ = setup::table::<T>().await;
    Ok(())
}

/// Setup a DynamoDB table with GSI for testing
///
/// Creates the table with global secondary index if it doesn't exist.
/// Automatically initializes the DynamoDB client if not already initialized.
#[allow(dead_code)]
pub async fn setup_table_with_gsi<T>() -> Result<(), dynamo_table::Error>
where
    T: DynamoTable + dynamo_table::table::GSITable,
    T::PK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
    T::SK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
{
    init_test_client().await;
    let _ = setup::table_with_gsi::<T>().await;
    Ok(())
}
