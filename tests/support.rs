/// Legacy support module - use helpers module for new tests
///
/// This module is kept for backwards compatibility with existing tests.
/// New tests should use the `helpers` module instead.

#[allow(unused)]
pub use rusty_ulid::{Ulid, generate_ulid_string};
#[allow(unused)]
pub use serde::{Deserialize, Serialize};

#[allow(unused)]
pub use dynamo_table::setup;
#[allow(unused)]
pub use dynamo_table::{dynamodb_client, table::DynamoTable};

use tokio::sync::OnceCell;

/// Ensure DynamoDB client is initialized for tests
static TEST_INIT: OnceCell<()> = OnceCell::const_new();

/// Initialize DynamoDB client for tests (idempotent)
#[allow(unused)]
pub async fn init_test_client() {
    TEST_INIT
        .get_or_init(|| async {
            // Trigger auto-initialization
            let _ = dynamo_table::dynamodb_client().await;
        })
        .await;
}
