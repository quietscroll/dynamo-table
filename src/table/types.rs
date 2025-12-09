use aws_sdk_dynamodb::operation;
use serde_dynamo::from_items;
use std::{fmt, time::Duration};

use crate::table::DynamoTable;

/// Partition key type alias - now fully generic
pub type PartitionKey<PK> = PK;

/// Sort key type alias - now fully generic
pub type SortKey<SK> = Option<SK>;

/// Composite key type alias - now fully generic
pub type CompositeKey<PK, SK> = (PK, SortKey<SK>);

/// Generic query output with typed keys
#[must_use = "query results should be used or you'll lose the fetched data"]
#[derive(Clone, Debug)]
pub struct OutputItems<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// The items returned by the query
    pub items: Vec<T>,
    /// Initial query limit
    pub limit: u16,
    /// Count of items returned
    pub count: i32,
    /// Scanned count of items
    pub scanned_count: i32,
    /// The last evaluated key for pagination
    pub last_evaluated_key: Option<(T::PK, Option<T::SK>)>,
}

/// A typed pagination cursor for a table `T`.
#[must_use = "cursor should be used for pagination to fetch the next page"]
#[derive(Clone, Debug)]
pub struct Cursor<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// Partition key for the next page
    pub pk: T::PK,
    /// Optional sort key for the next page
    pub sk: Option<T::SK>,
}

impl<T> Cursor<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// Returns the partition key reference.
    pub fn partition_key(&self) -> &T::PK {
        &self.pk
    }

    /// Returns the optional sort key reference.
    pub fn sort_key(&self) -> Option<&T::SK> {
        self.sk.as_ref()
    }

    /// Convenience: the value to pass as `exclusive_start_key` to query APIs.
    pub fn exclusive_start_key(&self) -> Option<&T::SK> {
        self.sort_key()
    }
}

impl<T> OutputItems<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// Returns a typed cursor for starting the next page, if present.
    pub fn start_cursor(&self) -> Option<Cursor<T>> {
        self.last_evaluated_key
            .as_ref()
            .map(|(pk, sk)| Cursor::<T> {
                pk: pk.clone(),
                sk: sk.clone(),
            })
    }
}

impl<T> From<(operation::scan::ScanOutput, u16)> for OutputItems<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    fn from((output, limit): (operation::scan::ScanOutput, u16)) -> Self {
        if let Some(items) = output.items {
            let items: Vec<T> = from_items(items).unwrap_or_else(|e| {
                if cfg!(debug_assertions) {
                eprintln!("WARNING: Failed to deserialize scan results for table '{}': {}. This usually indicates a schema mismatch between the database and the model. Returning empty results.", T::TABLE, e);

                    panic!("Deserialization failed in debug mode for table '{}': {}", T::TABLE, e);
                }
                Vec::new()
            });
            let count = output.count;
            let scanned_count = output.scanned_count;

            let last_evaluated_key = if items.is_empty() || output.last_evaluated_key.is_none() {
                None
            } else {
                items.last().map(|i| i.composite_key())
            };

            Self {
                items,
                limit,
                count,
                scanned_count,
                last_evaluated_key,
            }
        } else {
            Self::default()
        }
    }
}

impl<T> From<(operation::query::QueryOutput, u16)> for OutputItems<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    fn from((output, limit): (operation::query::QueryOutput, u16)) -> Self {
        if let Some(items) = output.items {
            let items: Vec<T> = from_items(items).unwrap_or_else(|e| {
                if cfg!(debug_assertions) {
                eprintln!("WARNING: Failed to deserialize query results for table '{}': {}. This usually indicates a schema mismatch between the database and the model. Returning empty results.", T::TABLE, e);

                    panic!("Deserialization failed in debug mode for table '{}': {}", T::TABLE, e);
                }
                Vec::new()
            });
            let count = output.count;
            let scanned_count = output.scanned_count;

            let last_evaluated_key = if items.is_empty() || output.last_evaluated_key.is_none() {
                None
            } else {
                items.last().map(|i| i.composite_key())
            };

            Self {
                items,
                limit,
                count,
                scanned_count,
                last_evaluated_key,
            }
        } else {
            Self::default()
        }
    }
}

impl<T> Default for OutputItems<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    fn default() -> Self {
        Self {
            items: Vec::new(),
            limit: 0,
            count: 0,
            scanned_count: 0,
            last_evaluated_key: None,
        }
    }
}

/// Retry configuration for DynamoDB operations
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts for batch operations
    pub max_retries: usize,
    /// Initial delay before first retry (in milliseconds)
    pub initial_delay: Duration,
    /// Maximum delay between retries (in milliseconds)
    pub max_delay: Duration,
}
