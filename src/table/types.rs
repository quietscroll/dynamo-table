use aws_sdk_dynamodb::operation;
use aws_sdk_dynamodb::types::AttributeValue;
use serde::de::DeserializeOwned;
use serde_dynamo::{from_attribute_value, from_items};
use std::{collections::HashMap, fmt, time::Duration};

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

fn parse_last_evaluated_key<T>(
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
) -> Option<(T::PK, Option<T::SK>)>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
{
    let attributes = last_evaluated_key?;

    let pk = attributes.get(T::PARTITION_KEY).and_then(|value| {
        from_attribute_value::<_, T::PK>(value.clone())
            .map_err(|error| {
                eprintln!(
                    "WARNING: Failed to deserialize last_evaluated_key partition key '{}' for table '{}': {}",
                    T::PARTITION_KEY,
                    T::TABLE,
                    error
                );
                error
            })
            .ok()
    })?;

    let sk = if let Some(sort_key_name) = T::SORT_KEY {
        attributes.get(sort_key_name).and_then(|value| {
            from_attribute_value::<_, T::SK>(value.clone())
                .map_err(|error| {
                    eprintln!(
                        "WARNING: Failed to deserialize last_evaluated_key sort key '{}' for table '{}': {}",
                        sort_key_name,
                        T::TABLE,
                        error
                    );
                    error
                })
                .ok()
        })
    } else {
        None
    };

    Some((pk, sk))
}

impl<T> From<(operation::scan::ScanOutput, u16)> for OutputItems<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
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
            let last_evaluated_key = parse_last_evaluated_key::<T>(output.last_evaluated_key);

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
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
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
            let last_evaluated_key = parse_last_evaluated_key::<T>(output.last_evaluated_key);

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
