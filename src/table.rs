use aws_sdk_dynamodb::operation;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput;
use aws_sdk_dynamodb::operation::put_item::PutItemOutput;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConsumedCapacity, DeleteRequest, ItemCollectionMetrics, KeysAndAttributes,
    PutRequest, ReturnConsumedCapacity, ReturnItemCollectionMetrics, ReturnValue, Select,
    WriteRequest,
};
use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Serialize, de::DeserializeOwned};
use serde_dynamo::{from_attribute_value, from_item, from_items, to_item};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::time::Duration;
use tokio_stream::{self as stream, Stream};

use crate::assert_not_reserved_key;
use crate::error::Error;

/// Retry configuration for batch operations
mod retry_config {
    use std::time::Duration;

    /// Calculate retry delay with exponential backoff
    ///
    /// # Arguments
    /// * `attempt` - The retry attempt number (0-based)
    /// * `initial` - Initial delay duration
    /// * `max` - Maximum delay duration
    ///
    /// # Returns
    /// Duration to wait before retrying
    pub(super) fn retry_delay(attempt: usize, initial: Duration, max: Duration) -> Duration {
        let delay_ms = initial.as_millis() as u64 * 2u64.pow(attempt as u32);
        let capped_delay = delay_ms.min(max.as_millis() as u64);
        Duration::from_millis(capped_delay)
    }
}

/// Validation helpers for table operations
///
/// These validators check for DynamoDB reserved words in key names and field names
/// to prevent runtime errors. All validations only run in debug builds.
mod validation {
    use serde::Serialize;
    use serde_dynamo::{AttributeValue, to_item};

    use super::{DynamoTable, GSITable};
    use crate::assert_not_reserved_key;
    use std::{collections::HashMap, fmt};

    /// Validate a single key is not a reserved word
    #[inline]
    fn validate_key(key: &str) {
        if cfg!(debug_assertions) {
            assert_not_reserved_key(key);
        }
    }

    /// Validate an optional key is not a reserved word
    #[inline]
    fn validate_optional_key(key: Option<&str>) {
        if let Some(k) = key {
            validate_key(k);
        }
    }

    /// Validate reserved keys for a DynamoTable
    ///
    /// Checks that both partition key and optional sort key are not reserved words.
    pub(super) fn validate_table_keys<T>()
    where
        T: DynamoTable,
        T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
        T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    {
        validate_key(T::PARTITION_KEY);
        validate_optional_key(T::SORT_KEY);
    }

    /// Validate reserved keys for a GSITable
    ///
    /// Checks both the main table keys and the GSI-specific keys.
    pub(super) fn validate_gsi_keys<T>()
    where
        T: GSITable,
        T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
        T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    {
        validate_table_keys::<T>();
        validate_key(T::GSI_PARTITION_KEY);
        validate_optional_key(T::GSI_SORT_KEY);
    }

    /// Validate field names for update operations
    ///
    /// Ensures none of the field names are DynamoDB reserved words.
    pub(super) fn validate_field_names(field_names: &[&str]) {
        if cfg!(debug_assertions) {
            for field in field_names {
                validate_key(field);
            }
        }
    }

    /// Validate filter expression parameter names
    ///
    /// Ensures filter expression value keys (e.g., `:paramName`) are not reserved words.
    pub(super) fn validate_filter_expression_values<U: Serialize>(filter_expression_values: &U) {
        if cfg!(debug_assertions) {
            let filter_keys =
                to_item::<_, HashMap<String, AttributeValue>>(filter_expression_values)
                    .expect("valid serialization for validation");

            for key in filter_keys.keys() {
                validate_key(key);
            }
        }
    }
}

/// Key condition expression builder for DynamoDB queries
mod expressions {
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;

    pub(super) struct KeyConditionBuilder {
        expression: String,
        values: HashMap<String, AttributeValue>,
    }

    impl KeyConditionBuilder {
        pub(super) fn new() -> Self {
            Self {
                expression: String::new(),
                values: HashMap::new(),
            }
        }

        pub(super) fn with_partition_key(mut self, field: &str, value: String) -> Self {
            self.expression = format!("{field} = :hash_value");
            let _ = self
                .values
                .insert(":hash_value".to_string(), AttributeValue::S(value));
            self
        }

        pub(super) fn with_sort_key(mut self, field: &str, value: String) -> Self {
            if !self.expression.is_empty() {
                self.expression.push_str(" and ");
            }
            self.expression.push_str(&format!("{field} = :range_value"));
            let _ = self
                .values
                .insert(":range_value".to_string(), AttributeValue::S(value));
            self
        }

        pub(super) fn build(self) -> (String, HashMap<String, AttributeValue>) {
            (self.expression, self.values)
        }
    }
}

/// Shared query builder for DynamoDB operations
mod query_builder {
    use super::{DynamoTable, GSITable, expressions};
    use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
    use aws_sdk_dynamodb::types::{AttributeValue, Select};
    use std::collections::HashMap;
    use std::fmt;

    pub(super) struct QueryBuilder<'a> {
        table_name: &'a str,
        index_name: Option<String>,
        partition_key_field: &'a str,
        sort_key_field: Option<&'a str>,
    }

    impl<'a> QueryBuilder<'a> {
        /// Create builder for main table queries
        pub(super) fn for_table<T>() -> Self
        where
            T: DynamoTable,
            T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
            T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
        {
            Self {
                table_name: T::TABLE,
                index_name: None,
                partition_key_field: T::PARTITION_KEY,
                sort_key_field: T::SORT_KEY,
            }
        }

        /// Create builder for GSI queries
        pub(super) fn for_gsi<T>() -> Self
        where
            T: GSITable,
            T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
            T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
        {
            Self {
                table_name: T::TABLE,
                index_name: Some(T::global_index_name()),
                partition_key_field: T::GSI_PARTITION_KEY,
                sort_key_field: T::GSI_SORT_KEY,
            }
        }

        /// Create builder for custom index queries
        pub(super) fn for_index<T>(index_name: String) -> Self
        where
            T: DynamoTable,
            T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
            T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
        {
            Self {
                table_name: T::TABLE,
                index_name: Some(index_name),
                partition_key_field: T::PARTITION_KEY,
                sort_key_field: T::SORT_KEY,
            }
        }

        /// Build a DynamoDB query with common parameters
        pub(super) fn build_query(
            &self,
            client: &aws_sdk_dynamodb::Client,
            partition_key: String,
            sort_key: Option<String>,
            exclusive_start_key: Option<String>,
            limit: u16,
            scan_index_forward: bool,
        ) -> QueryFluentBuilder {
            // DynamoDB only allows `AllAttributes` on the base table; secondary indexes are limited
            // to the attributes projected onto the index. See:
            // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.SelectingAttributes.html
            let select = if self.index_name.is_some() {
                Select::AllProjectedAttributes
            } else {
                Select::AllAttributes
            };

            let mut builder = client
                .query()
                .table_name(self.table_name)
                .select(select)
                .set_return_consumed_capacity(None)
                .scan_index_forward(scan_index_forward)
                .limit(limit as i32);

            if let Some(ref index_name) = self.index_name {
                builder = builder.index_name(index_name);
            }

            // Handle exclusive start key
            if let Some(start_key) = exclusive_start_key {
                if let Some(sort_key_field) = self.sort_key_field {
                    builder = builder
                        .exclusive_start_key(
                            self.partition_key_field,
                            AttributeValue::S(partition_key.clone()),
                        )
                        .exclusive_start_key(sort_key_field, AttributeValue::S(start_key));
                }
            }

            // Build key condition expression
            let (condition_expr, condition_values) =
                self.build_key_condition(partition_key, sort_key);
            builder = builder.key_condition_expression(condition_expr);

            for (key, value) in condition_values {
                builder = builder.expression_attribute_values(key, value);
            }

            builder
        }

        /// Build count query for the configured table/index
        pub(super) fn build_count_query(
            &self,
            client: &aws_sdk_dynamodb::Client,
            partition_key: String,
        ) -> QueryFluentBuilder {
            let mut builder = client
                .query()
                .table_name(self.table_name)
                .select(Select::Count)
                .set_return_consumed_capacity(None);

            if let Some(ref index_name) = self.index_name {
                builder = builder.index_name(index_name);
            }

            let condition_expr = format!("{} = :hash_value", self.partition_key_field);
            builder = builder
                .key_condition_expression(condition_expr)
                .expression_attribute_values(":hash_value", AttributeValue::S(partition_key));

            builder
        }

        fn build_key_condition(
            &self,
            partition_key: String,
            sort_key: Option<String>,
        ) -> (String, HashMap<String, AttributeValue>) {
            let mut builder = expressions::KeyConditionBuilder::new()
                .with_partition_key(self.partition_key_field, partition_key);

            if let (Some(sort_key_field), Some(sort_value)) = (self.sort_key_field, sort_key) {
                builder = builder.with_sort_key(sort_key_field, sort_value);
            }

            builder.build()
        }
    }
}

/// Batch processing utilities
mod batch_processor {
    use crate::Error;
    use futures_util::{StreamExt, TryStreamExt};
    use std::{cmp, future::Future};
    use tokio_stream::{self as stream};

    /// Generic batch processor for handling chunking and concurrent execution
    #[allow(dead_code)]
    pub(super) struct BatchProcessor {
        chunk_size: usize,
        concurrency: usize,
    }

    impl BatchProcessor {
        #[allow(dead_code)]
        pub(super) fn new(chunk_size: usize, concurrency: usize) -> Self {
            Self {
                chunk_size,
                concurrency,
            }
        }

        /// Process items in batches with concurrent execution
        #[allow(dead_code)]
        pub(super) async fn process<T, R, F, Fut, O, M>(
            &self,
            items: Vec<T>,
            operation: F,
            output: O,
            merge_results: M,
        ) -> Result<O, Error>
        where
            F: Fn(Vec<T>) -> Fut + Send + Sync,
            Fut: Future<Output = Result<R, Error>> + Send,
            T: Send + Clone + 'static,
            R: Send,
            O: Send,
            M: Fn(&mut O, R) -> Result<(), Error> + Send + Sync,
        {
            if items.is_empty() {
                return Ok(output);
            }

            let batches: Vec<Vec<T>> = items
                .chunks(self.chunk_size)
                .map(|chunk| chunk.to_vec())
                .collect();

            let concurrency = cmp::max(1, batches.len().min(self.concurrency));

            stream::iter(batches.into_iter().map(operation))
                .buffer_unordered(concurrency)
                .map_err(Into::<Error>::into)
                .try_fold(output, |mut acc, result| {
                    let merge_results = &merge_results;
                    async move {
                        merge_results(&mut acc, result)?;
                        Ok(acc)
                    }
                })
                .await
        }
    }

    /// Standard batch sizes for DynamoDB operations
    pub(super) const BATCH_WRITE_SIZE: usize = 25;
    pub(super) const BATCH_READ_SIZE: usize = 100;
    pub(super) const DEFAULT_CONCURRENCY: usize = 10;
}

/// Partition key type alias - now fully generic
pub type PartitionKey<PK> = PK;

/// Sort key type alias - now fully generic
pub type SortKey<SK> = Option<SK>;

/// Composite key type alias - now fully generic
pub type CompositeKey<PK, SK> = (PK, SortKey<SK>);

/// Generic query output with typed keys - fully generic now!
///
/// This type contains the results of a query or scan operation, including
/// pagination support via `last_evaluated_key`.
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
    ///
    /// The number of items in the response.
    /// If you used a <code>QueryFilter</code> in the request, then <code>Count</code> is the number of items returned after the filter was applied, and <code>ScannedCount</code> is the number of matching items before the filter was applied.
    /// If you did not use a filter in the request, then <code>Count</code> and <code>ScannedCount</code> are the same.
    pub count: i32,

    /// Scanned count of items
    ///
    /// The number of items evaluated, before any <code>QueryFilter</code> is applied. A high <code>ScannedCount</code> value with few, or no, <code>Count</code> results indicates an inefficient <code>Query</code> operation. For more information, see <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Count">Count and ScannedCount</a> in the <i>Amazon DynamoDB Developer Guide</i>.
    /// If you did not use a filter in the request, then <code>ScannedCount</code> is the same as <code>Count</code>.
    pub scanned_count: i32,

    /// The last evaluated key for pagination
    ///
    /// The primary key of the item where the operation stopped, inclusive of the previous result set.
    /// Use this value to start a new operation, excluding this value in the new request.
    /// If <code>LastEvaluatedKey</code> is empty, then the "last page" of results has been processed and there is no more data to be retrieved.
    /// If <code>LastEvaluatedKey</code> is not empty, it does not necessarily mean that there is more data in the result set. The only way to know when you have reached the end of the result set is when <code>LastEvaluatedKey</code> is empty.
    pub last_evaluated_key: Option<(T::PK, Option<T::SK>)>,
}

/// A typed pagination cursor for a table `T`.
///
/// Contains the partition and sort key values needed to continue
/// pagination from where the previous query left off.
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
    ///
    /// Batch operations can fail partially due to throttling or capacity limits.
    /// This constant defines how many times we'll retry unprocessed items.
    pub max_retries: usize,

    /// Initial delay before first retry (in milliseconds)
    ///
    /// Uses exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
    pub initial_delay: Duration,

    /// Maximum delay between retries (in milliseconds)
    ///
    /// Caps the exponential backoff to prevent excessive delays
    pub max_delay: Duration,
}

/// Generic table trait - NOW FULLY GENERIC OVER PK AND SK TYPES!
pub trait DynamoTable: Serialize + DeserializeOwned + Send + Sync
where
    Self::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    Self::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// Associated partition key type
    type PK;

    /// Associated sort key type
    type SK;

    /// Name of table to retrieve
    const TABLE: &'static str;

    /// Partition key of the table
    const PARTITION_KEY: &'static str;

    /// Sort key of the table
    const SORT_KEY: Option<&'static str> = None;

    /// Default page size for queries
    const DEFAULT_PAGE_SIZE: u16 = 10;

    /// Retry configuration for operations
    const BATCH_RETRIES_CONFIG: RetryConfig = RetryConfig {
        max_retries: 2,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(2000),
    };

    /// Partition key name
    fn partition_key_name(&self) -> &'static str {
        Self::PARTITION_KEY
    }

    /// Sort key name
    fn sort_key_name(&self) -> Option<&'static str> {
        Self::SORT_KEY
    }

    /// Partition key - now returns the generic PK type!
    fn partition_key(&self) -> Self::PK;

    /// Sort key - now returns the generic SK type!
    fn sort_key(&self) -> Option<Self::SK> {
        None
    }

    /// Composite key - now returns the generic composite key!
    fn composite_key(&self) -> CompositeKey<Self::PK, Self::SK> {
        (self.partition_key(), self.sort_key())
    }

    /// Get the DynamoDB client for this table
    ///
    /// By default, returns the global client. Can be overridden for testing
    /// or to use a different client per table.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::DynamoTable;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() {
    ///     // Get the global client (auto-initializes if needed)
    ///     let client = User::dynamodb_client().await;
    ///
    ///     // Use client for custom operations
    ///     let _tables = client.list_tables().send().await;
    /// }
    /// ```
    fn dynamodb_client() -> impl Future<Output = &'static aws_sdk_dynamodb::Client> {
        crate::dynamodb_client()
    }

    /// Add an item to the table
    ///
    /// Inserts a new item or replaces an existing item with the same partition and sort key.
    /// This performs a PutItem operation in DynamoDB.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// #     name: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     let user = User {
    ///         user_id: "user123".to_string(),
    ///         email: "user@example.com".to_string(),
    ///         name: "John Doe".to_string(),
    ///     };
    ///
    ///     user.add_item().await?;
    ///     Ok(())
    /// }
    /// ```
    fn add_item(&self) -> impl Future<Output = Result<PutItemOutput, Error>> {
        add_item::<Self>(self)
    }

    /// Get an item from the table by its primary key
    ///
    /// Retrieves a single item using its partition key and optional sort key.
    /// Returns `None` if the item doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition key value
    /// * `sort_key` - Optional sort key value (required if table has a sort key)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     // Table with no sort key
    ///     let user = User::get_item(&"user123".to_string(), None).await?;
    ///
    ///     if let Some(user) = user {
    ///         println!("Found user: {:?}", user);
    ///     } else {
    ///         println!("User not found");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn get_item(
        partition_key: &Self::PK,
        sort_key: Option<&Self::SK>,
    ) -> impl Future<Output = Result<Option<Self>, Error>> {
        get_item::<Self>(partition_key, sort_key)
    }

    /// Query items from the table by partition key
    ///
    /// Retrieves all items matching the partition key. Optionally filter by sort key,
    /// limit results, and paginate using a cursor.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition key to query
    /// * `sort_key` - Optional exact sort key match
    /// * `limit` - Maximum number of items to return (default: 10)
    /// * `exclusive_start_key` - Pagination cursor from previous query
    ///
    /// # Returns
    ///
    /// Returns `OutputItems<Self>` containing:
    /// - `items`: Vec of matching items
    /// - `last_evaluated_key`: Pagination cursor for next page (if more results exist)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     // Query all items for a partition key
    ///     let result = User::query_items(
    ///         &"user123".to_string(),
    ///         None,    // No sort key filter
    ///         Some(20), // Limit to 20 items
    ///         None,    // No cursor
    ///     ).await?;
    ///
    ///     println!("Found {} items", result.items.len());
    ///
    ///     // Iterate through items
    ///     for user in result.items {
    ///         println!("User: {:?}", user);
    ///     }
    ///
    ///     // Check if there are more results
    ///     if result.last_evaluated_key.is_some() {
    ///         println!("More results available");
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn query_items(
        partition_key: &Self::PK,
        sort_key: Option<&Self::SK>,
        limit: Option<u16>,
        exclusive_start_key: Option<&Self::SK>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        _query_items(
            partition_key,
            sort_key,
            exclusive_start_key,
            limit,
            None,
            true,
        )
    }

    /// Get items from the table with filter expression
    fn query_items_with_filter<U: Serialize>(
        partition_key: &Self::PK,
        sort_key: Option<&Self::SK>,
        limit: Option<u16>,
        exclusive_start_key: Option<&Self::SK>,
        filter_expression: String,
        filter_expression_values: U,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_items_with_filter::<Self, U>(
            partition_key,
            sort_key,
            exclusive_start_key,
            limit,
            None,
            true,
            filter_expression,
            filter_expression_values,
        )
    }

    /// Get items from the table
    fn reverse_query_items(
        partition_key: &Self::PK,
        sort_key: Option<&Self::SK>,
        limit: Option<u16>,
        exclusive_start_key: Option<&Self::SK>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        _query_items(
            partition_key,
            sort_key,
            exclusive_start_key,
            limit,
            None,
            false,
        )
    }

    /// Get items from the table with filter expression (descending order)
    fn reverse_query_items_with_filter<U: Serialize>(
        partition_key: &Self::PK,
        sort_key: Option<&Self::SK>,
        limit: Option<u16>,
        exclusive_start_key: Option<&Self::SK>,
        filter_expression: String,
        filter_expression_values: U,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_items_with_filter::<Self, U>(
            partition_key,
            sort_key,
            exclusive_start_key,
            limit,
            None,
            false,
            filter_expression,
            filter_expression_values,
        )
    }

    /// Get an item from the table
    fn query_item(partition_key: &Self::PK) -> impl Future<Output = Result<Option<Self>, Error>> {
        query_item::<Self>(partition_key)
    }

    /// Delete an item from the table using the item itself
    ///
    /// Consumes the item and deletes it from DynamoDB.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     let user = User {
    ///         user_id: "user123".to_string(),
    ///         email: "user@example.com".to_string(),
    ///     };
    ///
    ///     user.destroy_item().await?;
    ///     Ok(())
    /// }
    /// ```
    fn destroy_item(self) -> impl Future<Output = Result<DeleteItemOutput, Error>> {
        let partition_key = self.partition_key();
        let sort_key = self.sort_key();
        delete_item::<Self>(partition_key, sort_key)
    }

    /// Delete an item from the table by its keys
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The partition key value
    /// * `sort_key` - Optional sort key value (required if table has a sort key)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     // Delete with partition key only
    ///     User::delete_item("user123".to_string(), None).await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    fn delete_item(
        partition_key: Self::PK,
        sort_key: Option<Self::SK>,
    ) -> impl Future<Output = Result<DeleteItemOutput, Error>> {
        delete_item::<Self>(partition_key, sort_key)
    }

    /// Update an item's fields
    ///
    /// Updates specific fields of an existing item. Only the fields present in the
    /// update object will be modified; other fields remain unchanged.
    ///
    /// # Arguments
    ///
    /// * `update` - Any serializable object containing the fields to update
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// use serde_json::json;
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// #     name: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     let user = User {
    ///         user_id: "user123".to_string(),
    ///         email: "old@example.com".to_string(),
    ///         name: "John Doe".to_string(),
    ///     };
    ///
    ///     // Update specific fields
    ///     let updates = json!({
    ///         "email": "new@example.com",
    ///         "name": "Jane Doe",
    ///     });
    ///
    ///     user.update_item(updates).await?;
    ///     Ok(())
    /// }
    /// ```
    fn update_item<T: Serialize + Send>(
        &self,
        update: T,
    ) -> impl Future<Output = Result<UpdateItemOutput, Error>> {
        let partition_key = self.partition_key();
        let sort_key = self.sort_key();
        update_item::<Self, T>(partition_key, sort_key, update)
    }

    /// Count items by partition key
    fn count_items(partition_key: &Self::PK) -> impl Future<Output = Result<usize, Error>> {
        count_items::<Self>(partition_key)
    }

    /// Scan items from the table
    fn scan_items(
        limit: Option<u16>,
        exclusive_start_key: Option<CompositeKey<Self::PK, Self::SK>>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        scan_items::<Self>(limit, exclusive_start_key)
    }

    /// Scan items with filter from the table
    fn scan_items_with_filter<U: Serialize>(
        limit: Option<u16>,
        exclusive_start_key: Option<CompositeKey<Self::PK, Self::SK>>,
        filter_expression: String,
        filter_expression_values: U,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        scan_items_with_filter::<Self, U>(
            limit,
            exclusive_start_key,
            filter_expression,
            filter_expression_values,
        )
    }

    /// Increment multiple fields by values
    fn increment_multiple(
        partition_key: &Self::PK,
        sort_key: Option<&Self::SK>,
        fields: &[(&str, u64)],
    ) -> impl Future<Output = Result<UpdateItemOutput, Error>> {
        increment_multiple::<Self>(partition_key, sort_key, fields)
    }

    /// Update an item with an optional condition expression (same params as `update_item` plus condition)
    fn update_item_with_condition<U: Serialize + Send, C: Serialize>(
        &self,
        update: U,
        condition_expression: Option<String>,
        condition_expression_values: Option<C>,
    ) -> impl Future<Output = Result<UpdateItemOutput, Error>> {
        let partition_key = self.partition_key();
        let sort_key = self.sort_key();
        update_item_with_condition::<Self, U, C>(
            partition_key,
            sort_key,
            update,
            condition_expression,
            condition_expression_values,
        )
    }

    /// Batch write (insert or update) multiple items
    ///
    /// Efficiently writes up to 25 items per batch request. Automatically handles
    /// batching, retries, and unprocessed items.
    ///
    /// # Arguments
    ///
    /// * `upsert` - Vector of items to write
    ///
    /// # Returns
    ///
    /// Returns `BatchWriteOutput<Self>` containing:
    /// - `processed_puts`: Successfully written items
    /// - `failed_puts`: Items that failed after all retries
    /// - `total_duration`: Total execution duration
    /// - `retry_count`: Number of retry attempts made
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     let users = vec![
    ///         User { user_id: "1".to_string(), email: "user1@example.com".to_string() },
    ///         User { user_id: "2".to_string(), email: "user2@example.com".to_string() },
    ///         User { user_id: "3".to_string(), email: "user3@example.com".to_string() },
    ///     ];
    ///
    ///     let result = User::batch_upsert(users).await?;
    ///
    ///     println!("Wrote {} items in {:?}",
    ///         result.processed_puts.len(),
    ///         result.total_duration
    ///     );
    ///
    ///     if !result.failed_puts.is_empty() {
    ///         println!("Failed to write {} items", result.failed_puts.len());
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn batch_upsert(
        upsert: Vec<Self>,
    ) -> impl Future<Output = Result<BatchWriteOutput<Self>, Error>>
    where
        Self: Clone,
    {
        batch_write(upsert, vec![])
    }

    /// Batch delete multiple items
    ///
    /// Efficiently deletes up to 25 items per batch request. Automatically handles
    /// batching, retries, and unprocessed items.
    ///
    /// # Arguments
    ///
    /// * `delete` - Vector of items to delete
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     let users_to_delete = vec![
    ///         User { user_id: "1".to_string(), email: "user1@example.com".to_string() },
    ///         User { user_id: "2".to_string(), email: "user2@example.com".to_string() },
    ///     ];
    ///
    ///     let result = User::batch_delete(users_to_delete).await?;
    ///
    ///     println!("Deleted {} items", result.processed_deletes.len());
    ///     Ok(())
    /// }
    /// ```
    fn batch_delete(
        delete: Vec<Self>,
    ) -> impl Future<Output = Result<BatchWriteOutput<Self>, Error>>
    where
        Self: Clone,
    {
        batch_write(vec![], delete)
    }

    /// Batch get multiple items by their keys
    ///
    /// Efficiently retrieves up to 100 items per batch request. Automatically handles
    /// batching and retries for unprocessed keys.
    ///
    /// # Arguments
    ///
    /// * `values` - Vector of (partition_key, sort_key) tuples
    ///
    /// # Returns
    ///
    /// Returns `BatchReadOutput<Self>` containing:
    /// - `items`: Successfully retrieved items
    /// - `failed_keys`: Keys that failed after all retries
    /// - `execution_time`: Total execution duration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use dynamo_table::{DynamoTable, Error};
    /// # use serde::{Deserialize, Serialize};
    /// #
    /// # #[derive(Debug, Clone, Serialize, Deserialize)]
    /// # struct User {
    /// #     user_id: String,
    /// #     email: String,
    /// # }
    /// #
    /// # impl DynamoTable for User {
    /// #     type PK = String;
    /// #     type SK = String;
    /// #     const TABLE: &'static str = "users";
    /// #     const PARTITION_KEY: &'static str = "user_id";
    /// #     fn partition_key(&self) -> Self::PK { self.user_id.clone() }
    /// # }
    ///
    /// async fn example() -> Result<(), Error> {
    ///     // For tables without sort key
    ///     let keys = vec![
    ///         ("user1".to_string(), None),
    ///         ("user2".to_string(), None),
    ///         ("user3".to_string(), None),
    ///     ];
    ///
    ///     let result = User::batch_get(keys).await?;
    ///
    ///     println!("Retrieved {} users", result.items.len());
    ///
    ///     for user in result.items {
    ///         println!("User: {:?}", user);
    ///     }
    ///
    ///     if !result.failed_keys.is_empty() {
    ///         println!("Failed to retrieve {} keys", result.failed_keys.len());
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    fn batch_get(
        values: Vec<CompositeKey<Self::PK, Self::SK>>,
    ) -> impl Future<Output = Result<BatchReadOutput<Self>, Error>>
    where
        Self::PK: DeserializeOwned,
        Self::SK: DeserializeOwned,
    {
        batch_get::<Self>(values)
    }

    /// Query items in between sort key values
    fn query_items_between(
        partition_key: &Self::PK,
        exclusive_start_key: Option<&Self::SK>,
        limit: Option<u16>,
        scan_index_forward: bool,
        start_sort_key: String,
        end_sort_key: String,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_items_between::<Self>(
            partition_key,
            exclusive_start_key,
            limit,
            None,
            scan_index_forward,
            start_sort_key,
            end_sort_key,
        )
    }

    /// Query items beginning with sort key prefix
    fn query_items_begins_with(
        partition_key: &Self::PK,
        exclusive_start_key: Option<&Self::SK>,
        limit: Option<u16>,
        scan_index_forward: bool,
        sort_key_prefix: String,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_items_begins_with::<Self>(
            partition_key,
            exclusive_start_key,
            limit,
            None,
            scan_index_forward,
            sort_key_prefix,
        )
    }
}

/// Add an item to a DynamoDB table
///
/// This performs a PutItem operation, which creates a new item or replaces an existing item
/// with the same primary key.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait representing the table schema
///
/// # Arguments
///
/// * `payload` - The item to add to the table
///
/// # Returns
///
/// Returns `Ok(PutItemOutput)` on success, or an `Error` if the operation fails.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::DynamoTable;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User {
///     user_id: String,
///     name: String,
///     email: String,
/// }
///
/// impl DynamoTable for User {
///     type PK = String;
///     type SK = String;
///     const TABLE: &'static str = "users";
///     const PARTITION_KEY: &'static str = "user_id";
///
///     fn partition_key(&self) -> String {
///         self.user_id.clone()
///     }
/// }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// let user = User {
///     user_id: "user123".to_string(),
///     name: "Alice".to_string(),
///     email: "alice@example.com".to_string(),
/// };
///
/// // Add the user to the table
/// user.add_item().await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_item<T>(payload: &T) -> Result<PutItemOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    validation::validate_table_keys::<T>();
    let item: HashMap<String, AttributeValue> = to_item(payload)?;

    let result = T::dynamodb_client()
        .await
        .put_item()
        .table_name(T::TABLE)
        .return_values(ReturnValue::None)
        .return_consumed_capacity(ReturnConsumedCapacity::None)
        .set_item(Some(item));

    Ok(result.send().await?)
}

/// Get a single item from a DynamoDB table by its primary key
///
/// This performs a GetItem operation, retrieving a single item using its partition key
/// and optional sort key.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait
///
/// # Arguments
///
/// * `partition_key` - The partition key value
/// * `sort_key` - Optional sort key value (required if table has a sort key)
///
/// # Returns
///
/// Returns `Ok(Some(T))` if the item exists, `Ok(None)` if not found, or `Err` on failure.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::DynamoTable;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct User { user_id: String, name: String }
/// # impl DynamoTable for User {
/// #     type PK = String;
/// #     type SK = String;
/// #     const TABLE: &'static str = "users";
/// #     const PARTITION_KEY: &'static str = "user_id";
/// #     fn partition_key(&self) -> String { self.user_id.clone() }
/// # }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// // Get user by partition key
/// let user = User::get_item(&"user123".to_string(), None).await?;
///
/// match user {
///     Some(u) => println!("Found user: {}", u.name),
///     None => println!("User not found"),
/// }
/// # Ok(())
/// # }
/// ```
pub async fn get_item<T>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
) -> Result<Option<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    debug_assert!(
        !(T::SORT_KEY.is_some() && sort_key.is_none()),
        "get_item argument SORT_KEY is defined but sort_key argument not given"
    );

    validation::validate_table_keys::<T>();

    let mut builder = T::dynamodb_client()
        .await
        .get_item()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .key(
            T::PARTITION_KEY,
            AttributeValue::S(partition_key.to_string()),
        );

    if let (Some(sort_key_field), Some(sort_value)) = (T::SORT_KEY, sort_key) {
        builder = builder.key(sort_key_field, AttributeValue::S(sort_value.to_string()));
    }

    let result = builder.send().await?;

    if let Some(item) = result.item {
        let item: T = from_item(item)?;

        Ok(Some(item))
    } else {
        Ok(None)
    }
}

/// Query multiple items from a DynamoDB table
///
/// Retrieves all items matching the partition key, with optional pagination.
/// Results are returned in ascending sort key order.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait
///
/// # Arguments
///
/// * `partition_key` - The partition key value to query
/// * `limit` - Maximum number of items to return (defaults to `T::DEFAULT_PAGE_SIZE`)
/// * `exclusive_start_key` - Sort key to start from (for pagination)
///
/// # Returns
///
/// Returns `OutputItems<T>` containing the matched items and pagination info.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::DynamoTable;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct Order { user_id: String, order_id: String, total: f64 }
/// # impl DynamoTable for Order {
/// #     type PK = String;
/// #     type SK = String;
/// #     const TABLE: &'static str = "orders";
/// #     const PARTITION_KEY: &'static str = "user_id";
/// #     const SORT_KEY: Option<&'static str> = Some("order_id");
/// #     fn partition_key(&self) -> String { self.user_id.clone() }
/// #     fn sort_key(&self) -> Option<String> { Some(self.order_id.clone()) }
/// # }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// // Get all orders for a user with pagination
/// let user_id = "user123".to_string();
/// let results = Order::query_items(&user_id, None, Some(10), None).await?;
///
/// for order in &results.items {
///     println!("Order: {} - ${}", order.order_id, order.total);
/// }
///
/// // Check if there are more results
/// if let Some(cursor) = results.start_cursor() {
///     // Fetch next page using the cursor
///     let next_page = Order::query_items(
///         &user_id,
///         None,
///         Some(10),
///         cursor.exclusive_start_key()
///     ).await?;
/// }
/// # Ok(())
/// # }
/// ```
pub async fn query_items<T>(
    partition_key: &T::PK,
    limit: Option<u16>,
    exclusive_start_key: Option<&T::SK>,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    _query_items::<T>(partition_key, None, exclusive_start_key, limit, None, false).await
}

/// Query items builder
async fn _query_items_builder<T>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
    exclusive_start_key: Option<&T::SK>,
    limit: u16,
    index_name: Option<String>,
    scan_index_forward: bool,
) -> QueryFluentBuilder
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    validation::validate_table_keys::<T>();

    let client = T::dynamodb_client().await;

    let builder = if let Some(index_name) = index_name {
        query_builder::QueryBuilder::for_index::<T>(index_name)
    } else {
        query_builder::QueryBuilder::for_table::<T>()
    };

    builder.build_query(
        client,
        partition_key.to_string(),
        sort_key.map(|sk| sk.to_string()),
        exclusive_start_key.map(|sk| sk.to_string()),
        limit,
        scan_index_forward,
    )
}

/// Query items from a table with filter expression
#[allow(clippy::too_many_arguments)]
pub async fn query_items_with_filter<T, U>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
    exclusive_start_key: Option<&T::SK>,
    limit: Option<u16>,
    index_name: Option<String>,
    scan_index_forward: bool,
    filter_expression: String,
    filter_expression_values: U,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    U: Serialize,
{
    if cfg!(debug_assertions) {
        validation::validate_filter_expression_values(&filter_expression_values);
    }

    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    let filter_expression_values =
        to_item::<_, HashMap<String, AttributeValue>>(filter_expression_values)?;

    let mut builder = _query_items_builder::<T>(
        partition_key,
        sort_key,
        exclusive_start_key,
        limit,
        index_name,
        scan_index_forward,
    )
    .await
    .filter_expression(filter_expression);

    for (key, value) in filter_expression_values {
        builder = builder.expression_attribute_values(key, value);
    }

    let result = builder.send().await?;

    Ok(OutputItems::from((result, limit)))
}

/// Query items using a BETWEEN condition on the sort key.
#[allow(clippy::too_many_arguments)]
pub async fn query_items_between<T>(
    partition_key: &T::PK,
    exclusive_start_key: Option<&T::SK>,
    limit: Option<u16>,
    index_name: Option<String>,
    scan_index_forward: bool,
    range_start: String,
    range_end: String,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);
    let sort_key_field = T::SORT_KEY.expect("sort key required for between query");

    let mut builder = T::dynamodb_client()
        .await
        .query()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .scan_index_forward(scan_index_forward)
        .limit(limit as i32)
        .key_condition_expression(format!(
            "{} = :hash_value AND {sort_key_field} BETWEEN :range_start AND :range_end",
            T::PARTITION_KEY
        ))
        .expression_attribute_values(":hash_value", AttributeValue::S(partition_key.to_string()))
        .expression_attribute_values(":range_start", AttributeValue::S(range_start))
        .expression_attribute_values(":range_end", AttributeValue::S(range_end));

    if let Some(index_name) = index_name {
        builder = builder.index_name(index_name);
    }

    if let Some(sort_key) = exclusive_start_key {
        builder = builder
            .exclusive_start_key(
                T::PARTITION_KEY,
                AttributeValue::S(partition_key.to_string()),
            )
            .exclusive_start_key(sort_key_field, AttributeValue::S(sort_key.to_string()));
    }

    let result = builder.send().await?;

    Ok(OutputItems::from((result, limit)))
}

/// Query items using a `begins_with` condition on the sort key.
#[allow(clippy::too_many_arguments)]
pub async fn query_items_begins_with<T>(
    partition_key: &T::PK,
    exclusive_start_key: Option<&T::SK>,
    limit: Option<u16>,
    index_name: Option<String>,
    scan_index_forward: bool,
    prefix: String,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);
    let sort_key_field = T::SORT_KEY.expect("sort key required for begins_with query");

    let mut builder = T::dynamodb_client()
        .await
        .query()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .scan_index_forward(scan_index_forward)
        .limit(limit as i32)
        .key_condition_expression(format!(
            "{} = :hash_value AND begins_with({sort_key_field}, :sort_prefix)",
            T::PARTITION_KEY,
        ))
        .expression_attribute_values(":hash_value", AttributeValue::S(partition_key.to_string()))
        .expression_attribute_values(":sort_prefix", AttributeValue::S(prefix));

    if let Some(index_name) = index_name {
        builder = builder.index_name(index_name);
    }

    if let Some(sort_key) = exclusive_start_key {
        builder = builder
            .exclusive_start_key(
                T::PARTITION_KEY,
                AttributeValue::S(partition_key.to_string()),
            )
            .exclusive_start_key(sort_key_field, AttributeValue::S(sort_key.to_string()));
    }

    let result = builder.send().await?;

    Ok(OutputItems::from((result, limit)))
}

/// Query item from a table
async fn _query_items<T>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
    exclusive_start_key: Option<&T::SK>,
    limit: Option<u16>,
    index_name: Option<String>,
    scan_index_forward: bool,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    let result = _query_items_builder::<T>(
        partition_key,
        sort_key,
        exclusive_start_key,
        limit,
        index_name,
        scan_index_forward,
    )
    .await
    .send()
    .await?;

    Ok(OutputItems::from((result, limit)))
}

/// Query item from a table
pub async fn query_item<T>(partition_key: &T::PK) -> Result<Option<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    _query_items::<T>(partition_key, None, None, Some(1), None, false)
        .await
        .map(|mut output| output.items.pop())
}

/// Delete an item from a DynamoDB table
///
/// Removes an item from the table using its primary key.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait
///
/// # Arguments
///
/// * `partition_key` - The partition key value
/// * `sort_key` - Optional sort key value (required if table has a sort key)
///
/// # Returns
///
/// Returns `Ok(DeleteItemOutput)` on success, or an `Error` if the operation fails.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::DynamoTable;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct User { user_id: String, name: String }
/// # impl DynamoTable for User {
/// #     type PK = String;
/// #     type SK = String;
/// #     const TABLE: &'static str = "users";
/// #     const PARTITION_KEY: &'static str = "user_id";
/// #     fn partition_key(&self) -> String { self.user_id.clone() }
/// # }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// // Delete a user by ID
/// User::delete_item("user123".to_string(), None).await?;
/// println!("User deleted successfully");
/// # Ok(())
/// # }
/// ```
pub async fn delete_item<T>(
    partition_key: T::PK,
    sort_key: Option<T::SK>,
) -> Result<DeleteItemOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    debug_assert!(
        !(T::SORT_KEY.is_some() && sort_key.is_none()),
        "delete_item argument SORT_KEY is defined but sort_key argument not given"
    );

    validation::validate_table_keys::<T>();

    let mut builder = T::dynamodb_client()
        .await
        .delete_item()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .key(
            T::PARTITION_KEY,
            AttributeValue::S(partition_key.to_string()),
        );

    if let (Some(sort_key), Some(sort_value)) = (T::SORT_KEY, sort_key) {
        builder = builder.key(sort_key, AttributeValue::S(sort_value.to_string()));
    }

    Ok(builder.send().await?)
}

/// Increment field by value
pub async fn increment<T>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
    field: &str,
    increment_by: u64,
) -> Result<UpdateItemOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    increment_multiple::<T>(partition_key, sort_key, &[(field, increment_by)]).await
}

/// Increment field by value
pub async fn increment_multiple<T>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
    fields: &[(&str, u64)],
) -> Result<UpdateItemOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    if fields.is_empty() {
        return Ok(UpdateItemOutput::builder().build());
    }

    debug_assert!(
        !(T::SORT_KEY.is_some() && sort_key.is_none()),
        "increment argument SORT_KEY is defined but sort_key argument not given"
    );

    validation::validate_table_keys::<T>();
    if cfg!(debug_assertions) {
        let field_names: Vec<&str> = fields.iter().map(|f| f.0).collect();
        validation::validate_field_names(&field_names);
    }

    let mut builder = T::dynamodb_client()
        .await
        .update_item()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .set_return_values(Some(ReturnValue::None))
        .key(
            T::PARTITION_KEY,
            AttributeValue::S(partition_key.to_string()),
        );

    let mut update_expressions: Vec<String> = Vec::with_capacity(fields.len());

    for (index, field) in fields.iter().enumerate() {
        update_expressions.push(format!("{} = {} + :incr{}", field.0, field.0, index));
        builder = builder.expression_attribute_values(
            format!(":incr{index}"),
            AttributeValue::N(format!("{}", field.1)),
        );
    }

    builder = builder.update_expression(format!("SET {}", update_expressions.join(",")));

    if let (Some(sort_key), Some(sort_value)) = (T::SORT_KEY, sort_key) {
        builder = builder.key(sort_key, AttributeValue::S(sort_value.to_string()));
    }

    Ok(builder.send().await?)
}

/// Update specific fields of an item in a DynamoDB table
///
/// Updates one or more attributes of an existing item. If the item doesn't exist,
/// it will be created with the specified attributes.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait
/// * `U` - A serializable type containing the fields to update
///
/// # Arguments
///
/// * `partition_key` - The partition key value
/// * `sort_key` - Optional sort key value (required if table has a sort key)
/// * `update` - A struct or map containing the fields and values to update
///
/// # Returns
///
/// Returns `Ok(UpdateItemOutput)` on success, or an `Error` if the operation fails.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::DynamoTable;
/// use std::collections::HashMap;
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct User { user_id: String, name: String, email: String }
/// # impl DynamoTable for User {
/// #     type PK = String;
/// #     type SK = String;
/// #     const TABLE: &'static str = "users";
/// #     const PARTITION_KEY: &'static str = "user_id";
/// #     fn partition_key(&self) -> String { self.user_id.clone() }
/// # }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// // Update user's email
/// let mut updates = HashMap::new();
/// updates.insert("email", "newemail@example.com");
/// updates.insert("updated_at", "2024-01-01T00:00:00Z");
///
/// User::delete_item("user123".to_string(), None)
///     .await?;
///
/// // Or use a struct for type safety
/// #[derive(serde::Serialize)]
/// struct UserUpdate {
///     email: String,
///     updated_at: String,
/// }
///
/// let update = UserUpdate {
///     email: "newemail@example.com".to_string(),
///     updated_at: "2024-01-01T00:00:00Z".to_string(),
/// };
///
/// User::delete_item("user123".to_string(), None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn update_item<T, U>(
    partition_key: T::PK,
    sort_key: Option<T::SK>,
    update: U,
) -> Result<UpdateItemOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    U: Serialize + Send,
{
    debug_assert!(
        !(T::SORT_KEY.is_some() && sort_key.is_none()),
        "update_item argument SORT_KEY is defined but sort_key argument not given"
    );

    let item = to_item::<_, HashMap<String, AttributeValue>>(update)?;

    if cfg!(debug_assertions) {
        assert_not_reserved_key(T::PARTITION_KEY);
        assert_not_reserved_key(T::SORT_KEY.unwrap_or_default());

        assert!(!item.is_empty());
        let field_names: Vec<&str> = item.keys().map(|k| k.as_str()).collect();
        validation::validate_field_names(&field_names);
    }

    let mut update_expressions: Vec<String> = Vec::with_capacity(item.len());

    let mut builder = T::dynamodb_client()
        .await
        .update_item()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .set_return_values(Some(ReturnValue::None))
        .key(
            T::PARTITION_KEY,
            AttributeValue::S(partition_key.to_string()),
        );

    for (index, (k, v)) in item.into_iter().enumerate() {
        let val = format!(":val{index}");
        update_expressions.push(format!("{k} = {val}"));
        builder = builder.expression_attribute_values(val, v);
    }

    builder = builder.update_expression(format!("SET {}", update_expressions.join(",")));

    if let (Some(sort_key), Some(sort_value)) = (T::SORT_KEY, sort_key) {
        builder = builder.key(sort_key, AttributeValue::S(sort_value.to_string()));
    }

    Ok(builder.send().await?)
}

/// Update an item with a custom update expression and values
/// Update an item with a condition expression and values
pub async fn update_item_with_condition<T, U, C>(
    partition_key: T::PK,
    sort_key: Option<T::SK>,
    update: U,
    condition_expression: Option<String>,
    condition_expression_values: Option<C>,
) -> Result<UpdateItemOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    U: Serialize + Send,
    C: Serialize,
{
    debug_assert!(
        !(T::SORT_KEY.is_some() && sort_key.is_none()),
        "update_item_with_condition argument SORT_KEY is defined but sort_key argument not given"
    );

    let item = to_item::<_, HashMap<String, AttributeValue>>(update)?;

    if cfg!(debug_assertions) {
        assert_not_reserved_key(T::PARTITION_KEY);
        assert_not_reserved_key(T::SORT_KEY.unwrap_or_default());

        assert!(!item.is_empty());
        let field_names: Vec<&str> = item.keys().map(|k| k.as_str()).collect();
        validation::validate_field_names(&field_names);
    }

    let mut update_expressions: Vec<String> = Vec::with_capacity(item.len());

    let mut builder = T::dynamodb_client()
        .await
        .update_item()
        .table_name(T::TABLE)
        .set_return_consumed_capacity(None)
        .set_return_values(Some(ReturnValue::None))
        .key(
            T::PARTITION_KEY,
            AttributeValue::S(partition_key.to_string()),
        );

    for (index, (k, v)) in item.into_iter().enumerate() {
        let val = format!(":val{index}");
        update_expressions.push(format!("{k} = {val}"));
        builder = builder.expression_attribute_values(val, v);
    }

    builder = builder.update_expression(format!("SET {}", update_expressions.join(",")));

    if let (Some(sort_key_field), Some(sort_value)) = (T::SORT_KEY, sort_key) {
        builder = builder.key(sort_key_field, AttributeValue::S(sort_value.to_string()));
    }

    if let Some(cond) = condition_expression {
        builder = builder.condition_expression(cond);
        if let Some(values) = condition_expression_values {
            let values = to_item::<_, HashMap<String, AttributeValue>>(values)?;
            for (k, v) in values {
                builder = builder.expression_attribute_values(k, v);
            }
        }
    }

    Ok(builder.send().await?)
}

/// Count items by partition key
pub async fn count_items<T>(partition_key: &T::PK) -> Result<usize, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    validation::validate_table_keys::<T>();

    let client = T::dynamodb_client().await;
    let builder = query_builder::QueryBuilder::for_table::<T>();
    let result = builder
        .build_count_query(client, partition_key.to_string())
        .send()
        .await?;

    Ok(result.count as usize)
}

async fn _scan_items_builder<T>(
    exclusive_start_key: Option<CompositeKey<T::PK, T::SK>>,
) -> operation::scan::builders::ScanFluentBuilder
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    validation::validate_table_keys::<T>();

    let mut builder = T::dynamodb_client()
        .await
        .scan()
        .table_name(T::TABLE)
        // Scans currently operate on the base table, so requesting all attributes is always valid.
        .select(Select::AllAttributes)
        .set_return_consumed_capacity(None);

    // Set exclusive start key for pagination if provided
    if let Some((partition_key, sort_key)) = exclusive_start_key {
        builder = builder.exclusive_start_key(
            T::PARTITION_KEY,
            AttributeValue::S(partition_key.to_string()),
        );

        if let (Some(sort_key_field), Some(sort_value)) = (T::SORT_KEY, sort_key) {
            builder = builder
                .exclusive_start_key(sort_key_field, AttributeValue::S(sort_value.to_string()));
        }
    }
    builder
}

/// Scan items from a table
pub async fn scan_items<T>(
    limit: Option<u16>,
    exclusive_start_key: Option<CompositeKey<T::PK, T::SK>>,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    let result = _scan_items_builder::<T>(exclusive_start_key)
        .await
        .limit(limit as i32)
        .send()
        .await?;

    Ok(OutputItems::from((result, limit)))
}

/// Scan items from a table
pub async fn scan_items_with_filter<T, U>(
    limit: Option<u16>,
    exclusive_start_key: Option<CompositeKey<T::PK, T::SK>>,
    filter_expression: String,
    filter_expression_values: U,
) -> Result<OutputItems<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    U: Serialize,
{
    if cfg!(debug_assertions) {
        validation::validate_filter_expression_values(&filter_expression_values);
    }

    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    let filter_expression_values =
        to_item::<_, HashMap<String, AttributeValue>>(filter_expression_values)?;

    let mut builder = _scan_items_builder::<T>(exclusive_start_key)
        .await
        .filter_expression(filter_expression)
        .limit(limit as i32);

    // Add filter expression values
    for (key, value) in filter_expression_values {
        builder = builder.expression_attribute_values(key, value);
    }

    let result = builder.send().await?;

    Ok(OutputItems::from((result, limit)))
}

/// Stream item from a table
pub async fn query_items_stream<T>(
    partition_key: &T::PK,
    sort_key: Option<&T::SK>,
    exclusive_start_key: Option<&T::SK>,
    limit: Option<u16>,
    index_name: Option<String>,
    scan_index_forward: bool,
) -> impl Stream<Item = Result<T, Error>>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    _query_items_builder::<T>(
        partition_key,
        sort_key,
        exclusive_start_key,
        limit,
        index_name,
        scan_index_forward,
    )
    .await
    .into_paginator()
    .page_size(limit as i32)
    .items()
    .send()
    .into_stream_03x()
    .map_err(Into::into)
    .and_then(|item| async { from_item(item).map_err(Into::into) })
}

/// Batch write output containing comprehensive metrics and tracking
///
/// Tracks which items were successfully processed, which failed after retries,
/// execution time, and retry attempts.
#[must_use = "batch write results contain failed items and metrics that should be checked"]
#[derive(Debug)]
pub struct BatchWriteOutput<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// Items successfully written (puts)
    pub processed_puts: Vec<T>,

    /// Items that failed to be written after all retry attempts
    pub failed_puts: Vec<T>,

    /// Items successfully deleted
    pub processed_deletes: Vec<T>,

    /// Items that failed to be deleted after all retry attempts
    pub failed_deletes: Vec<T>,

    /// Total execution time including all retries
    pub total_duration: Duration,

    /// Number of retry attempts made (0 means no retries needed)
    pub retry_count: usize,

    /// A list of tables that were processed by <code>BatchWriteItem</code> and, for each table, information about any item collections that were affected by individual <code>DeleteItem</code> or <code>PutItem</code> operations.
    pub item_collection_metrics: HashMap<String, Vec<ItemCollectionMetrics>>,

    /// The capacity units consumed by the entire <code>BatchWriteItem</code> operation.
    pub consumed_capacity: Vec<ConsumedCapacity>,
}

impl<T> BatchWriteOutput<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    fn new() -> Self {
        Self {
            processed_puts: Vec::new(),
            failed_puts: Vec::new(),
            processed_deletes: Vec::new(),
            failed_deletes: Vec::new(),
            total_duration: Duration::ZERO,
            retry_count: 0,
            item_collection_metrics: HashMap::new(),
            consumed_capacity: Vec::new(),
        }
    }

    /// Check if all operations were successful
    pub fn is_success(&self) -> bool {
        self.failed_puts.is_empty() && self.failed_deletes.is_empty()
    }

    /// Get total number of items processed successfully
    pub fn processed_count(&self) -> usize {
        self.processed_puts.len() + self.processed_deletes.len()
    }

    /// Get total number of items that failed
    pub fn failed_count(&self) -> usize {
        self.failed_puts.len() + self.failed_deletes.len()
    }

    /// Get success rate as a percentage (0.0 to 100.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.processed_count() + self.failed_count();
        if total == 0 {
            return 100.0;
        }
        (self.processed_count() as f64 / total as f64) * 100.0
    }
}

/// Batch write (put/delete) multiple items to a DynamoDB table
///
/// Performs multiple put and/or delete operations in a single batch request.
/// DynamoDB processes up to 25 items per batch, automatically chunking larger requests.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait
///
/// # Arguments
///
/// * `update` - Vector of items to put (insert or replace)
/// * `delete` - Vector of items to delete
///
/// # Returns
///
/// Returns `BatchWriteOutput<T>` containing information about processed items and any
/// unprocessed items that need to be retried.
///
/// # Automatic Retry Behavior
///
/// **Built-in retry with exponential backoff**: This function automatically retries
/// unprocessed items up to 5 times with exponential backoff (100ms, 200ms, 400ms, 800ms, 1600ms).
///
/// DynamoDB may not process all items in one attempt due to:
/// - **Throttling** - Provisioned throughput limits exceeded
/// - **Item size** - Items over 400KB are rejected
/// - **Capacity limits** - Table or account-level limits
/// - **Temporary issues** - Network problems or service hiccups
///
/// **After automatic retries**, any remaining failed items will be returned in:
/// - `failed_puts` - Items that failed to be written after all retries
/// - `failed_deletes` - Items that failed to be deleted after all retries
///
/// If these vectors are non-empty after the function returns, consider logging them
/// for manual investigation or sending to a dead-letter queue.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::{DynamoTable, table::batch_write};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize, Clone)]
/// # struct Product { product_id: String, name: String, price: f64 }
/// # impl DynamoTable for Product {
/// #     type PK = String;
/// #     type SK = String;
/// #     const TABLE: &'static str = "products";
/// #     const PARTITION_KEY: &'static str = "product_id";
/// #     fn partition_key(&self) -> String { self.product_id.clone() }
/// # }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// // Batch create/update products
/// let products = vec![
///     Product { product_id: "p1".into(), name: "Widget".into(), price: 9.99 },
///     Product { product_id: "p2".into(), name: "Gadget".into(), price: 19.99 },
///     Product { product_id: "p3".into(), name: "Gizmo".into(), price: 29.99 },
/// ];
///
/// // Automatic retry is built-in! Function retries up to 5 times automatically
/// let result = batch_write(products, vec![]).await?;
///
/// // Check if any items failed after all automatic retries
/// if !result.failed_puts.is_empty() {
///     eprintln!("ERROR: {} items failed after 5 retry attempts",
///         result.failed_puts.len());
///
///     // Handle persistent failures:
///     // 1. Log to monitoring/alerting system
///     // 2. Send to dead-letter queue for manual investigation
///     // 3. Store in failure table for later processing
///     for failed_item in &result.failed_puts {
///         eprintln!("Failed item: {:?}", failed_item.product_id);
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn batch_write<T>(update: Vec<T>, delete: Vec<T>) -> Result<BatchWriteOutput<T>, Error>
where
    T: DynamoTable + Clone,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    let start_time = std::time::Instant::now();
    let retries = T::BATCH_RETRIES_CONFIG;

    // Track original counts
    let original_puts_count = update.len();
    let original_deletes_count = delete.len();

    // Clone for tracking
    let original_puts = update.clone();
    let original_deletes = delete.clone();

    // Initial call
    let mut result = batch_write_internal(update, delete).await?;

    // Retry loop for failed items
    let mut retry_count = 0;

    while (!result.failed_puts.is_empty() || !result.failed_deletes.is_empty())
        && retry_count < retries.max_retries
    {
        // Wait with exponential backoff
        tokio::time::sleep(retry_config::retry_delay(
            retry_count,
            retries.initial_delay,
            retries.max_delay,
        ))
        .await;

        retry_count += 1;

        // Take failed items for retry
        let unprocessed_puts = std::mem::take(&mut result.failed_puts);
        let unprocessed_deletes = std::mem::take(&mut result.failed_deletes);

        // Retry the failed items
        let retry_result = batch_write_internal(unprocessed_puts, unprocessed_deletes).await?;

        // Merge results
        result.failed_puts.extend(retry_result.failed_puts);
        result.failed_deletes.extend(retry_result.failed_deletes);
        result
            .consumed_capacity
            .extend(retry_result.consumed_capacity);

        for (table_name, values) in retry_result.item_collection_metrics {
            result
                .item_collection_metrics
                .entry(table_name)
                .or_default()
                .extend(values);
        }
    }

    // Calculate metrics
    let failed_puts_count = result.failed_puts.len();
    let processed_puts_count = original_puts_count - failed_puts_count;

    // For puts, separate processed from failed
    result.processed_puts = original_puts
        .into_iter()
        .take(processed_puts_count)
        .collect();

    // For deletes, we track using the original list
    let failed_deletes_count = result.failed_deletes.len();
    let processed_deletes_count = original_deletes_count - failed_deletes_count;

    result.processed_deletes = original_deletes
        .into_iter()
        .take(processed_deletes_count)
        .collect();

    result.total_duration = start_time.elapsed();
    result.retry_count = retry_count;

    Ok(result)
}

/// Internal batch write implementation without retry logic
pub(super) async fn batch_write_internal<T>(
    update: Vec<T>,
    delete: Vec<T>,
) -> Result<BatchWriteOutput<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    if update.is_empty() && delete.is_empty() {
        return Ok(BatchWriteOutput::new());
    }

    let mut write_ops: Vec<WriteRequest> = Vec::with_capacity(update.len() + delete.len());

    for value in update {
        let item = to_item::<_, HashMap<String, AttributeValue>>(value)?;

        let put_request = PutRequest::builder().set_item(Some(item)).build()?;

        write_ops.push(
            WriteRequest::builder()
                .set_put_request(Some(put_request))
                .build(),
        );
    }

    for value in delete {
        let mut delete_request = DeleteRequest::builder().key(
            value.partition_key_name(),
            AttributeValue::S(value.partition_key().to_string()),
        );

        if let (Some(name), Some(value)) = (value.sort_key_name(), value.sort_key()) {
            delete_request = delete_request.key(name, AttributeValue::S(value.to_string()));
        }

        write_ops.push(
            WriteRequest::builder()
                .set_delete_request(Some(delete_request.build()?))
                .build(),
        );
    }

    let output: BatchWriteOutput<T> = BatchWriteOutput::new();

    let batches: Vec<Vec<WriteRequest>> = write_ops
        .chunks(batch_processor::BATCH_WRITE_SIZE)
        .map(|data| data.to_vec())
        .collect();

    let client = T::dynamodb_client().await;

    let concurrency = batches.len().min(batch_processor::DEFAULT_CONCURRENCY); // Limit concurrency to 10 or number of batches

    stream::iter(batches.into_iter().map(|batch| {
        client
            .batch_write_item()
            .request_items(T::TABLE, batch)
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .return_item_collection_metrics(ReturnItemCollectionMetrics::Size)
            .send()
    }))
    .buffer_unordered(concurrency)
    .map_err(Into::<Error>::into)
    .try_fold(output, |mut builder, result| async {
        if let Some(unprocessed_items) = result.unprocessed_items {
            for items in unprocessed_items.into_values() {
                for item in items {
                    if let Some(put_request) = item.put_request {
                        let item = from_item(put_request.item)?;
                        builder.failed_puts.push(item);
                    }

                    if let Some(delete_request) = item.delete_request {
                        let item = from_item(delete_request.key)?;
                        builder.failed_deletes.push(item);
                    }
                }
            }
        }

        if let Some(item_collection_metrics) = result.item_collection_metrics {
            for (table_name, values) in item_collection_metrics {
                builder
                    .item_collection_metrics
                    .entry(table_name)
                    .or_default()
                    .extend(values);
            }
        }

        if let Some(capacities) = result.consumed_capacity {
            builder.consumed_capacity.extend(capacities);
        }

        Ok(builder)
    })
    .await
}

/// Global Secondary Index (GSI) table trait for querying on alternate keys
///
/// A Global Secondary Index allows you to query your DynamoDB table using a different
/// partition key (and optional sort key) than the primary table key. This is useful when
/// you need to access your data in multiple ways without duplicating data.
///
/// # When to Use GSI
///
/// Use a GSI when you need to:
/// - Query data by an attribute other than the primary partition key
/// - Support multiple access patterns on the same table
/// - Avoid expensive scan operations for common queries
///
/// # GSI vs Primary Table
///
/// - **Primary table**: Optimized for queries by the main partition/sort key
/// - **GSI**: Creates an alternate view of your data with different keys
/// - GSI queries have the same performance as primary table queries
/// - GSI data is eventually consistent (not strongly consistent like GetItem)
///
/// # Example: User Table with Email Lookup
///
/// ```rust,ignore
/// use dynamo_table::{DynamoTable, GSITable};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Clone, Debug)]
/// struct User {
///     user_id: String,      // Primary partition key
///     email: String,        // GSI partition key - for email-based lookups
///     name: String,
///     created_at: String,   // GSI sort key - for time-based queries
/// }
///
/// impl DynamoTable for User {
///     type PK = String;
///     type SK = String;
///
///     const TABLE: &'static str = "users";
///     const PARTITION_KEY: &'static str = "user_id";
///     const SORT_KEY: Option<&'static str> = None;
///
///     fn partition_key(&self) -> Self::PK {
///         self.user_id.clone()
///     }
/// }
///
/// impl GSITable for User {
///     // Define GSI keys - can query by email instead of user_id
///     const GSI_PARTITION_KEY: &'static str = "email";
///     const GSI_SORT_KEY: Option<&'static str> = Some("created_at");
///
///     fn gsi_partition_key(&self) -> String {
///         self.email.clone()
///     }
///
///     fn gsi_sort_key(&self) -> Option<String> {
///         Some(self.created_at.clone())
///     }
/// }
///
/// // Usage examples:
/// async fn example() -> Result<(), dynamo_table::Error> {
///     // Query by email (GSI) instead of user_id (primary key)
///     let user = User::query_gsi_item(
///         "alice@example.com".to_string(),
///         None
///     ).await?;
///
///     // Get users by email, sorted by creation time
///     let users = User::query_gsi_items(
///         "alice@example.com".to_string(),
///         None,
///         Some(10),
///         None
///     ).await?;
///
///     // Count users with specific email
///     let count = User::count_gsi_items("alice@example.com".to_string()).await?;
///
///     Ok(())
/// }
/// ```
///
/// # Available GSI Methods
///
/// - [`query_gsi_item`](GSITable::query_gsi_item) - Query a single item by GSI keys
/// - [`query_gsi_items`](GSITable::query_gsi_items) - Query multiple items
/// - [`reverse_query_gsi_items`](GSITable::reverse_query_gsi_items) - Query in reverse order
/// - [`query_gsi_items_with_filter`](GSITable::query_gsi_items_with_filter) - Query with filter expression
/// - [`count_gsi_items`](GSITable::count_gsi_items) - Count items without retrieving them
///
/// # GSI Index Naming
///
/// The index name is auto-generated based on your configuration:
/// - With sort key: `global-{TABLE}-{GSI_PARTITION_KEY}-{GSI_SORT_KEY}`
/// - Without sort key: `global-{TABLE}-{GSI_PARTITION_KEY}`
///
/// For the example above: `global-users-email-created_at`
///
/// # Performance Considerations
///
/// - GSI queries are as fast as primary table queries
/// - GSI data is eventually consistent (slight delay after writes)
/// - Each GSI consumes additional write capacity (automatically replicated)
/// - GSI can have different projection (ALL, KEYS_ONLY, or INCLUDE specific attributes)
///
/// # Common Access Patterns
///
/// 1. **Lookup by alternate identifier**: Find user by email, product by SKU
/// 2. **Time-based queries**: Get orders by user and date range
/// 3. **Category queries**: Find items by category and rating
/// 4. **Status filtering**: Query active items, pending orders
///
pub trait GSITable: DynamoTable {
    /// GSI partition key field name
    ///
    /// This is the attribute name in your table that will be used as the partition key
    /// for the Global Secondary Index. It must match an attribute in your struct.
    ///
    /// # Example
    /// ```ignore
    /// const GSI_PARTITION_KEY: &'static str = "email";
    /// ```
    const GSI_PARTITION_KEY: &'static str;

    /// GSI sort key field name (optional)
    ///
    /// This is the attribute name in your table that will be used as the sort key
    /// for the Global Secondary Index. It must match an attribute in your struct.
    /// Leave as `None` if you only need a partition key for your GSI.
    ///
    /// # Example
    /// ```ignore
    /// const GSI_SORT_KEY: Option<&'static str> = Some("created_at");
    /// ```
    const GSI_SORT_KEY: Option<&'static str> = None;

    /// Returns the GSI partition key value for this item
    ///
    /// This method extracts the partition key value from the item for GSI operations.
    ///
    /// # Example
    /// ```ignore
    /// fn gsi_partition_key(&self) -> String {
    ///     self.email.clone()
    /// }
    /// ```
    fn gsi_partition_key(&self) -> String;

    /// Returns the GSI sort key value for this item (optional)
    ///
    /// This method extracts the sort key value from the item for GSI operations.
    /// Return `None` if your GSI doesn't have a sort key.
    ///
    /// # Example
    /// ```ignore
    /// fn gsi_sort_key(&self) -> Option<String> {
    ///     Some(self.created_at.clone())
    /// }
    /// ```
    fn gsi_sort_key(&self) -> Option<String> {
        None
    }

    /// GSI index name
    fn global_index_name() -> String {
        if let Some(sort_key) = Self::GSI_SORT_KEY {
            format!(
                "global-{}-{}-{}",
                Self::TABLE,
                Self::GSI_PARTITION_KEY,
                sort_key
            )
        } else {
            format!("global-{}-{}", Self::TABLE, Self::GSI_PARTITION_KEY)
        }
    }

    /// Query items using the GSI
    fn query_gsi_items(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        limit: Option<u16>,
        exclusive_start_key: Option<String>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_gsi_items::<Self>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_key,
            limit,
            true,
        )
    }

    /// Query items using the GSI in reverse order
    fn reverse_query_gsi_items(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        limit: Option<u16>,
        exclusive_start_key: Option<String>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_gsi_items::<Self>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_key,
            limit,
            false,
        )
    }

    /// Query a single item using the GSI
    fn query_gsi_item(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
    ) -> impl Future<Output = Result<Option<Self>, Error>> {
        query_gsi_item::<Self>(gsi_partition_key, gsi_sort_key)
    }

    /// Query items using the GSI with filter expression
    fn query_gsi_items_with_filter<U: Serialize>(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        exclusive_start_key: Option<String>,
        limit: Option<u16>,
        scan_index_forward: bool,
        filter_expression: String,
        filter_expression_values: U,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_gsi_items_with_filter::<Self, U>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_key,
            limit,
            scan_index_forward,
            filter_expression,
            filter_expression_values,
        )
    }

    /// Count items by GSI partition key
    fn count_gsi_items(gsi_partition_key: String) -> impl Future<Output = Result<usize, Error>> {
        count_gsi_items::<Self>(gsi_partition_key)
    }
}

/// Query single item from a GSI
async fn query_gsi_item<T>(
    gsi_partition_key: String,
    gsi_sort_key: Option<String>,
) -> Result<Option<T>, Error>
where
    T: GSITable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    let mut output =
        query_gsi_items::<T>(gsi_partition_key, gsi_sort_key, None, Some(1), true).await?;
    Ok(output.items.pop())
}

/// Query items from a GSI
async fn query_gsi_items<T>(
    gsi_partition_key: String,
    gsi_sort_key: Option<String>,
    exclusive_start_key: Option<String>,
    limit: Option<u16>,
    scan_index_forward: bool,
) -> Result<OutputItems<T>, Error>
where
    T: GSITable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    validation::validate_gsi_keys::<T>();

    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    let client = T::dynamodb_client().await;
    let builder = query_builder::QueryBuilder::for_gsi::<T>();
    let result = builder
        .build_query(
            client,
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_key,
            limit,
            scan_index_forward,
        )
        .send()
        .await?;

    Ok(OutputItems::from((result, limit)))
}

/// Query GSI items with filter expression
pub async fn query_gsi_items_with_filter<T, U>(
    gsi_partition_key: String,
    gsi_sort_key: Option<String>,
    exclusive_start_key: Option<String>,
    limit: Option<u16>,
    scan_index_forward: bool,
    filter_expression: String,
    filter_expression_values: U,
) -> Result<OutputItems<T>, Error>
where
    T: GSITable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    U: Serialize,
{
    if cfg!(debug_assertions) {
        validation::validate_filter_expression_values(&filter_expression_values);
    }

    if limit.map(|l| l == 0).unwrap_or(false) {
        return Ok(OutputItems::default());
    }

    let limit = limit.unwrap_or(T::DEFAULT_PAGE_SIZE);

    let filter_expression_values =
        to_item::<_, HashMap<String, AttributeValue>>(filter_expression_values)?;

    validation::validate_gsi_keys::<T>();

    let mut builder = T::dynamodb_client()
        .await
        .query()
        .table_name(T::TABLE)
        .index_name(T::global_index_name())
        // Secondary indexes only expose their projected attributes; DynamoDB rejects AllAttributes.
        // See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.SelectingAttributes.html
        .select(Select::AllProjectedAttributes)
        .set_return_consumed_capacity(None)
        .scan_index_forward(scan_index_forward)
        .filter_expression(filter_expression)
        .limit(limit as i32);

    // Set exclusive start key for pagination if provided
    if let Some(start_key) = exclusive_start_key {
        if let Some(gsi_sort_key_field) = T::GSI_SORT_KEY {
            builder = builder
                .exclusive_start_key(
                    T::GSI_PARTITION_KEY,
                    AttributeValue::S(gsi_partition_key.clone()),
                )
                .exclusive_start_key(gsi_sort_key_field, AttributeValue::S(start_key));
        }
    }

    // Build key condition expression using GSI keys
    if let (Some(gsi_sort_key_name), Some(gsi_sort_value)) = (T::GSI_SORT_KEY, gsi_sort_key) {
        builder = builder
            .key_condition_expression(format!(
                "{} = :hash_value and {} = :range_value",
                T::GSI_PARTITION_KEY,
                gsi_sort_key_name
            ))
            .expression_attribute_values(":hash_value", AttributeValue::S(gsi_partition_key))
            .expression_attribute_values(":range_value", AttributeValue::S(gsi_sort_value));
    } else {
        builder = builder
            .key_condition_expression(format!("{} = :hash_value", T::GSI_PARTITION_KEY))
            .expression_attribute_values(":hash_value", AttributeValue::S(gsi_partition_key));
    }

    for (key, value) in filter_expression_values {
        builder = builder.expression_attribute_values(key, value);
    }

    let result = builder.send().await?;

    Ok(OutputItems::from((result, limit)))
}

/// Count items by GSI partition key
pub async fn count_gsi_items<T>(gsi_partition_key: String) -> Result<usize, Error>
where
    T: GSITable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    validation::validate_gsi_keys::<T>();

    let client = T::dynamodb_client().await;
    let builder = query_builder::QueryBuilder::for_gsi::<T>();
    let result = builder
        .build_count_query(client, gsi_partition_key)
        .send()
        .await?;

    Ok(result.count as usize)
}

/// Batch read output containing comprehensive metrics and tracking
///
/// Tracks which items were successfully retrieved, which keys failed after retries,
/// execution time, and retry attempts.
#[must_use = "batch read results contain failed keys and metrics that should be checked"]
#[derive(Debug)]
pub struct BatchReadOutput<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    /// Items successfully retrieved
    pub items: Vec<T>,

    /// Keys that failed to be retrieved after all retry attempts
    pub failed_keys: Vec<CompositeKey<T::PK, T::SK>>,

    /// Total execution time including all retries
    pub total_duration: Duration,

    /// Number of retry attempts made (0 means no retries needed)
    pub retry_count: usize,

    /// The capacity units consumed by the entire <code>BatchGetItem</code> operation.
    pub consumed_capacity: Vec<ConsumedCapacity>,
}

impl<T> BatchReadOutput<T>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    fn new() -> Self {
        Self {
            items: Vec::new(),
            failed_keys: Vec::new(),
            total_duration: Duration::ZERO,
            retry_count: 0,
            consumed_capacity: Vec::new(),
        }
    }

    /// Check if all operations were successful
    pub fn is_success(&self) -> bool {
        self.failed_keys.is_empty()
    }

    /// Get total number of items requested
    pub fn total_requested(&self) -> usize {
        self.items.len() + self.failed_keys.len()
    }

    /// Get success rate as a percentage (0.0 to 100.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.total_requested();
        if total == 0 {
            return 100.0;
        }
        (self.items.len() as f64 / total as f64) * 100.0
    }
}

/// Batch retrieve multiple items from a DynamoDB table
///
/// Fetches multiple items in a single batch request using their primary keys.
/// DynamoDB processes up to 100 items per batch, automatically chunking larger requests.
///
/// # Type Parameters
///
/// * `T` - A type implementing `DynamoTable` trait
///
/// # Arguments
///
/// * `values` - Vector of composite keys (partition_key, optional sort_key) to retrieve
///
/// # Returns
///
/// Returns `BatchReadOutput<T>` containing the retrieved items and any unprocessed keys
/// that need to be retried.
///
/// # Automatic Retry Behavior
///
/// **Built-in retry with exponential backoff**: This function automatically retries
/// unprocessed keys up to 5 times with exponential backoff (100ms, 200ms, 400ms, 800ms, 1600ms).
///
/// DynamoDB may not retrieve all keys in one attempt due to:
/// - **Throttling** - Provisioned throughput limits exceeded
/// - **Capacity limits** - Table or account-level read limits
/// - **Temporary issues** - Network problems or service hiccups
///
/// **After automatic retries**, any remaining failed keys will be returned in
/// `failed_keys`. If this is non-empty, consider logging for investigation or
/// implementing additional retry logic with longer delays.
///
/// # Example
///
/// ```no_run
/// use dynamo_table::{DynamoTable, CompositeKey, table::batch_get};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Serialize, Deserialize)]
/// # struct User { user_id: String, name: String }
/// # impl DynamoTable for User {
/// #     type PK = String;
/// #     type SK = String;
/// #     const TABLE: &'static str = "users";
/// #     const PARTITION_KEY: &'static str = "user_id";
/// #     fn partition_key(&self) -> String { self.user_id.clone() }
/// # }
///
/// # async fn example() -> Result<(), dynamo_table::Error> {
/// // Get multiple users by their IDs
/// let keys: Vec<CompositeKey<String, String>> = vec![
///     ("user1".to_string(), None),
///     ("user2".to_string(), None),
///     ("user3".to_string(), None),
/// ];
///
/// // Automatic retry is built-in! Function retries up to 5 times automatically
/// let result = batch_get::<User>(keys).await?;
///
/// println!("Retrieved {} users", result.items.len());
/// for user in &result.items {
///     println!("User: {}", user.name);
/// }
///
/// // Check if any keys failed after all automatic retries
/// if !result.failed_keys.is_empty() {
///     eprintln!("WARNING: {} keys failed after 5 retry attempts",
///         result.failed_keys.len());
///     // Log to monitoring system for investigation
/// }
/// # Ok(())
/// # }
/// ```
pub async fn batch_get<T>(
    values: Vec<CompositeKey<T::PK, T::SK>>,
) -> Result<BatchReadOutput<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
{
    let start_time = std::time::Instant::now();
    let retries = T::BATCH_RETRIES_CONFIG;

    // Initial call
    let mut result = batch_get_internal::<T>(values).await?;

    // Retry loop for failed keys
    let mut retry_count = 0;

    while !result.failed_keys.is_empty() && retry_count < retries.max_retries {
        // Wait with exponential backoff
        tokio::time::sleep(retry_config::retry_delay(
            retry_count,
            retries.initial_delay,
            retries.max_delay,
        ))
        .await;

        retry_count += 1;

        // Take failed keys for retry
        let unprocessed_keys = std::mem::take(&mut result.failed_keys);

        // Retry the failed keys
        let retry_result = batch_get_internal::<T>(unprocessed_keys).await?;

        // Merge results
        result.items.extend(retry_result.items);
        result.failed_keys.extend(retry_result.failed_keys);
        result
            .consumed_capacity
            .extend(retry_result.consumed_capacity);
    }

    result.total_duration = start_time.elapsed();
    result.retry_count = retry_count;

    Ok(result)
}

/// Internal batch get implementation without retry logic
pub(super) async fn batch_get_internal<T>(
    values: Vec<CompositeKey<T::PK, T::SK>>,
) -> Result<BatchReadOutput<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
{
    if values.is_empty() {
        return Ok(BatchReadOutput::new());
    }

    // Combination of partition key and sort_key
    let mut keys: Vec<HashMap<String, AttributeValue>> = Vec::with_capacity(values.len());

    for (partition_key, sort_key) in values {
        let mut item = HashMap::new();
        let _ = item.insert(
            T::PARTITION_KEY.to_string(),
            AttributeValue::S(partition_key.to_string()),
        );
        if let Some(sort_key) = sort_key {
            let _ = item.insert(
                T::SORT_KEY.expect("safety: sort_key is set").to_string(),
                AttributeValue::S(sort_key.to_string()),
            );
        }

        keys.push(item);
    }

    let output = BatchReadOutput::<T>::new();

    let batches: Vec<KeysAndAttributes> = keys
        .chunks(batch_processor::BATCH_READ_SIZE)
        .map(|data| {
            KeysAndAttributes::builder()
                .set_keys(Some(data.to_vec()))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let client = T::dynamodb_client().await;

    let concurrency = batches.len().min(batch_processor::DEFAULT_CONCURRENCY); // Limit concurrency to 10 or number of batches

    stream::iter(batches.into_iter().map(|batch| {
        client
            .batch_get_item()
            .request_items(T::TABLE, batch)
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
    }))
    .buffer_unordered(concurrency)
    .map_err(Into::<Error>::into)
    .try_fold(output, |mut builder, result| async {
        if let Some(responses) = result.responses {
            for items in responses.into_values() {
                let items = from_items(items)?;
                builder.items.extend(items);
            }
        }

        if let Some(unprocessed_keys) = result.unprocessed_keys {
            for (_table, keys_attrs) in unprocessed_keys {
                for key_map in keys_attrs.keys {
                    // Deserialize partition key directly to correct type
                    if let Some(pk_attr) = key_map.get(T::PARTITION_KEY) {
                        let pk: T::PK = from_attribute_value(pk_attr.clone())?;

                        // Deserialize sort key if present
                        let sk: Option<T::SK> = T::SORT_KEY
                            .and_then(|sk_name| key_map.get(sk_name))
                            .map(|sk_attr| from_attribute_value(sk_attr.clone()))
                            .transpose()?;

                        builder.failed_keys.push((pk, sk));
                    }
                }
            }
        }

        if let Some(capacities) = result.consumed_capacity {
            builder.consumed_capacity.extend(capacities);
        }

        Ok(builder)
    })
    .await
}
