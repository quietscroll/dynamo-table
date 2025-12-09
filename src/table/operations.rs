use aws_sdk_dynamodb::operation;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput;
use aws_sdk_dynamodb::operation::put_item::PutItemOutput;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnConsumedCapacity, ReturnValue, Select};
use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures_util::TryStreamExt;
use serde::{Serialize, de::DeserializeOwned};
use serde_dynamo::{from_item, to_item};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::time::Duration;
use tokio_stream::Stream;

use crate::assert_not_reserved_key;
use crate::error::Error;
use crate::table::batch::{BatchReadOutput, BatchWriteOutput, batch_get, batch_write};
use crate::table::helpers::{query_builder, validation};
use crate::table::types::{CompositeKey, OutputItems, RetryConfig};

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
