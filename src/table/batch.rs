use aws_sdk_dynamodb::types::{
    AttributeValue, ConsumedCapacity, DeleteRequest, ItemCollectionMetrics, KeysAndAttributes,
    PutRequest, ReturnConsumedCapacity, ReturnItemCollectionMetrics, WriteRequest,
};
use futures_util::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde_dynamo::{from_attribute_value, from_item, from_items, to_item};
use std::{
    collections::HashMap,
    fmt,
    time::{Duration, Instant},
};
use tokio::time::sleep;
use tokio_stream::{self as stream};

use crate::Error;
use crate::table::DynamoTable;
use crate::table::helpers::{batch_processor, retry_config};
use crate::table::types::CompositeKey;

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
pub async fn batch_write<T>(update: Vec<T>, delete: Vec<T>) -> Result<BatchWriteOutput<T>, Error>
where
    T: DynamoTable + Clone,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    let start_time = Instant::now();
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
        sleep(retry_config::retry_delay(
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

    let concurrency = batches.len().min(batch_processor::DEFAULT_CONCURRENCY);

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
pub async fn batch_get<T>(
    values: Vec<CompositeKey<T::PK, T::SK>>,
) -> Result<BatchReadOutput<T>, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + DeserializeOwned,
{
    let start_time = Instant::now();
    let retries = T::BATCH_RETRIES_CONFIG;

    // Initial call
    let mut result = batch_get_internal::<T>(values).await?;

    // Retry loop for failed keys
    let mut retry_count = 0;

    while !result.failed_keys.is_empty() && retry_count < retries.max_retries {
        // Wait with exponential backoff
        sleep(retry_config::retry_delay(
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

    let concurrency = batches.len().min(batch_processor::DEFAULT_CONCURRENCY);

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
