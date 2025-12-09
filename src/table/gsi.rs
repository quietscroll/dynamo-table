use aws_sdk_dynamodb::types::{AttributeValue, Select};
use serde::Serialize;
use serde_dynamo::to_item;
use std::{collections::HashMap, fmt, future::Future};

use crate::table::helpers::{query_builder, validation};
use crate::table::types::OutputItems;
use crate::{Error, table::DynamoTable};

/// Global Secondary Index (GSI) table trait for querying on alternate keys
pub trait GSITable: DynamoTable {
    /// GSI partition key field name
    const GSI_PARTITION_KEY: &'static str;
    /// GSI sort key field name (optional)
    const GSI_SORT_KEY: Option<&'static str> = None;

    /// Returns the GSI partition key value for this item
    fn gsi_partition_key(&self) -> String;
    /// Returns the GSI sort key value for this item (optional)
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
