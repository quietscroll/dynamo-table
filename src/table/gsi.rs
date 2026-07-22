use aws_sdk_dynamodb::types::AttributeValue;
use serde::Serialize;
use serde_dynamo::to_item;
use std::{collections::HashMap, fmt, future::Future};

use crate::table::helpers::{query_builder, validation};
use crate::table::types::{Cursor, OutputItems};
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

    /// Query items using the GSI.
    ///
    /// Use `OutputItems::start_cursor()` to request the next page.
    fn query_gsi_items(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        limit: Option<u16>,
        exclusive_start_cursor: Option<Cursor<Self>>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_gsi_items::<Self>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_cursor,
            limit,
            true,
            None,
        )
    }

    /// Query items using the GSI, retrieving only the specified attributes.
    ///
    /// The returned items are still deserialized as `Self`, so selected attributes
    /// must be sufficient for the model to deserialize.
    fn query_gsi_items_with_projection<'a>(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        limit: Option<u16>,
        exclusive_start_cursor: Option<Cursor<Self>>,
        projection_attributes: Option<&'a [&'a str]>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> + 'a
    where
        Self: 'a,
    {
        query_gsi_items::<Self>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_cursor,
            limit,
            true,
            projection_attributes,
        )
    }

    /// Query items using the GSI in reverse order.
    fn reverse_query_gsi_items(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        limit: Option<u16>,
        exclusive_start_cursor: Option<Cursor<Self>>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_gsi_items::<Self>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_cursor,
            limit,
            false,
            None,
        )
    }

    /// Query items using the GSI in reverse order, retrieving only the specified attributes.
    ///
    /// The returned items are still deserialized as `Self`, so selected attributes
    /// must be sufficient for the model to deserialize.
    fn reverse_query_gsi_items_with_projection<'a>(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        limit: Option<u16>,
        exclusive_start_cursor: Option<Cursor<Self>>,
        projection_attributes: Option<&'a [&'a str]>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> + 'a
    where
        Self: 'a,
    {
        query_gsi_items::<Self>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_cursor,
            limit,
            false,
            projection_attributes,
        )
    }

    /// Query a single item using the GSI
    fn query_gsi_item(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
    ) -> impl Future<Output = Result<Option<Self>, Error>> {
        query_gsi_item::<Self>(gsi_partition_key, gsi_sort_key)
    }

    /// Query items using the GSI with filter expression.
    ///
    /// Use `OutputItems::start_cursor()` to request the next page.
    fn query_gsi_items_with_filter<U: Serialize>(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        exclusive_start_cursor: Option<Cursor<Self>>,
        limit: Option<u16>,
        scan_index_forward: bool,
        filter_expression: String,
        filter_expression_values: U,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> {
        query_gsi_items_with_filter::<Self, U>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_cursor,
            limit,
            scan_index_forward,
            filter_expression,
            filter_expression_values,
        )
    }

    /// Query items using the GSI with filter expression, retrieving only the specified attributes.
    ///
    /// The returned items are still deserialized as `Self`, so selected attributes
    /// must be sufficient for the model to deserialize.
    #[allow(clippy::too_many_arguments)]
    fn query_gsi_items_with_filter_and_projection<'a, U: Serialize>(
        gsi_partition_key: String,
        gsi_sort_key: Option<String>,
        exclusive_start_cursor: Option<Cursor<Self>>,
        limit: Option<u16>,
        scan_index_forward: bool,
        filter_expression: String,
        filter_expression_values: U,
        projection_attributes: Option<&'a [&'a str]>,
    ) -> impl Future<Output = Result<OutputItems<Self>, Error>> + 'a
    where
        U: 'a,
        Self: 'a,
    {
        query_gsi_items_with_filter_and_projection::<Self, U>(
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_cursor,
            limit,
            scan_index_forward,
            filter_expression,
            filter_expression_values,
            projection_attributes,
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
        query_gsi_items::<T>(gsi_partition_key, gsi_sort_key, None, Some(1), true, None).await?;
    Ok(output.items.pop())
}

/// Query items from a GSI
async fn query_gsi_items<T>(
    gsi_partition_key: String,
    gsi_sort_key: Option<String>,
    exclusive_start_cursor: Option<Cursor<T>>,
    limit: Option<u16>,
    scan_index_forward: bool,
    projection_attributes: Option<&[&str]>,
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
            exclusive_start_cursor.as_ref().map(|cursor| {
                cursor
                    .sk
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| cursor.pk.to_string())
            }),
            exclusive_start_cursor
                .as_ref()
                .map(|cursor| cursor.pk.to_string()),
            limit,
            scan_index_forward,
            projection_attributes,
        )
        .send()
        .await?;

    #[cfg(feature = "consumed_capacity_stats")]
    crate::consumed_capacity::stats::record_from_option(result.consumed_capacity.as_ref());

    Ok(OutputItems::from((result, limit)))
}

/// Query GSI items with filter expression
pub async fn query_gsi_items_with_filter<T, U>(
    gsi_partition_key: String,
    gsi_sort_key: Option<String>,
    exclusive_start_cursor: Option<Cursor<T>>,
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
    query_gsi_items_with_filter_and_projection::<T, U>(
        gsi_partition_key,
        gsi_sort_key,
        exclusive_start_cursor,
        limit,
        scan_index_forward,
        filter_expression,
        filter_expression_values,
        None,
    )
    .await
}

/// Query GSI items with filter expression and optional projection attributes
#[allow(clippy::too_many_arguments)]
pub async fn query_gsi_items_with_filter_and_projection<T, U>(
    gsi_partition_key: String,
    gsi_sort_key: Option<String>,
    exclusive_start_cursor: Option<Cursor<T>>,
    limit: Option<u16>,
    scan_index_forward: bool,
    filter_expression: String,
    filter_expression_values: U,
    projection_attributes: Option<&[&str]>,
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

    let exclusive_start_key = exclusive_start_cursor.as_ref().map(|cursor| {
        cursor
            .sk
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| cursor.pk.to_string())
    });
    let exclusive_start_table_pk = exclusive_start_cursor
        .as_ref()
        .map(|cursor| cursor.pk.to_string());

    let client = T::dynamodb_client().await;
    let builder = query_builder::QueryBuilder::for_gsi::<T>();
    let mut builder = builder
        .build_query(
            client,
            gsi_partition_key,
            gsi_sort_key,
            exclusive_start_key,
            exclusive_start_table_pk,
            limit,
            scan_index_forward,
            projection_attributes,
        )
        .filter_expression(filter_expression);

    for (key, value) in filter_expression_values {
        builder = builder.expression_attribute_values(key, value);
    }

    let result = builder.send().await?;

    #[cfg(feature = "consumed_capacity_stats")]
    crate::consumed_capacity::stats::record_from_option(result.consumed_capacity.as_ref());

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

    #[cfg(feature = "consumed_capacity_stats")]
    crate::consumed_capacity::stats::record_from_option(result.consumed_capacity.as_ref());

    Ok(result.count as usize)
}
