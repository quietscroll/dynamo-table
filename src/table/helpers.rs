use crate::table::{DynamoTable, GSITable};
use serde::Serialize;
use serde_dynamo::{AttributeValue, to_item};
use std::{collections::HashMap, fmt};

/// Retry configuration for batch operations
pub(crate) mod retry_config {
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
    pub(crate) fn retry_delay(attempt: usize, initial: Duration, max: Duration) -> Duration {
        let delay_ms = initial.as_millis() as u64 * 2u64.pow(attempt as u32);
        let capped_delay = delay_ms.min(max.as_millis() as u64);
        Duration::from_millis(capped_delay)
    }
}

/// Validation helpers for table operations
///
/// These validators check for DynamoDB reserved words in key names and field names
/// to prevent runtime errors. All validations only run in debug builds.
pub(crate) mod validation {
    use super::*;

    /// Validate a single key is not a reserved word
    #[inline]
    fn validate_key(key: &str) {
        if cfg!(debug_assertions) {
            crate::assert_not_reserved_key(key);
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
    pub(crate) fn validate_table_keys<T>()
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
    pub(crate) fn validate_gsi_keys<T>()
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
    pub(crate) fn validate_field_names(field_names: &[&str]) {
        if cfg!(debug_assertions) {
            for field in field_names {
                validate_key(field);
            }
        }
    }

    /// Validate filter expression parameter names
    ///
    /// Ensures filter expression value keys (e.g., `:paramName`) are not reserved words.
    pub(crate) fn validate_filter_expression_values<U: Serialize>(filter_expression_values: &U) {
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
pub(crate) mod expressions {
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;

    pub(crate) struct KeyConditionBuilder {
        expression: String,
        values: HashMap<String, AttributeValue>,
    }

    impl KeyConditionBuilder {
        pub(crate) fn new() -> Self {
            Self {
                expression: String::new(),
                values: HashMap::new(),
            }
        }

        pub(crate) fn with_partition_key(mut self, field: &str, value: String) -> Self {
            self.expression = format!("{field} = :hash_value");
            let _ = self
                .values
                .insert(":hash_value".to_string(), AttributeValue::S(value));
            self
        }

        pub(crate) fn with_sort_key(mut self, field: &str, value: String) -> Self {
            if !self.expression.is_empty() {
                self.expression.push_str(" and ");
            }
            self.expression.push_str(&format!("{field} = :range_value"));
            let _ = self
                .values
                .insert(":range_value".to_string(), AttributeValue::S(value));
            self
        }

        pub(crate) fn build(self) -> (String, HashMap<String, AttributeValue>) {
            (self.expression, self.values)
        }
    }
}

/// Shared query builder for DynamoDB operations
pub(crate) mod query_builder {
    use super::{DynamoTable, GSITable, expressions};
    use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
    use aws_sdk_dynamodb::types::{AttributeValue, Select};
    use std::collections::HashMap;
    use std::fmt;

    pub(crate) struct QueryBuilder<'a> {
        table_name: &'a str,
        index_name: Option<String>,
        partition_key_field: &'a str,
        sort_key_field: Option<&'a str>,
    }

    impl<'a> QueryBuilder<'a> {
        /// Create builder for main table queries
        pub(crate) fn for_table<T>() -> Self
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
        pub(crate) fn for_gsi<T>() -> Self
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
        pub(crate) fn for_index<T>(index_name: String) -> Self
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
        pub(crate) fn build_query(
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
        pub(crate) fn build_count_query(
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
pub(crate) mod batch_processor {
    use crate::Error;
    use futures_util::{StreamExt, TryStreamExt};
    use std::{cmp, future::Future};
    use tokio_stream::{self as stream};

    /// Generic batch processor for handling chunking and concurrent execution
    #[allow(dead_code)]
    pub(crate) struct BatchProcessor {
        chunk_size: usize,
        concurrency: usize,
    }

    impl BatchProcessor {
        #[allow(dead_code)]
        pub(crate) fn new(chunk_size: usize, concurrency: usize) -> Self {
            Self {
                chunk_size,
                concurrency,
            }
        }

        /// Process items in batches with concurrent execution
        #[allow(dead_code)]
        pub(crate) async fn process<T, R, F, Fut, O, M>(
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
    pub(crate) const BATCH_WRITE_SIZE: usize = 25;
    pub(crate) const BATCH_READ_SIZE: usize = 100;
    pub(crate) const DEFAULT_CONCURRENCY: usize = 10;
}
