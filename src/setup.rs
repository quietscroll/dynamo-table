use aws_sdk_dynamodb::operation::create_table::CreateTableOutput;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ProvisionedThroughput, ScalarAttributeType,
};
use std::fmt;

use crate::error::Error;
use crate::table::{DynamoTable, GSITable};
use crate::{assert_not_reserved_key, dynamodb_client};

/// Create a table from DynamoTable
///
/// Automatically initializes the DynamoDB client with defaults if not already initialized.
/// This makes it convenient to use in tests and examples.
pub async fn table<T>() -> Result<CreateTableOutput, Error>
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    let client = dynamodb_client().await;

    assert_not_reserved_key(T::PARTITION_KEY);

    let mut builder = client
        .create_table()
        .table_name(T::TABLE)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(T::PARTITION_KEY)
                .key_type(KeyType::Hash)
                .build()?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(T::PARTITION_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(10)
                .write_capacity_units(10)
                .build()?,
        );

    if let Some(sort_key) = T::SORT_KEY {
        assert_not_reserved_key(sort_key);

        builder = builder
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(sort_key)
                    .key_type(KeyType::Range)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(sort_key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            );
    }

    // Ignore ResourceInUseException - table already exists
    match builder.send().await {
        Ok(output) => Ok(output),
        Err(e) => {
            if let Some(service_error) = e.as_service_error() {
                if matches!(service_error, aws_sdk_dynamodb::operation::create_table::CreateTableError::ResourceInUseException(_)) {
                    // Table already exists - return empty output (callers ignore it anyway)
                    return Ok(CreateTableOutput::builder().build());
                }
            }
            Err(e.into())
        }
    }
}

/// Create a table from DynamoTable with GSI support
///
/// Automatically initializes the DynamoDB client with defaults if not already initialized.
/// This makes it convenient to use in tests and examples.
pub async fn table_with_gsi<T>() -> Result<CreateTableOutput, Error>
where
    T: DynamoTable + GSITable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug,
{
    let client = dynamodb_client().await;

    assert_not_reserved_key(T::PARTITION_KEY);
    assert_not_reserved_key(T::GSI_PARTITION_KEY);

    let mut builder = client
        .create_table()
        .table_name(T::TABLE)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(T::PARTITION_KEY)
                .key_type(KeyType::Hash)
                .build()?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(T::PARTITION_KEY)
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        )
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(10)
                .write_capacity_units(10)
                .build()?,
        );

    // Add sort key if defined for the main table
    if let Some(sort_key) = T::SORT_KEY {
        assert_not_reserved_key(sort_key);

        builder = builder
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(sort_key)
                    .key_type(KeyType::Range)
                    .build()?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(sort_key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            );
    }

    // Add GSI partition key attribute definition
    builder = builder.attribute_definitions(
        AttributeDefinition::builder()
            .attribute_name(T::GSI_PARTITION_KEY)
            .attribute_type(ScalarAttributeType::S)
            .build()?,
    );

    // Build the GSI
    let mut global_secondary_index = GlobalSecondaryIndex::builder()
        .index_name(T::global_index_name())
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(T::GSI_PARTITION_KEY)
                .key_type(KeyType::Hash)
                .build()?,
        )
        .projection(
            Projection::builder()
                .projection_type(ProjectionType::All)
                .build(),
        )
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(10)
                .write_capacity_units(10)
                .build()?,
        );

    // Add GSI sort key if defined
    if let Some(gsi_sort_key) = T::GSI_SORT_KEY {
        assert_not_reserved_key(gsi_sort_key);

        // Only add attribute definition if it's different from the main table sort key
        if T::SORT_KEY != Some(gsi_sort_key) {
            builder = builder.attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(gsi_sort_key)
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            );
        }

        global_secondary_index = global_secondary_index.key_schema(
            KeySchemaElement::builder()
                .attribute_name(gsi_sort_key)
                .key_type(KeyType::Range)
                .build()?,
        );
    }

    builder = builder.global_secondary_indexes(global_secondary_index.build()?);

    // Ignore ResourceInUseException - table already exists
    match builder.send().await {
        Ok(output) => Ok(output),
        Err(e) => {
            if let Some(service_error) = e.as_service_error() {
                if matches!(service_error, aws_sdk_dynamodb::operation::create_table::CreateTableError::ResourceInUseException(_)) {
                    // Table already exists - return empty output (callers ignore it anyway)
                    return Ok(CreateTableOutput::builder().build());
                }
            }
            Err(e.into())
        }
    }
}
