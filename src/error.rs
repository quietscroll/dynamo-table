use aws_sdk_dynamodb::error::BuildError;
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::get_item::GetItemError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::scan::ScanError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::error::ConditionalCheckFailedException;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;
use serde_dynamo::Error as SerdeDynamoError;
use std::error::Error as StdError;
use std::fmt;

type DynamoPutError = SdkError<PutItemError, Response>;
type DynamoUpdateError = SdkError<UpdateItemError, Response>;
type DynamoGetError = SdkError<GetItemError, Response>;
type DynamoQueryError = SdkError<QueryError, Response>;
type DynamoScanError = SdkError<ScanError, Response>;
type DynamoDeleteItemError = SdkError<DeleteItemError, Response>;
type DynamoCreateTableError = SdkError<CreateTableError, Response>;
type DynamoBatchWriteItemError = SdkError<BatchWriteItemError, Response>;
type DynamoBatchGetItemError = SdkError<BatchGetItemError, Response>;
type DynamoDbError = SdkError<aws_sdk_dynamodb::Error, Response>;

/// DynamoDB table operation error
#[derive(Debug)]
pub enum Error {
    /// Serde DynamoDB serialization/deserialization error
    SerdeDynamo(SerdeDynamoError),
    /// Generic DynamoDB SDK error
    DynamoDB(DynamoDbError),
    /// DynamoDB request builder error
    BuildError(BuildError),
    /// DynamoDB PutItem operation error
    DynamoPutError(DynamoPutError),
    /// DynamoDB GetItem operation error
    DynamoGetError(DynamoGetError),
    /// DynamoDB Query operation error
    DynamoQueryError(DynamoQueryError),
    /// DynamoDB Scan operation error
    DynamoScanError(DynamoScanError),
    /// DynamoDB UpdateItem operation error
    DynamoUpdateError(DynamoUpdateError),
    /// DynamoDB DeleteItem operation error
    DynamoDeleteItemError(DynamoDeleteItemError),
    /// DynamoDB CreateTable operation error
    DynamoCreateTableError(DynamoCreateTableError),
    /// DynamoDB BatchWriteItem operation error
    DynamoBatchWriteItemError(DynamoBatchWriteItemError),
    /// DynamoDB BatchGetItem operation error
    DynamoBatchGetItemError(DynamoBatchGetItemError),
}

impl Error {
    /// Check if the error is a DynamoDB ConditionalCheckFailedException
    ///
    /// This is useful for detecting optimistic locking failures when using
    /// conditional expressions in update operations.
    ///
    /// # Example
    /// ```no_run
    /// # use dynamo_table::Error;
    /// # async fn example(error: Error) {
    /// if error.is_conditional_check_failed() {
    ///     // Handle optimistic locking failure
    ///     println!("Item was modified by another process");
    /// }
    /// # }
    /// ```
    pub fn is_conditional_check_failed(&self) -> bool {
        matches!(self, Error::DynamoUpdateError(dynamo_err) if {
            let source = dynamo_err.as_service_error();
            matches!(source, Some(UpdateItemError::ConditionalCheckFailedException(ConditionalCheckFailedException { .. })))
        })
    }

    /// Check if the error is a serialization/deserialization error
    ///
    /// Returns `true` for DynamoDB serialization errors.
    pub fn is_serialization_error(&self) -> bool {
        matches!(self, Error::SerdeDynamo(_))
    }

    /// Check if the error is a DynamoDB-related error
    ///
    /// Returns `true` for any DynamoDB operation error.
    pub fn is_dynamodb_error(&self) -> bool {
        !matches!(self, Error::SerdeDynamo(_))
    }
}

macro_rules! impl_from_error {
    ($name:ident, $variant:ident) => {
        impl From<$name> for Error {
            fn from(e: $name) -> Self {
                Error::$variant(e)
            }
        }
    };
    ($name:ident) => {
        impl From<$name> for Error {
            fn from(e: $name) -> Self {
                Error::$name(e)
            }
        }
    };
}

impl_from_error!(SerdeDynamoError, SerdeDynamo);
impl_from_error!(DynamoDbError, DynamoDB);
impl_from_error!(BuildError);
impl_from_error!(DynamoPutError);
impl_from_error!(DynamoGetError);
impl_from_error!(DynamoUpdateError);
impl_from_error!(DynamoQueryError);
impl_from_error!(DynamoScanError);
impl_from_error!(DynamoDeleteItemError);
impl_from_error!(DynamoCreateTableError);
impl_from_error!(DynamoBatchWriteItemError);
impl_from_error!(DynamoBatchGetItemError);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SerdeDynamo(e) => write!(f, "DynamoDB serialization error: {}", e),
            Error::DynamoDB(e) => write!(f, "DynamoDB operation error: {}", e),
            Error::BuildError(e) => write!(f, "DynamoDB request builder error: {}", e),
            Error::DynamoPutError(e) => {
                write!(f, "DynamoDB PutItem operation failed: {}", e)
            }
            Error::DynamoGetError(e) => {
                write!(f, "DynamoDB GetItem operation failed: {}", e)
            }
            Error::DynamoQueryError(e) => {
                write!(f, "DynamoDB Query operation failed: {}", e)
            }
            Error::DynamoScanError(e) => {
                write!(f, "DynamoDB Scan operation failed: {}", e)
            }
            Error::DynamoUpdateError(e) => {
                write!(f, "DynamoDB UpdateItem operation failed: {}", e)
            }
            Error::DynamoDeleteItemError(e) => {
                write!(f, "DynamoDB DeleteItem operation failed: {}", e)
            }
            Error::DynamoCreateTableError(e) => {
                write!(f, "DynamoDB CreateTable operation failed: {}", e)
            }
            Error::DynamoBatchWriteItemError(e) => {
                write!(f, "DynamoDB BatchWriteItem operation failed: {}", e)
            }
            Error::DynamoBatchGetItemError(e) => {
                write!(f, "DynamoDB BatchGetItem operation failed: {}", e)
            }
        }
    }
}

impl StdError for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_serialization_error() {
        // Test with a build error - should not be serialization error
        let err = Error::BuildError(BuildError::other("test"));
        assert!(!err.is_serialization_error());
    }

    #[test]
    fn test_is_dynamodb_error() {
        let err = Error::BuildError(BuildError::other("test"));
        assert!(err.is_dynamodb_error());
    }

    #[test]
    fn test_error_conversion() {
        let build_err = BuildError::other("test");
        let err: Error = build_err.into();
        assert!(matches!(err, Error::BuildError(_)));
    }

    #[test]
    fn test_error_debug() {
        let err = Error::BuildError(BuildError::other("test"));
        let debug = format!("{:?}", err);
        assert!(debug.contains("BuildError"));
    }
}
