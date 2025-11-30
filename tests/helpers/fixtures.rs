/// Common test fixtures and data structures
///
/// Defines reusable test models that implement DynamoTable and GSITable
/// for use across multiple test files.

use super::{Deserialize, DynamoTable, Serialize};
use dynamo_table::table::{GSITable, SortKey};

/// Simple test object with partition and sort key
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TestObject {
    pub game: String,
    pub age: String,
    pub ux: String,
    pub number2: usize,
}

impl DynamoTable for TestObject {
    type PK = String;
    type SK = String;
    const TABLE: &'static str = "tests_generic_objects";
    const PARTITION_KEY: &'static str = "game";
    const SORT_KEY: Option<&'static str> = Some("age");

    fn partition_key(&self) -> String {
        self.game.to_string()
    }

    fn sort_key(&self) -> SortKey<String> {
        Some(self.age.clone())
    }
}

/// Test object for counter operations
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TestCounters {
    pub imo: String,
    pub det: String,
    pub p1: usize,
    pub p2: usize,
}

impl DynamoTable for TestCounters {
    type PK = String;
    type SK = String;
    const TABLE: &'static str = "tests_generic_counters";
    const PARTITION_KEY: &'static str = "imo";

    fn partition_key(&self) -> String {
        self.imo.to_string()
    }
}

/// Test object with Global Secondary Index
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct TestGSIObject {
    pub game: String,
    pub age: String,
    pub user_id: String,
    pub created_at: String,
    pub ux: String,
}

impl DynamoTable for TestGSIObject {
    type PK = String;
    type SK = String;
    const TABLE: &'static str = "tests_gsi_objects";
    const PARTITION_KEY: &'static str = "game";
    const SORT_KEY: Option<&'static str> = Some("age");

    fn partition_key(&self) -> String {
        self.game.to_string()
    }

    fn sort_key(&self) -> SortKey<String> {
        Some(self.age.clone())
    }
}

impl GSITable for TestGSIObject {
    const GSI_PARTITION_KEY: &'static str = "user_id";
    const GSI_SORT_KEY: Option<&'static str> = Some("age");

    fn gsi_partition_key(&self) -> String {
        self.user_id.clone()
    }

    fn gsi_sort_key(&self) -> Option<String> {
        Some(self.age.clone())
    }
}
