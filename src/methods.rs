use crate::table::{CompositeKey, DynamoTable};
use std::fmt;

/// Convenience methods over DynamoTable using associated key types
pub trait DynamoTableMethods: DynamoTable
where
    Self::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + PartialEq,
    Self::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + PartialEq,
{
    /// Visits the values representing the difference, i.e., the values that are in self but not in other.
    fn left_diff(left: Vec<Self>, right: Vec<CompositeKey<Self::PK, Self::SK>>) -> Vec<Self> {
        left.into_iter()
            .filter(|item| !right.contains(&item.composite_key()))
            .collect()
    }

    /// Visits the values representing the difference, i.e., the values that are in other but not in self.
    fn right_diff(
        left: Vec<Self>,
        right: Vec<CompositeKey<Self::PK, Self::SK>>,
    ) -> Vec<CompositeKey<Self::PK, Self::SK>> {
        let left_keys: Vec<CompositeKey<Self::PK, Self::SK>> =
            left.iter().map(|item| item.composite_key()).collect();

        right
            .into_iter()
            .filter(|key| !left_keys.contains(key))
            .collect()
    }
}

impl<T> DynamoTableMethods for T
where
    T: DynamoTable,
    T::PK: fmt::Display + Clone + Send + Sync + fmt::Debug + PartialEq,
    T::SK: fmt::Display + Clone + Send + Sync + fmt::Debug + PartialEq,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestItem {
        id: String,
        value: i32,
    }

    impl DynamoTable for TestItem {
        type PK = String;
        type SK = String;

        const TABLE: &'static str = "test_table";
        const PARTITION_KEY: &'static str = "id";
        const SORT_KEY: Option<&'static str> = None;

        fn partition_key(&self) -> Self::PK {
            self.id.clone()
        }
    }

    #[test]
    fn test_left_diff() {
        let left = vec![
            TestItem {
                id: "1".to_string(),
                value: 100,
            },
            TestItem {
                id: "2".to_string(),
                value: 200,
            },
            TestItem {
                id: "3".to_string(),
                value: 300,
            },
        ];

        let right = vec![("1".to_string(), None), ("2".to_string(), None)];

        let diff = TestItem::left_diff(left, right);

        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].id, "3");
        assert_eq!(diff[0].value, 300);
    }

    #[test]
    fn test_left_diff_empty() {
        let left = vec![TestItem {
            id: "1".to_string(),
            value: 100,
        }];

        let right = vec![("1".to_string(), None)];

        let diff = TestItem::left_diff(left, right);
        assert_eq!(diff.len(), 0);
    }

    #[test]
    fn test_right_diff() {
        let left = vec![TestItem {
            id: "1".to_string(),
            value: 100,
        }];

        let right = vec![
            ("1".to_string(), None),
            ("2".to_string(), None),
            ("3".to_string(), None),
        ];

        let diff = TestItem::right_diff(left, right);

        assert_eq!(diff.len(), 2);
        assert!(diff.contains(&("2".to_string(), None)));
        assert!(diff.contains(&("3".to_string(), None)));
    }

    #[test]
    fn test_right_diff_empty() {
        let left = vec![TestItem {
            id: "1".to_string(),
            value: 100,
        }];

        let right = vec![("1".to_string(), None)];

        let diff = TestItem::right_diff(left, right);
        assert_eq!(diff.len(), 0);
    }

    #[test]
    fn test_both_diffs_together() {
        let left = vec![
            TestItem {
                id: "1".to_string(),
                value: 100,
            },
            TestItem {
                id: "2".to_string(),
                value: 200,
            },
        ];

        let right = vec![("2".to_string(), None), ("3".to_string(), None)];

        let left_diff = TestItem::left_diff(left.clone(), right.clone());
        let right_diff = TestItem::right_diff(left, right);

        assert_eq!(left_diff.len(), 1);
        assert_eq!(left_diff[0].id, "1");

        assert_eq!(right_diff.len(), 1);
        assert!(right_diff.contains(&("3".to_string(), None)));
    }
}
