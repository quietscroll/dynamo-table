use dynamo_table::{CompositeKey, DynamoTable, DynamoTableMethods, GSITable};
use serde::{Deserialize, Serialize};

/// Test user struct with simple partition key
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    user_id: String,
    email: String,
    name: String,
    age: u32,
}

impl DynamoTable for User {
    type PK = String;
    type SK = String;

    const TABLE: &'static str = "users";
    const PARTITION_KEY: &'static str = "user_id";
    const SORT_KEY: Option<&'static str> = None;

    fn partition_key(&self) -> Self::PK {
        self.user_id.clone()
    }
}

impl GSITable for User {
    const GSI_PARTITION_KEY: &'static str = "email";

    fn gsi_partition_key(&self) -> String {
        self.email.clone()
    }

    fn gsi_sort_key(&self) -> Option<String> {
        None
    }
}

/// Test order struct with composite key (partition + sort)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    user_id: String,
    order_id: String,
    total: f64,
    status: String,
}

impl DynamoTable for Order {
    type PK = String;
    type SK = String;

    const TABLE: &'static str = "orders";
    const PARTITION_KEY: &'static str = "user_id";
    const SORT_KEY: Option<&'static str> = Some("order_id");

    fn partition_key(&self) -> Self::PK {
        self.user_id.clone()
    }

    fn sort_key(&self) -> Option<Self::SK> {
        Some(self.order_id.clone())
    }
}

#[test]
fn test_user_table_constants() {
    assert_eq!(User::TABLE, "users");
    assert_eq!(User::PARTITION_KEY, "user_id");
    assert_eq!(User::SORT_KEY, None);
}

#[test]
fn test_user_primary_key() {
    let user = User {
        user_id: "user123".to_string(),
        email: "test@example.com".to_string(),
        name: "Test User".to_string(),
        age: 25,
    };

    assert_eq!(user.partition_key(), "user123".to_string());
    assert_eq!(user.sort_key(), None);
}

#[test]
fn test_user_composite_key() {
    let user = User {
        user_id: "user123".to_string(),
        email: "test@example.com".to_string(),
        name: "Test User".to_string(),
        age: 25,
    };

    let key: CompositeKey<String, String> = user.composite_key();
    assert_eq!(key.0, "user123");
    assert_eq!(key.1, None);
}

#[test]
fn test_user_gsi_keys() {
    let user = User {
        user_id: "user123".to_string(),
        email: "test@example.com".to_string(),
        name: "Test User".to_string(),
        age: 25,
    };

    assert_eq!(user.gsi_partition_key(), "test@example.com");
    assert_eq!(user.gsi_sort_key(), None);
}

#[test]
fn test_order_table_constants() {
    assert_eq!(Order::TABLE, "orders");
    assert_eq!(Order::PARTITION_KEY, "user_id");
    assert_eq!(Order::SORT_KEY, Some("order_id"));
}

#[test]
fn test_order_composite_key() {
    let order = Order {
        user_id: "user123".to_string(),
        order_id: "order456".to_string(),
        total: 99.99,
        status: "pending".to_string(),
    };

    assert_eq!(order.partition_key(), "user123");
    assert_eq!(order.sort_key(), Some("order456".to_string()));

    let key: CompositeKey<String, String> = order.composite_key();
    assert_eq!(key.0, "user123");
    assert_eq!(key.1, Some("order456".to_string()));
}

#[test]
fn test_dynamo_table_methods_left_diff() {
    let users = vec![
        User {
            user_id: "1".to_string(),
            email: "user1@example.com".to_string(),
            name: "User 1".to_string(),
            age: 20,
        },
        User {
            user_id: "2".to_string(),
            email: "user2@example.com".to_string(),
            name: "User 2".to_string(),
            age: 30,
        },
    ];

    let keys = vec![("1".to_string(), None)];

    let diff = User::left_diff(users, keys);

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].user_id, "2");
}

#[test]
fn test_dynamo_table_methods_right_diff() {
    let users = vec![User {
        user_id: "1".to_string(),
        email: "user1@example.com".to_string(),
        name: "User 1".to_string(),
        age: 20,
    }];

    let keys = vec![("1".to_string(), None), ("2".to_string(), None)];

    let diff = User::right_diff(users, keys);

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0], ("2".to_string(), None));
}

#[test]
fn test_serialization() {
    let user = User {
        user_id: "user123".to_string(),
        email: "test@example.com".to_string(),
        name: "Test User".to_string(),
        age: 25,
    };

    // Test that it can be serialized
    let json = serde_json::to_string(&user).unwrap();
    assert!(json.contains("user123"));
    assert!(json.contains("test@example.com"));

    // Test that it can be deserialized
    let deserialized: User = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, user);
}

#[test]
fn test_order_serialization() {
    let order = Order {
        user_id: "user123".to_string(),
        order_id: "order456".to_string(),
        total: 99.99,
        status: "completed".to_string(),
    };

    let json = serde_json::to_string(&order).unwrap();
    let deserialized: Order = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, order);
}

#[test]
fn test_default_page_size() {
    assert_eq!(User::DEFAULT_PAGE_SIZE, 10);
    assert_eq!(Order::DEFAULT_PAGE_SIZE, 10);
}

#[test]
fn test_partition_key_name() {
    let user = User {
        user_id: "test".to_string(),
        email: "test@test.com".to_string(),
        name: "Test".to_string(),
        age: 30,
    };

    assert_eq!(user.partition_key_name(), "user_id");
    assert_eq!(user.sort_key_name(), None);
}

#[test]
fn test_order_key_names() {
    let order = Order {
        user_id: "user1".to_string(),
        order_id: "order1".to_string(),
        total: 50.0,
        status: "active".to_string(),
    };

    assert_eq!(order.partition_key_name(), "user_id");
    assert_eq!(order.sort_key_name(), Some("order_id"));
}

// Test multiple users for diff operations
#[test]
fn test_multiple_users_diff() {
    let users = vec![
        User {
            user_id: "1".to_string(),
            email: "1@test.com".to_string(),
            name: "User 1".to_string(),
            age: 20,
        },
        User {
            user_id: "2".to_string(),
            email: "2@test.com".to_string(),
            name: "User 2".to_string(),
            age: 30,
        },
        User {
            user_id: "3".to_string(),
            email: "3@test.com".to_string(),
            name: "User 3".to_string(),
            age: 40,
        },
    ];

    let existing_keys = vec![("2".to_string(), None), ("4".to_string(), None)];

    let to_add = User::left_diff(users.clone(), existing_keys.clone());
    let to_remove = User::right_diff(users, existing_keys);

    assert_eq!(to_add.len(), 2); // Users 1 and 3 need to be added
    assert_eq!(to_remove.len(), 1); // Key 4 needs to be removed

    assert!(to_add.iter().any(|u| u.user_id == "1"));
    assert!(to_add.iter().any(|u| u.user_id == "3"));
    assert!(to_remove.contains(&("4".to_string(), None)));
}
