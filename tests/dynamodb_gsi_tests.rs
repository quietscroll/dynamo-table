/// DynamoDB Global Secondary Index Tests
///
/// Tests GSI query operations including filtering and counting.
use serial_test::serial;
use std::collections::HashMap;

mod helpers;
use dynamo_table::table::GSITable;
use helpers::*;

/// Helper to clean and setup GSI table
async fn setup_gsi_table() {
    // Client auto-initializes with defaults on first use
    let client = dynamo_table::dynamodb_client().await;
    let _ = client
        .delete_table()
        .table_name("tests_gsi_objects")
        .send()
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let _ = setup::table_with_gsi::<TestGSIObject>().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

/// Test GSI query basic functionality
#[tokio::test]
#[serial]
async fn test_gsi_query_basic() {
    setup_gsi_table().await;

    let user_id = "gsi_user_1";

    // Create test objects with same user_id
    let objects = vec![
        TestGSIObject {
            game: "game1".to_string(),
            age: "age1".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-01-01T10:00:00Z".to_string(),
            ux: "test1".to_string(),
        },
        TestGSIObject {
            game: "game2".to_string(),
            age: "age2".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-01-02T10:00:00Z".to_string(),
            ux: "test2".to_string(),
        },
        TestGSIObject {
            game: "game3".to_string(),
            age: "age3".to_string(),
            user_id: "other_user".to_string(),
            created_at: "2024-01-03T10:00:00Z".to_string(),
            ux: "test3".to_string(),
        },
    ];

    for obj in &objects {
        obj.add_item().await.unwrap();
    }

    // Query by GSI
    let gsi_results = TestGSIObject::query_gsi_items(user_id.to_string(), None, Some(10), None)
        .await
        .unwrap();

    assert_eq!(
        gsi_results.items.len(),
        2,
        "Should find 2 items for user_id"
    );

    for result in &gsi_results.items {
        assert_eq!(result.user_id, user_id);
    }
}

/// Test GSI reverse query
#[tokio::test]
#[serial]
async fn test_gsi_reverse_query() {
    setup_gsi_table().await;

    let user_id = "gsi_reverse_user";

    // Create items
    for i in 0..5 {
        let obj = TestGSIObject {
            game: format!("game{}", i),
            age: format!("age{}", i),
            user_id: user_id.to_string(),
            created_at: format!("2024-01-0{}T10:00:00Z", i + 1),
            ux: "reverse_test".to_string(),
        };
        obj.add_item().await.unwrap();
    }

    // Query in reverse order
    let reverse_results =
        TestGSIObject::reverse_query_gsi_items(user_id.to_string(), None, Some(10), None)
            .await
            .unwrap();

    assert_eq!(reverse_results.items.len(), 5);

    // Results should be in descending order by age (GSI sort key)
    assert!(reverse_results.items[0].age > reverse_results.items[1].age);
}

/// Test GSI query single item
#[tokio::test]
#[serial]
async fn test_gsi_query_single() {
    setup_gsi_table().await;

    let user_id = "gsi_single_user";
    let age = "unique_age".to_string();

    let obj = TestGSIObject {
        game: "single_game".to_string(),
        age: age.clone(),
        user_id: user_id.to_string(),
        created_at: "2024-01-01T12:00:00Z".to_string(),
        ux: "single_test".to_string(),
    };

    obj.add_item().await.unwrap();

    // Query single item without sort key
    let result = TestGSIObject::query_gsi_item(user_id.to_string(), None)
        .await
        .unwrap();

    assert!(result.is_some());
    let found = result.unwrap();
    assert_eq!(found.user_id, user_id);
    assert_eq!(found.game, "single_game");

    // Query single item with sort key
    let result_with_sk = TestGSIObject::query_gsi_item(user_id.to_string(), Some(age))
        .await
        .unwrap();

    assert!(result_with_sk.is_some());
    assert_eq!(result_with_sk.unwrap().user_id, user_id);
}

/// Test GSI count items
#[tokio::test]
#[serial]
async fn test_gsi_count() {
    setup_gsi_table().await;

    let user_id = "gsi_count_user";

    // Create multiple items for the same user
    for i in 0..7 {
        let obj = TestGSIObject {
            game: format!("count_game{}", i),
            age: format!("age{}", i),
            user_id: user_id.to_string(),
            created_at: format!("2024-01-0{}T10:00:00Z", i + 1),
            ux: format!("count_test{}", i),
        };
        obj.add_item().await.unwrap();
    }

    // Count items by GSI partition key
    let count = TestGSIObject::count_gsi_items(user_id.to_string())
        .await
        .unwrap();

    assert_eq!(count, 7, "Should count 7 items for user");

    // Count for non-existent user
    let zero_count = TestGSIObject::count_gsi_items("nonexistent_user".to_string())
        .await
        .unwrap();

    assert_eq!(zero_count, 0, "Should count 0 for non-existent user");
}

/// Test GSI query with filter expression
#[tokio::test]
#[serial]
async fn test_gsi_with_filter() {
    setup_gsi_table().await;

    let user_id = "gsi_filter_user";

    // Create objects with different ux values
    let objects = vec![
        TestGSIObject {
            game: "filter_game1".to_string(),
            age: "age1".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-01-01T10:00:00Z".to_string(),
            ux: "match_this".to_string(),
        },
        TestGSIObject {
            game: "filter_game2".to_string(),
            age: "age2".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-01-02T10:00:00Z".to_string(),
            ux: "dont_match".to_string(),
        },
        TestGSIObject {
            game: "filter_game3".to_string(),
            age: "age3".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-01-03T10:00:00Z".to_string(),
            ux: "match_this".to_string(),
        },
    ];

    for obj in &objects {
        obj.add_item().await.unwrap();
    }

    // Query with filter
    let filter_expression = "ux = :ux_value".to_string();
    let mut filter_values = HashMap::new();
    filter_values.insert(":ux_value".to_string(), "match_this".to_string());

    let filtered = TestGSIObject::query_gsi_items_with_filter(
        user_id.to_string(),
        None,
        None,
        Some(10),
        true,
        filter_expression,
        filter_values,
    )
    .await
    .unwrap();

    assert_eq!(filtered.items.len(), 2, "Should find 2 matching items");
    for item in &filtered.items {
        assert_eq!(item.ux, "match_this");
    }
}

/// Test GSI query with both partition and sort key
#[tokio::test]
#[serial]
async fn test_gsi_with_sort_key() {
    setup_gsi_table().await;

    let user_id = "gsi_sort_user";

    // Create items with same user_id but different ages
    for i in 0..5 {
        let obj = TestGSIObject {
            game: format!("game{}", i),
            age: format!("age{:02}", i), // Zero-padded
            user_id: user_id.to_string(),
            created_at: format!("2024-01-0{}T10:00:00Z", i + 1),
            ux: "sort_test".to_string(),
        };
        obj.add_item().await.unwrap();
    }

    // Query with specific sort key
    let results = TestGSIObject::query_gsi_items(
        user_id.to_string(),
        Some("age02".to_string()),
        Some(10),
        None,
    )
    .await
    .unwrap();

    assert_eq!(results.items.len(), 1, "Should find exactly 1 item");
    assert_eq!(results.items[0].age, "age02");
}
