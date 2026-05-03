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

/// Test GSI query pagination — second page uses exclusive_start_key.
///
/// Before ed62d2a, DynamoDB rejected the paginated request because the base
/// table PK was missing from ExclusiveStartKey. This test exercises the
/// `QueryBuilder::for_gsi` path fixed in helpers.rs.
#[tokio::test]
#[serial]
async fn test_gsi_pagination_second_page() {
    setup_gsi_table().await;

    let user_id = "gsi_page_user";

    // Insert 5 items in sort-key order so pages are deterministic.
    let mut ages: Vec<String> = (0..5).map(|i| format!("pg_age{:02}", i)).collect();
    ages.sort();

    for (i, age) in ages.iter().enumerate() {
        let obj = TestGSIObject {
            game: format!("pg_game{}", i),
            age: age.clone(),
            user_id: user_id.to_string(),
            created_at: format!("2024-01-{:02}T00:00:00Z", i + 1),
            ux: "pg_test".to_string(),
        };
        obj.add_item().await.unwrap();
    }

    // Page 1: limit 2
    let page1 = TestGSIObject::query_gsi_items(user_id.to_string(), None, Some(2), None)
        .await
        .unwrap();

    assert_eq!(page1.items.len(), 2, "page 1 should have 2 items");
    assert!(
        page1.last_evaluated_key.is_some(),
        "last_evaluated_key must be set for page 1"
    );

    // Extract the sort key of the last item to use as exclusive_start_key.
    let cursor = page1
        .start_cursor()
        .expect("cursor must be present in last_evaluated_key");

    // Page 2: must not fail and must start after the first page.
    let page2 = TestGSIObject::query_gsi_items(user_id.to_string(), None, Some(2), Some(cursor))
        .await
        .unwrap();

    assert_eq!(page2.items.len(), 2, "page 2 should have 2 items");

    // Items on page 2 come after those on page 1.
    assert!(
        page2.items[0].age > page1.items[1].age,
        "page 2 items must sort after page 1 items"
    );
}

/// Test GSI reverse-query pagination — second page uses exclusive_start_key.
///
/// Exercises the same `QueryBuilder::for_gsi` path with `scan_index_forward = false`.
#[tokio::test]
#[serial]
async fn test_gsi_pagination_reverse_second_page() {
    setup_gsi_table().await;

    let user_id = "gsi_rev_page_user";

    let mut ages: Vec<String> = (0..5).map(|i| format!("rp_age{:02}", i)).collect();
    ages.sort();

    for (i, age) in ages.iter().enumerate() {
        let obj = TestGSIObject {
            game: format!("rp_game{}", i),
            age: age.clone(),
            user_id: user_id.to_string(),
            created_at: format!("2024-02-{:02}T00:00:00Z", i + 1),
            ux: "rp_test".to_string(),
        };
        obj.add_item().await.unwrap();
    }

    // Page 1 (descending): limit 2 → should return the two largest ages.
    let page1 = TestGSIObject::reverse_query_gsi_items(user_id.to_string(), None, Some(2), None)
        .await
        .unwrap();

    assert_eq!(page1.items.len(), 2, "reverse page 1 should have 2 items");
    assert!(
        page1.items[0].age > page1.items[1].age,
        "should be descending"
    );
    assert!(
        page1.last_evaluated_key.is_some(),
        "last_evaluated_key must be set"
    );

    let cursor = page1
        .start_cursor()
        .expect("cursor must be present in last_evaluated_key");

    // Page 2 must not fail and items must be smaller than the last item on page 1.
    let page2 =
        TestGSIObject::reverse_query_gsi_items(user_id.to_string(), None, Some(2), Some(cursor))
            .await
            .unwrap();

    assert_eq!(page2.items.len(), 2, "reverse page 2 should have 2 items");
    assert!(
        page2.items[0].age < page1.items[1].age,
        "page 2 items must sort before the last item on page 1"
    );
}

/// Test GSI query_with_filter pagination — second page uses exclusive_start_key.
///
/// Exercises the direct fix in gsi.rs that always includes the base-table PK
/// in ExclusiveStartKey for the filter code path.
#[tokio::test]
#[serial]
async fn test_gsi_pagination_with_filter_second_page() {
    setup_gsi_table().await;

    let user_id = "gsi_filter_page_user";

    // Insert 6 items; all have ux = "keep" so none are filtered out.
    let mut ages: Vec<String> = (0..6).map(|i| format!("fp_age{:02}", i)).collect();
    ages.sort();

    for (i, age) in ages.iter().enumerate() {
        let obj = TestGSIObject {
            game: format!("fp_game{}", i),
            age: age.clone(),
            user_id: user_id.to_string(),
            created_at: format!("2024-03-{:02}T00:00:00Z", i + 1),
            ux: "keep".to_string(),
        };
        obj.add_item().await.unwrap();
    }

    let filter_expr = "ux = :ux_val".to_string();
    let mut filter_vals = HashMap::new();
    filter_vals.insert(":ux_val".to_string(), "keep".to_string());

    // Page 1
    let page1 = TestGSIObject::query_gsi_items_with_filter(
        user_id.to_string(),
        None,
        None,
        Some(2),
        true,
        filter_expr.clone(),
        filter_vals.clone(),
    )
    .await
    .unwrap();

    assert_eq!(page1.items.len(), 2, "filter page 1 should have 2 items");
    assert!(
        page1.last_evaluated_key.is_some(),
        "last_evaluated_key must be set for filter page 1"
    );

    let cursor = page1
        .start_cursor()
        .expect("cursor must be present in last_evaluated_key");

    // Page 2 must not fail (this is what broke before the fix).
    let page2 = TestGSIObject::query_gsi_items_with_filter(
        user_id.to_string(),
        None,
        Some(cursor),
        Some(2),
        true,
        filter_expr,
        filter_vals,
    )
    .await
    .unwrap();

    assert_eq!(page2.items.len(), 2, "filter page 2 should have 2 items");
    assert!(
        page2.items[0].age > page1.items[1].age,
        "filter page 2 items must sort after filter page 1 items"
    );
}

/// Test GSI filtered pagination preserves DynamoDB's real last evaluated key
/// even when a page returns zero items after filtering.
#[tokio::test]
#[serial]
async fn test_gsi_pagination_with_filter_empty_page_keeps_cursor() {
    setup_gsi_table().await;

    let user_id = "gsi_filter_empty_page_user";

    let objects = vec![
        TestGSIObject {
            game: "empty_page_game0".to_string(),
            age: "ep_age00".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-04-01T00:00:00Z".to_string(),
            ux: "drop".to_string(),
        },
        TestGSIObject {
            game: "empty_page_game1".to_string(),
            age: "ep_age01".to_string(),
            user_id: user_id.to_string(),
            created_at: "2024-04-02T00:00:00Z".to_string(),
            ux: "keep".to_string(),
        },
    ];

    for obj in &objects {
        obj.add_item().await.unwrap();
    }

    let filter_expr = "ux = :ux_val".to_string();
    let mut filter_vals = HashMap::new();
    filter_vals.insert(":ux_val".to_string(), "keep".to_string());

    let page1 = TestGSIObject::query_gsi_items_with_filter(
        user_id.to_string(),
        None,
        None,
        Some(1),
        true,
        filter_expr.clone(),
        filter_vals.clone(),
    )
    .await
    .unwrap();

    assert!(
        page1.items.is_empty(),
        "page 1 should be empty because the first evaluated item is filtered out"
    );
    assert!(
        page1.last_evaluated_key.is_some(),
        "page 1 must still expose the real DynamoDB cursor"
    );

    let cursor = page1
        .start_cursor()
        .expect("cursor must be present in last_evaluated_key");

    let page2 = TestGSIObject::query_gsi_items_with_filter(
        user_id.to_string(),
        None,
        Some(cursor),
        Some(1),
        true,
        filter_expr,
        filter_vals,
    )
    .await
    .unwrap();

    assert_eq!(page2.items.len(), 1, "page 2 should return the kept item");
    assert_eq!(page2.items[0].age, "ep_age01");
}
