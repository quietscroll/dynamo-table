/// DynamoDB Query Operations Tests
///
/// Tests query operations including pagination, filtering, and sorting.
use futures_util::StreamExt;
use serial_test::serial;
use std::collections::HashMap;

mod helpers;
use dynamo_table::table::query_items_stream;
use helpers::*;

/// Test pagination with last evaluated key
#[tokio::test]
#[serial]
async fn test_query_with_pagination() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "pagination_test_1".to_string();
    let count = 50;

    // Add items
    for i in 0..(count + 10) {
        let obj = TestObject {
            game: partition_key.clone(),
            age: format!("{:03}", i), // Zero-padded for proper sorting
            ux: "test".into(),
            number2: i,
        };
        obj.add_item().await.unwrap();
    }

    // Get first page
    let first_page = TestObject::query_items(&partition_key, None, Some(count as u16), None)
        .await
        .unwrap();

    assert_eq!(first_page.items.len(), count);

    // Get second page using cursor
    if let Some(cursor) = first_page.start_cursor() {
        let second_page = TestObject::query_items(
            &partition_key,
            None,
            Some(count as u16),
            cursor.exclusive_start_key(),
        )
        .await
        .unwrap();

        assert_eq!(second_page.items.len(), 10);
    } else {
        panic!("Should have pagination cursor");
    }
}

/// Test query with filter expression
#[tokio::test]
#[serial]
async fn test_query_with_filter() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "filter_test_1".to_string();

    // Add items with different number2 values
    for i in 0..10 {
        let obj = TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: format!("value{}", i),
            number2: i,
        };
        obj.add_item().await.unwrap();
    }

    // Query with filter for number2 > 5
    let expression = "number2 > :threshold".to_string();
    let mut filter_values: HashMap<String, u8> = HashMap::new();
    filter_values.insert(":threshold".into(), 5);

    let results = TestObject::query_items_with_filter(
        &partition_key,
        None,
        Some(100),
        None,
        expression,
        filter_values,
    )
    .await
    .unwrap();

    assert_eq!(
        results.items.len(),
        4,
        "Should have 4 items with number2 > 5"
    );
    for item in &results.items {
        assert!(item.number2 > 5);
    }
}

/// Test reverse query (descending sort order)
#[tokio::test]
#[serial]
async fn test_reverse_query() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "reverse_test_1".to_string();

    // Add items
    for i in 0..5 {
        let obj = TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: "test".into(),
            number2: i,
        };
        obj.add_item().await.unwrap();
    }

    // Query in reverse order
    let results = TestObject::reverse_query_items(&partition_key, None, Some(10), None)
        .await
        .unwrap();

    assert_eq!(results.items.len(), 5);

    // Verify descending order
    assert_eq!(results.items[0].age, "item4");
    assert_eq!(results.items[4].age, "item0");
}

/// Test query items stream for large result sets
#[tokio::test]
#[serial]
async fn test_query_stream() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "stream_test_1".to_string();
    let count = 50;

    // Add items
    for i in 0..count {
        let obj = TestObject {
            game: partition_key.clone(),
            age: format!("{:03}", i),
            ux: "stream".into(),
            number2: i,
        };
        obj.add_item().await.unwrap();
    }

    // Stream all items
    let items = query_items_stream::<TestObject>(&partition_key, None, None, Some(10), None, true)
        .await
        .filter_map(|item| async { item.ok() })
        .collect::<Vec<TestObject>>()
        .await;

    assert_eq!(items.len(), count, "Should stream all items");
}

/// Test querying between sort key values
#[tokio::test]
#[serial]
async fn test_query_between() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "between_test_1".to_string();

    // Add items with date-like sort keys
    for i in 0..10 {
        let obj = TestObject {
            game: partition_key.clone(),
            age: format!("2024-01-{:02}", i + 1),
            ux: "date_test".into(),
            number2: i,
        };
        obj.add_item().await.unwrap();
    }

    // Query between two dates
    let results = TestObject::query_items_between(
        &partition_key,
        None,
        Some(100),
        true,
        "2024-01-03".to_string(),
        "2024-01-07".to_string(),
    )
    .await
    .unwrap();

    assert_eq!(results.items.len(), 5, "Should get items from 03 to 07");
}

/// Test querying with begins_with condition
#[tokio::test]
#[serial]
async fn test_query_begins_with() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "prefix_test_1".to_string();

    // Add items with different prefixes
    for prefix in &["user_", "admin_", "guest_"] {
        for i in 0..3 {
            let obj = TestObject {
                game: partition_key.clone(),
                age: format!("{}{}", prefix, i),
                ux: "prefix_test".into(),
                number2: i,
            };
            obj.add_item().await.unwrap();
        }
    }

    // Query for items beginning with "user_"
    let results = TestObject::query_items_begins_with(
        &partition_key,
        None,
        Some(100),
        true,
        "user_".to_string(),
    )
    .await
    .unwrap();

    assert_eq!(results.items.len(), 3);
    for item in &results.items {
        assert!(item.age.starts_with("user_"));
    }
}

/// Test query single item
#[tokio::test]
#[serial]
async fn test_query_single_item() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "single_query_test_1".to_string();

    // Add item
    let obj = TestObject {
        game: partition_key.clone(),
        age: "unique".into(),
        ux: "single".into(),
        number2: 42,
    };
    obj.add_item().await.unwrap();

    // Query single item
    let result = TestObject::query_item(&partition_key).await.unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap(), obj);
}
