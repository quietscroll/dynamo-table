/// DynamoDB CRUD Operations Tests
///
/// Tests basic create, read, update, and delete operations on DynamoDB tables.
use serial_test::serial;
use std::collections::HashMap;

mod helpers;
use helpers::*;

/// Test basic item creation and retrieval
#[tokio::test]
#[serial]
async fn test_add_and_get_item() {
    let _ = setup::table::<TestObject>().await;

    let obj = TestObject {
        game: "test_game_1".into(),
        age: "1".into(),
        ux: "test_ux".into(),
        number2: 42,
    };

    // Add item
    let put_result = obj.add_item().await;
    assert!(put_result.is_ok(), "Failed to add item: {:?}", put_result);

    // Get item back
    let got = TestObject::get_item(&obj.game, Some(&obj.age))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(obj, got, "Retrieved item should match added item");
}

/// Test querying items by partition key
#[tokio::test]
#[serial]
async fn test_query_items_basic() {
    let _ = setup::table::<TestObject>().await;

    let obj = TestObject {
        game: "query_test_1".into(),
        age: "1".into(),
        ux: "query_ux".into(),
        number2: 100,
    };

    obj.add_item().await.unwrap();

    // Query by partition key
    let results = TestObject::query_items(&obj.game, None, Some(10), None)
        .await
        .unwrap();

    assert_eq!(results.items.len(), 1);
    assert_eq!(results.items[0], obj);
}

/// Test updating item fields
#[tokio::test]
#[serial]
async fn test_update_item_fields() {
    let _ = setup::table::<TestObject>().await;

    let obj = TestObject {
        game: "update_test_1".into(),
        age: "1".into(),
        ux: "old_value".into(),
        number2: 5,
    };

    obj.add_item().await.unwrap();

    // Update fields using a serializable struct
    #[derive(serde::Serialize)]
    struct Updates {
        ux: String,
        number2: usize,
    }

    let updates = Updates {
        ux: "new_value".to_string(),
        number2: 10,
    };

    obj.update_item(updates).await.unwrap();

    // Verify update
    let got = TestObject::get_item(&obj.game, Some(&obj.age))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(got.ux, "new_value");
    assert_eq!(got.number2, 10);
}

/// Test conditional update operations
#[tokio::test]
#[serial]
async fn test_conditional_update_success() {
    let _ = setup::table::<TestObject>().await;

    let obj = TestObject {
        game: "cond_update_1".into(),
        age: "1".into(),
        ux: "old".into(),
        number2: 10,
    };

    obj.add_item().await.unwrap();

    // Update with correct condition
    #[derive(serde::Serialize)]
    struct Updates {
        ux: String,
    }

    let updates = Updates {
        ux: "new".to_string(),
    };
    let condition = Some("number2 = :expected".to_string());

    let mut condition_values: HashMap<String, serde_json::Value> = HashMap::new();
    condition_values.insert(":expected".into(), 10.into());

    let result = obj
        .update_item_with_condition(updates, condition, Some(condition_values))
        .await;

    assert!(result.is_ok(), "Conditional update should succeed");

    // Verify update
    let got = TestObject::get_item(&obj.game, Some(&obj.age))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.ux, "new");
}

/// Test conditional update failure
#[tokio::test]
#[serial]
async fn test_conditional_update_failure() {
    let _ = setup::table::<TestCounters>().await;

    let obj = TestCounters {
        imo: "cond_fail_1".into(),
        det: "det1".into(),
        p1: 1,
        p2: 2,
    };

    obj.add_item().await.unwrap();

    // Try to update with wrong condition
    let mut updates = HashMap::new();
    updates.insert("p1".to_string(), 100usize);

    let condition = Some("p2 = :expected".to_string());

    let mut condition_values: HashMap<String, serde_json::Value> = HashMap::new();
    condition_values.insert(":expected".into(), 999.into()); // Wrong value

    let result = obj
        .update_item_with_condition(updates, condition, Some(condition_values))
        .await;

    assert!(result.is_err(), "Conditional update should fail");
    let err = result.unwrap_err();
    assert!(
        err.is_conditional_check_failed(),
        "Error should be conditional check failed, got: {:?}",
        err
    );

    // Verify no update occurred (note: TestCounters has no sort key)
    let got = TestCounters::get_item(&obj.imo, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.p1, 1, "Value should not have changed");
}

/// Test deleting items
#[tokio::test]
#[serial]
async fn test_delete_item() {
    let _ = setup::table::<TestObject>().await;

    let obj = TestObject {
        game: "delete_test_1".into(),
        age: "1".into(),
        ux: "to_delete".into(),
        number2: 0,
    };

    obj.add_item().await.unwrap();

    // Verify exists
    let exists = TestObject::get_item(&obj.game, Some(&obj.age))
        .await
        .unwrap();
    assert!(exists.is_some());

    // Delete
    let pk = obj.game.clone();
    let sk = obj.age.clone();
    obj.destroy_item().await.unwrap();

    // Verify deleted
    let gone = TestObject::get_item(&pk, Some(&sk)).await.unwrap();
    assert!(gone.is_none(), "Item should be deleted");
}

/// Test counting items
#[tokio::test]
#[serial]
async fn test_count_items() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "count_test_1".to_string();

    // Add multiple items with same partition key
    for i in 0..5 {
        let obj = TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: "counter".into(),
            number2: i,
        };
        obj.add_item().await.unwrap();
    }

    // Count items
    let count = TestObject::count_items(&partition_key).await.unwrap();
    assert_eq!(count, 5, "Should have 5 items");
}

/// Test increment operations
#[tokio::test]
#[serial]
async fn test_increment_multiple_fields() {
    let _ = setup::table::<TestCounters>().await;

    let obj = TestCounters {
        imo: "increment_test_1".into(),
        det: "det1".into(),
        p1: 0,
        p2: 10,
    };

    obj.add_item().await.unwrap();

    // Increment multiple fields (note: TestCounters has no sort key)
    dynamo_table::table::increment_multiple::<TestCounters>(
        &obj.imo,
        None,
        &[("p1", 5), ("p2", 3)],
    )
    .await
    .unwrap();

    // Verify increments
    let got = TestCounters::get_item(&obj.imo, None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(got.p1, 5);
    assert_eq!(got.p2, 13);
}
