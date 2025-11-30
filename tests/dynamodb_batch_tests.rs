/// DynamoDB Batch Operations Tests
///
/// Tests batch read and write operations including handling of unprocessed items.
use serial_test::serial;
use std::collections::BTreeSet;

mod helpers;
use dynamo_table::table::{CompositeKey, SortKey, batch_get, batch_write};
use helpers::*;

/// Test batch write with multiple items
#[tokio::test]
#[serial]
async fn test_batch_write_basic() {
    let _ = setup::table::<TestObject>().await;

    let obj1 = TestObject {
        game: "batch_write_1".into(),
        age: "1".into(),
        ux: "item1".into(),
        number2: 1,
    };

    let obj2 = TestObject {
        game: "batch_write_2".into(),
        age: "1".into(),
        ux: "item2".into(),
        number2: 2,
    };

    // Batch write
    let result = batch_write::<TestObject>(vec![obj1.clone(), obj2.clone()], vec![])
        .await
        .unwrap();

    assert!(
        result.failed_puts.is_empty(),
        "All items should be processed"
    );

    // Verify items exist
    let got1 = TestObject::get_item(&obj1.game, Some(&obj1.age))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(obj1, got1);

    let got2 = TestObject::get_item(&obj2.game, Some(&obj2.age))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(obj2, got2);
}

/// Test batch write with delete operations
#[tokio::test]
#[serial]
async fn test_batch_write_with_delete() {
    let _ = setup::table::<TestObject>().await;

    let obj = TestObject {
        game: "batch_delete_1".into(),
        age: "1".into(),
        ux: "to_delete".into(),
        number2: 0,
    };

    // Create item
    obj.add_item().await.unwrap();

    // Verify exists
    assert!(
        TestObject::get_item(&obj.game, Some(&obj.age))
            .await
            .unwrap()
            .is_some()
    );

    // Batch delete
    batch_write::<TestObject>(vec![], vec![obj.clone()])
        .await
        .unwrap();

    // Verify deleted
    assert!(
        TestObject::get_item(&obj.game, Some(&obj.age))
            .await
            .unwrap()
            .is_none(),
        "Item should be deleted"
    );
}

/// Test batch write with more than 25 items (DynamoDB limit)
#[tokio::test]
#[serial]
async fn test_batch_write_chunking() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "batch_chunk_1".to_string();

    // Create 30 items (more than the 25-item batch limit)
    let objects: Vec<TestObject> = (0..30)
        .map(|i| TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: "chunked".into(),
            number2: i,
        })
        .collect();

    let result = batch_write(objects, vec![]).await.unwrap();

    // Should process in 2 batches
    assert_eq!(result.consumed_capacity.len(), 2, "Should have 2 batches");
    assert!(
        result.failed_puts.is_empty(),
        "All items should be processed"
    );

    // Verify count
    let count = TestObject::count_items(&partition_key).await.unwrap();
    assert_eq!(count, 30, "Should have all 30 items");
}

/// Test batch get operation
#[tokio::test]
#[serial]
async fn test_batch_get_basic() {
    let _ = setup::table::<TestObject>().await;

    let mut keys_to_get: BTreeSet<(String, SortKey<String>)> = BTreeSet::new();

    // Create items
    let objects: Vec<TestObject> = (0..5)
        .map(|i| {
            let game = format!("batch_get_{}", i);
            let age = format!("item{}", i);

            keys_to_get.insert((game.clone(), Some(age.clone())));

            TestObject {
                game,
                age,
                ux: "batch_get".into(),
                number2: i,
            }
        })
        .collect();

    batch_write::<TestObject>(objects, vec![]).await.unwrap();

    // Batch get subset of items
    let get_keys: Vec<CompositeKey<String, String>> = keys_to_get.iter().take(3).cloned().collect();

    let result = batch_get::<TestObject>(get_keys).await.unwrap();

    assert_eq!(result.items.len(), 3, "Should retrieve 3 items");
    assert!(
        result.failed_keys.is_empty(),
        "All keys should be processed"
    );
}

/// Test batch get with non-existent keys
#[tokio::test]
#[serial]
async fn test_batch_get_missing_items() {
    let _ = setup::table::<TestObject>().await;

    // Try to get items that don't exist
    let keys: Vec<CompositeKey<String, String>> = vec![
        ("nonexistent1".to_string(), Some("key1".to_string())),
        ("nonexistent2".to_string(), Some("key2".to_string())),
    ];

    let result = batch_get::<TestObject>(keys).await.unwrap();

    assert_eq!(
        result.items.len(),
        0,
        "Should not retrieve any items for missing keys"
    );
}

/// Test batch update using batch_update method
#[tokio::test]
#[serial]
async fn test_batch_update_method() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "batch_update_1".to_string();

    // Create initial items
    let initial: Vec<TestObject> = (0..3)
        .map(|i| TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: "initial".into(),
            number2: i,
        })
        .collect();

    batch_write(initial, vec![]).await.unwrap();

    // Update items
    let updated: Vec<TestObject> = (0..3)
        .map(|i| TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: "updated".into(),
            number2: i + 100,
        })
        .collect();

    TestObject::batch_upsert(updated).await.unwrap();

    // Verify updates
    for i in 0..3 {
        let item = TestObject::get_item(&partition_key, Some(&format!("item{}", i)))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(item.ux, "updated");
        assert_eq!(item.number2, i + 100);
    }
}

/// Test batch delete using batch_delete method
#[tokio::test]
#[serial]
async fn test_batch_delete_method() {
    let _ = setup::table::<TestObject>().await;

    let partition_key = "batch_delete_method_1".to_string();

    // Create items
    let items: Vec<TestObject> = (0..3)
        .map(|i| TestObject {
            game: partition_key.clone(),
            age: format!("item{}", i),
            ux: "to_delete".into(),
            number2: i,
        })
        .collect();

    batch_write(items.clone(), vec![]).await.unwrap();

    // Verify items exist
    let count_before = TestObject::count_items(&partition_key).await.unwrap();
    assert_eq!(count_before, 3);

    // Batch delete
    TestObject::batch_delete(items).await.unwrap();

    // Verify items deleted
    let count_after = TestObject::count_items(&partition_key).await.unwrap();
    assert_eq!(count_after, 0, "All items should be deleted");
}
