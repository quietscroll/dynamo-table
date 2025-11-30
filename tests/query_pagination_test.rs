use rusty_ulid::Ulid;
use serial_test::serial;

mod support;
use dynamo_table::table::{query_items_begins_with, query_items_between, DynamoTable, SortKey};
use support::*;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct Item {
    user_id: String,
    item_age: String,
}

impl DynamoTable for Item {
    type PK = String;
    type SK = String;
    const TABLE: &'static str = "tests_items";
    const PARTITION_KEY: &'static str = "user_id";
    const SORT_KEY: Option<&'static str> = Some("item_age");

    fn partition_key(&self) -> String {
        self.user_id.to_string()
    }

    fn sort_key(&self) -> SortKey<String> {
        Some(self.item_age.clone())
    }
}

#[tokio::test]
#[serial]
async fn test_query_pagination_partition_and_sort_keys() {
    let _ = setup::table::<Item>().await;
    let user_id = Ulid::generate();

    let items: Vec<Item> = (0..10)
        .map(|index| Item {
            user_id: user_id.to_string(),
            item_age: format!("age{}", index),
        })
        .collect();

    for item in items.iter() {
        let put_item_output = item.add_item().await;
        assert!(put_item_output.is_ok());
    }

    let got = Item::query_items(&user_id.to_string(), None, Some(2), None)
        .await
        .unwrap();

    assert_eq!(got.items.len(), 2);
    assert_eq!(got.items[0], items[0]);
    assert_eq!(got.items[1], items[1]);

    // Exclusive start from 2 item
    let sk1 = items[1].item_age.clone();
    let got = Item::query_items(&user_id.to_string(), None, Some(2), Some(&sk1))
        .await
        .unwrap();

    assert_eq!(got.items.len(), 2);
    assert_eq!(got.items[0], items[2]);
    assert_eq!(got.items[1], items[3]);

    // Exclusive start from 2 item with non existent start key
    let bogus = "no-such-key".to_string();
    let got = Item::query_items(&user_id.to_string(), None, Some(2), Some(&bogus))
        .await
        .unwrap();

    assert_eq!(got.items.len(), 0);
}

#[tokio::test]
#[serial]
async fn test_query_pagination_reverse_partition_and_sort_keys() {
    let _ = setup::table::<Item>().await;
    let user_id = Ulid::generate();

    let items: Vec<Item> = (0..10)
        .map(|index| Item {
            user_id: user_id.to_string(),
            item_age: format!("age{}", index),
        })
        .collect();

    for item in items.iter() {
        let put_item_output = item.add_item().await;
        assert!(put_item_output.is_ok());
    }

    let got = Item::reverse_query_items(&user_id.to_string(), None, Some(2), None)
        .await
        .unwrap();

    assert_eq!(got.items.len(), 2);
    assert_eq!(got.items[0], items[9]);
    assert_eq!(got.items[1], items[8]);

    // Exclusive start from 2 item
    let sk2 = items[9].item_age.clone();
    let got = Item::reverse_query_items(&user_id.to_string(), None, Some(2), Some(&sk2))
        .await
        .unwrap();

    assert_eq!(got.items.len(), 2);
    assert_eq!(got.items[0], items[8]);
    assert_eq!(got.items[1], items[7]);

    // Exclusive start from 2 item with non existent start key
    let bogus2 = Ulid::generate().to_string();
    let got = Item::reverse_query_items(&user_id.to_string(), None, Some(2), Some(&bogus2))
        .await
        .unwrap();

    assert_eq!(got.items.len(), 0, "Got: {:?}", got);
}

#[tokio::test]
#[serial]
async fn test_query_items_between() {
    let _ = setup::table::<Item>().await;
    let user_id = Ulid::generate();

    let items: Vec<Item> = (0..10)
        .map(|index| Item {
            user_id: user_id.to_string(),
            item_age: format!("age{:02}", index),
        })
        .collect();

    for item in &items {
        let _ = item.add_item().await.unwrap();
    }

    let between = query_items_between::<Item>(
        &user_id.to_string(),
        None,
        Some(10),
        None,
        true,
        items[3].item_age.clone(),
        items[6].item_age.clone(),
    )
    .await
    .unwrap();

    assert_eq!(between.items.len(), 4);
    assert_eq!(between.items[0], items[3]);
    assert_eq!(between.items[1], items[4]);
    assert_eq!(between.items[2], items[5]);
    assert_eq!(between.items[3], items[6]);
}

#[tokio::test]
#[serial]
async fn test_query_items_begins_with() {
    let _ = setup::table::<Item>().await;
    let user_id = Ulid::generate();

    let items: Vec<Item> = (0..6)
        .map(|index| Item {
            user_id: user_id.to_string(),
            item_age: format!("prefix{}suffix", index),
        })
        .collect();

    for item in &items {
        let _ = item.add_item().await.unwrap();
    }

    let got = query_items_begins_with::<Item>(
        &user_id.to_string(),
        None,
        Some(10),
        None,
        true,
        "prefix1".to_string(),
    )
    .await
    .unwrap();

    assert_eq!(got.items.len(), 1);
    assert_eq!(got.items[0], items[1]);
}
