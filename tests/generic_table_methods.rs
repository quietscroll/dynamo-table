mod support;
use dynamo_table::methods::DynamoTableMethods as DynamobTableMethods;
use dynamo_table::table::{DynamoTable, SortKey};
use support::*;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct ValueObject {
    prim: String,
    second: String,
    ux: String,
    number2: usize,
}

impl DynamoTable for ValueObject {
    type PK = String;
    type SK = String;
    const TABLE: &'static str = "tests_generic_value_objects";
    const PARTITION_KEY: &'static str = "prim";
    const SORT_KEY: Option<&'static str> = Some("second");

    fn partition_key(&self) -> String {
        self.prim.to_string()
    }

    fn sort_key(&self) -> SortKey<String> {
        Some(self.second.clone())
    }
}

#[tokio::test]
async fn test_generic_table_methods_left_diff() {
    let objects = (0..40)
        .map(|i| {
            let primary_key = format!("batchergetter{i}");
            let sort_key = i.to_string();

            ValueObject {
                prim: primary_key,
                second: sort_key,
                ux: "bla".into(),
                number2: 0,
            }
        })
        .collect::<Vec<_>>();

    let composite_keys = objects[0..10]
        .iter()
        .map(|obj| obj.composite_key())
        .collect::<Vec<_>>();

    let diffrence = ValueObject::left_diff(objects.clone(), composite_keys.clone());
    assert_eq!(diffrence, objects[10..].to_vec());

    let diffrence = ValueObject::left_diff(objects.clone(), vec![]);
    assert_eq!(diffrence, objects.to_vec());

    let diffrence = ValueObject::left_diff(
        objects.clone(),
        objects
            .iter()
            .map(|obj| obj.composite_key())
            .collect::<Vec<_>>(),
    );
    assert_eq!(diffrence, vec![]);
}

#[tokio::test]
async fn test_generic_table_methods_right_diff() {
    let objects = (0..40)
        .map(|i| {
            let primary_key = format!("batchergetter{i}");
            let sort_key = i.to_string();

            ValueObject {
                prim: primary_key,
                second: sort_key,
                ux: "bla".into(),
                number2: 0,
            }
        })
        .collect::<Vec<_>>();

    let composite_keys = objects[0..10]
        .iter()
        .map(|obj| obj.composite_key())
        .collect::<Vec<_>>();

    let diffrence = ValueObject::right_diff(objects.clone(), composite_keys.clone());
    assert_eq!(diffrence, vec![]);

    let diffrence = ValueObject::right_diff(objects[10..20].to_vec(), composite_keys.clone());
    assert_eq!(diffrence, composite_keys.clone());

    let diffrence = ValueObject::right_diff(objects[5..20].to_vec(), composite_keys.clone());
    assert_eq!(diffrence, composite_keys[..5].to_vec());
}
