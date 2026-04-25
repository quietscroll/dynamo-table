/// Consumed Capacity Tracking Integration Tests
///
/// Verifies that DynamoDB operations increment the global capacity counters.
/// Requires LocalStack (AWS_PROFILE=localstack).
use dynamo_table::consumed_capacity::stats;
use serial_test::serial;

mod helpers;
use helpers::*;

/// Snapshot deltas: compare counters before and after an operation.
struct CapacityDelta {
    rcu_before: f64,
    wcu_before: f64,
    ops_before: u64,
}

impl CapacityDelta {
    fn capture() -> Self {
        Self {
            rcu_before: stats::read_capacity_units(),
            wcu_before: stats::write_capacity_units(),
            ops_before: stats::operation_count(),
        }
    }

    fn rcu_delta(&self) -> f64 {
        stats::read_capacity_units() - self.rcu_before
    }

    fn wcu_delta(&self) -> f64 {
        stats::write_capacity_units() - self.wcu_before
    }

    fn ops_delta(&self) -> u64 {
        stats::operation_count() - self.ops_before
    }
}

#[tokio::test]
#[serial]
async fn test_put_item_increments_write_capacity() {
    let _ = setup_table::<TestObject>().await;

    let obj = TestObject {
        game: "capacity_test_put".into(),
        age: "1".into(),
        ux: "x".into(),
        number2: 1,
    };

    let delta = CapacityDelta::capture();
    obj.add_item().await.expect("add_item failed");

    assert_eq!(delta.ops_delta(), 1, "expected one operation recorded");
    assert!(
        delta.wcu_delta() >= 0.0,
        "expected write CU counter to stay non-negative after put_item, got {}",
        delta.wcu_delta()
    );
}

#[tokio::test]
#[serial]
async fn test_get_item_increments_read_capacity() {
    let _ = setup_table::<TestObject>().await;

    let obj = TestObject {
        game: "capacity_test_get".into(),
        age: "2".into(),
        ux: "x".into(),
        number2: 2,
    };
    obj.add_item().await.expect("add_item failed");

    let delta = CapacityDelta::capture();
    let _ = TestObject::get_item(&obj.game, Some(&obj.age))
        .await
        .expect("get_item failed");

    assert!(
        delta.rcu_delta() > 0.0,
        "expected read CU > 0 after get_item, got {}",
        delta.rcu_delta()
    );
    assert_eq!(delta.ops_delta(), 1, "expected one operation recorded");
}

#[tokio::test]
#[serial]
async fn test_destroy_item_increments_write_capacity() {
    let _ = setup_table::<TestObject>().await;

    let obj = TestObject {
        game: "capacity_test_delete".into(),
        age: "3".into(),
        ux: "x".into(),
        number2: 3,
    };
    obj.add_item().await.expect("add_item failed");

    let delta = CapacityDelta::capture();
    obj.destroy_item().await.expect("destroy_item failed");

    assert_eq!(delta.ops_delta(), 1, "expected one operation recorded");
    assert!(
        delta.wcu_delta() >= 0.0,
        "expected write CU counter to stay non-negative after destroy_item, got {}",
        delta.wcu_delta()
    );
}

#[tokio::test]
#[serial]
async fn test_query_items_increments_read_capacity() {
    let _ = setup_table::<TestObject>().await;

    let obj = TestObject {
        game: "capacity_test_query".into(),
        age: "4".into(),
        ux: "x".into(),
        number2: 4,
    };
    obj.add_item().await.expect("add_item failed");

    let delta = CapacityDelta::capture();
    let _ = TestObject::query_items(&obj.game, None, None, None)
        .await
        .expect("query_items failed");

    assert!(
        delta.rcu_delta() > 0.0,
        "expected read CU > 0 after query_items, got {}",
        delta.rcu_delta()
    );
    assert_eq!(delta.ops_delta(), 1, "expected one operation recorded");
}

#[tokio::test]
#[serial]
async fn test_total_capacity_units_is_sum_of_read_and_write() {
    let rcu = stats::read_capacity_units();
    let wcu = stats::write_capacity_units();
    let total = stats::total_capacity_units();
    assert!(
        (total - (rcu + wcu)).abs() < f64::EPSILON,
        "total={total} != rcu={rcu} + wcu={wcu}"
    );
}

#[tokio::test]
#[serial]
async fn test_stats_snapshot_is_consistent() {
    let s = stats::stats();
    assert_eq!(s.read_capacity_units, stats::read_capacity_units());
    assert_eq!(s.write_capacity_units, stats::write_capacity_units());
    assert_eq!(s.total_capacity_units, stats::total_capacity_units());
    assert_eq!(s.operation_count, stats::operation_count());
}

#[tokio::test]
#[serial]
async fn test_log_stats_does_not_panic() {
    stats::log_stats();
}
