//! Global atomic counters for DynamoDB consumed capacity tracking.

use std::sync::atomic::{AtomicU64, Ordering};

use aws_sdk_dynamodb::types::ConsumedCapacity;

/// Stores read capacity units scaled by 100 to preserve two decimal places.
static READ_CAPACITY_HUNDREDTHS: AtomicU64 = AtomicU64::new(0);

/// Stores write capacity units scaled by 100 to preserve two decimal places.
static WRITE_CAPACITY_HUNDREDTHS: AtomicU64 = AtomicU64::new(0);

/// Total number of DynamoDB operations that contributed to the counters.
static OPERATION_COUNT: AtomicU64 = AtomicU64::new(0);

fn add_hundredths(units: f64, counter: &AtomicU64) {
    let _ = counter.fetch_add((units * 100.0) as u64, Ordering::Relaxed);
}

/// Records read capacity units consumed by a DynamoDB read operation.
pub fn record_read(units: f64) {
    add_hundredths(units, &READ_CAPACITY_HUNDREDTHS);
    let _ = OPERATION_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Records write capacity units consumed by a DynamoDB write operation.
pub fn record_write(units: f64) {
    add_hundredths(units, &WRITE_CAPACITY_HUNDREDTHS);
    let _ = OPERATION_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Records consumed capacity from a [`ConsumedCapacity`] returned by DynamoDB.
///
/// Uses `read_capacity_units` and `write_capacity_units` when the SDK populates them.
/// Falls back to `capacity_units` (counted as reads) when the per-type breakdown is absent.
pub fn record(capacity: &ConsumedCapacity) {
    let mut recorded = false;

    if let Some(rcu) = capacity.read_capacity_units() {
        if rcu > 0.0 {
            add_hundredths(rcu, &READ_CAPACITY_HUNDREDTHS);
            let _ = OPERATION_COUNT.fetch_add(1, Ordering::Relaxed);
            recorded = true;
        }
    }

    if let Some(wcu) = capacity.write_capacity_units() {
        if wcu > 0.0 {
            add_hundredths(wcu, &WRITE_CAPACITY_HUNDREDTHS);
            if !recorded {
                let _ = OPERATION_COUNT.fetch_add(1, Ordering::Relaxed);
            }
            recorded = true;
        }
    }

    if !recorded {
        if let Some(total) = capacity.capacity_units() {
            if total > 0.0 {
                // Cannot distinguish read vs write; attribute to reads as a safe default.
                add_hundredths(total, &READ_CAPACITY_HUNDREDTHS);
                let _ = OPERATION_COUNT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Records consumed capacity from an optional [`ConsumedCapacity`]; a no-op when `None`.
pub fn record_from_option(capacity: Option<&ConsumedCapacity>) {
    if let Some(c) = capacity {
        record(c);
    }
}

/// Returns the total read capacity units consumed across all tracked operations.
pub fn read_capacity_units() -> f64 {
    READ_CAPACITY_HUNDREDTHS.load(Ordering::Relaxed) as f64 / 100.0
}

/// Returns the total write capacity units consumed across all tracked operations.
pub fn write_capacity_units() -> f64 {
    WRITE_CAPACITY_HUNDREDTHS.load(Ordering::Relaxed) as f64 / 100.0
}

/// Returns the combined read + write capacity units consumed.
pub fn total_capacity_units() -> f64 {
    read_capacity_units() + write_capacity_units()
}

/// Returns the total number of DynamoDB operations tracked.
pub fn operation_count() -> u64 {
    OPERATION_COUNT.load(Ordering::Relaxed)
}

/// Snapshot of all consumed capacity counters at a point in time.
#[derive(Debug, Clone)]
pub struct CapacityStats {
    /// Total read capacity units consumed.
    pub read_capacity_units: f64,
    /// Total write capacity units consumed.
    pub write_capacity_units: f64,
    /// Combined read + write capacity units consumed.
    pub total_capacity_units: f64,
    /// Number of DynamoDB operations that contributed to these stats.
    pub operation_count: u64,
}

/// Returns a point-in-time snapshot of all consumed capacity counters.
pub fn stats() -> CapacityStats {
    CapacityStats {
        read_capacity_units: read_capacity_units(),
        write_capacity_units: write_capacity_units(),
        total_capacity_units: total_capacity_units(),
        operation_count: operation_count(),
    }
}

/// Logs all consumed capacity counters using `tracing::info!`.
///
/// Emits a structured log event with `read_cu`, `write_cu`, `total_cu`,
/// and `operations` fields.
pub fn log_stats() {
    let s = stats();
    tracing::info!(
        read_cu = s.read_capacity_units,
        write_cu = s.write_capacity_units,
        total_cu = s.total_capacity_units,
        operations = s.operation_count,
        "DynamoDB consumed capacity"
    );
}

/// Logs all DynamoDB consumed capacity stats when dropped.
///
/// Hold this guard for the lifetime of your program to ensure stats are
/// emitted via the `tracing` subscriber on shutdown:
///
/// ```rust,no_run
/// let _stats_guard = dynamo_table::consumed_capacity::stats::CapacityStatsGuard::new();
/// // ... your program runs ...
/// // stats are logged when `_stats_guard` drops at the end of `main`
/// ```
#[derive(Debug)]
pub struct CapacityStatsGuard;

impl CapacityStatsGuard {
    /// Creates a new guard; stats will be logged to `tracing` when it drops.
    pub fn new() -> Self {
        Self
    }
}

impl Default for CapacityStatsGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CapacityStatsGuard {
    fn drop(&mut self) {
        log_stats();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::ConsumedCapacity;
    use serial_test::serial;

    fn make_capacity(rcu: Option<f64>, wcu: Option<f64>, total: Option<f64>) -> ConsumedCapacity {
        let mut b = ConsumedCapacity::builder();
        if let Some(v) = rcu {
            b = b.read_capacity_units(v);
        }
        if let Some(v) = wcu {
            b = b.write_capacity_units(v);
        }
        if let Some(v) = total {
            b = b.capacity_units(v);
        }
        b.build()
    }

    #[test]
    fn total_is_sum_of_read_and_write() {
        assert!(
            (total_capacity_units() - (read_capacity_units() + write_capacity_units())).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn stats_snapshot_matches_individual_counters() {
        let s = stats();
        assert_eq!(s.read_capacity_units, read_capacity_units());
        assert_eq!(s.write_capacity_units, write_capacity_units());
        assert_eq!(s.total_capacity_units, total_capacity_units());
        assert_eq!(s.operation_count, operation_count());
    }

    #[test]
    #[serial]
    fn record_from_option_none_is_noop() {
        let before_ops = operation_count();
        let before_rcu = read_capacity_units();
        let before_wcu = write_capacity_units();

        record_from_option(None);

        assert_eq!(operation_count(), before_ops);
        assert_eq!(read_capacity_units(), before_rcu);
        assert_eq!(write_capacity_units(), before_wcu);
    }

    #[test]
    #[serial]
    fn record_read_increments_read_and_operation_counters() {
        let before_rcu = read_capacity_units();
        let before_ops = operation_count();

        record_read(1.5);

        assert!((read_capacity_units() - (before_rcu + 1.5)).abs() < 0.01);
        assert_eq!(operation_count(), before_ops + 1);
    }

    #[test]
    #[serial]
    fn record_write_increments_write_and_operation_counters() {
        let before_wcu = write_capacity_units();
        let before_ops = operation_count();

        record_write(2.0);

        assert!((write_capacity_units() - (before_wcu + 2.0)).abs() < 0.01);
        assert_eq!(operation_count(), before_ops + 1);
    }

    #[test]
    #[serial]
    fn record_uses_read_capacity_units_field() {
        let before_rcu = read_capacity_units();
        let before_ops = operation_count();

        record(&make_capacity(Some(3.0), None, None));

        assert!((read_capacity_units() - (before_rcu + 3.0)).abs() < 0.01);
        assert_eq!(operation_count(), before_ops + 1);
    }

    #[test]
    #[serial]
    fn record_uses_write_capacity_units_field() {
        let before_wcu = write_capacity_units();
        let before_ops = operation_count();

        record(&make_capacity(None, Some(4.5), None));

        assert!((write_capacity_units() - (before_wcu + 4.5)).abs() < 0.01);
        assert_eq!(operation_count(), before_ops + 1);
    }

    #[test]
    #[serial]
    fn record_counts_single_operation_when_both_rcu_and_wcu_present() {
        let before_ops = operation_count();

        record(&make_capacity(Some(1.0), Some(1.0), None));

        assert_eq!(operation_count(), before_ops + 1);
    }

    #[test]
    #[serial]
    fn record_falls_back_to_capacity_units_when_no_breakdown() {
        let before_rcu = read_capacity_units();
        let before_ops = operation_count();

        record(&make_capacity(None, None, Some(5.0)));

        assert!((read_capacity_units() - (before_rcu + 5.0)).abs() < 0.01);
        assert_eq!(operation_count(), before_ops + 1);
    }

    #[test]
    #[serial]
    fn record_ignores_zero_capacity_units() {
        let before_ops = operation_count();

        record(&make_capacity(Some(0.0), Some(0.0), None));

        assert_eq!(operation_count(), before_ops);
    }

    #[test]
    fn capacity_stats_guard_creates_and_drops_without_panic() {
        let guard = CapacityStatsGuard::new();
        drop(guard);
    }

    #[test]
    fn capacity_stats_guard_default_creates_without_panic() {
        let _guard = CapacityStatsGuard::default();
    }
}
