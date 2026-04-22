//! DynamoDB consumed capacity tracking using global atomic counters.
//!
//! Gated by the `consumed_capacity_stats` feature flag (enabled by default).
//! Records read and write capacity units across every DynamoDB operation and
//! exposes convenience functions for querying totals and logging them via
//! the `tracing` crate.

pub mod stats;
