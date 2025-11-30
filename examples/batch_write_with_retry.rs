/// Example: Handling batch_write partial failures with exponential backoff retry
///
/// This demonstrates the proper way to handle unprocessed items from batch operations.

use std::time::Duration;
use dynamo_table::table::{batch_write, DynamoTable};

/// Retry batch write with exponential backoff
///
/// DynamoDB may not process all items in a batch due to throttling or capacity limits.
/// This function automatically retries unprocessed items with exponential backoff.
async fn batch_write_with_retry<T>(
    items: Vec<T>,
    max_retries: usize,
) -> Result<(), dynamo_table::Error>
where
    T: DynamoTable + Clone,
    T::PK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
    T::SK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
{
    let mut unprocessed = items;
    let mut retry_count = 0;
    let mut delay_ms = 100; // Start with 100ms delay

    while !unprocessed.is_empty() && retry_count < max_retries {
        println!(
            "Attempt {}: Writing {} items",
            retry_count + 1,
            unprocessed.len()
        );

        // Perform batch write
        let result = batch_write::<T>(unprocessed.clone(), vec![]).await?;

        // Check if all items were processed
        if result.failed_puts.is_empty() {
            println!("✓ All items processed successfully!");
            return Ok(());
        }

        // Some items weren't processed
        let unprocessed_count = result.failed_puts.len();
        println!(
            "⚠ {} items were not processed (throttling or capacity limits)",
            unprocessed_count
        );

        // Update unprocessed items for retry
        unprocessed = result.failed_puts;

        // Exponential backoff before retry
        if retry_count < max_retries - 1 {
            println!("Waiting {}ms before retry...", delay_ms);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            delay_ms *= 2; // Exponential backoff: 100ms, 200ms, 400ms, 800ms...
            retry_count += 1;
        } else {
            break;
        }
    }

    if !unprocessed.is_empty() {
        eprintln!(
            "✗ Failed to process {} items after {} retries",
            unprocessed.len(),
            max_retries
        );
        // In production, you might want to:
        // 1. Log these items for manual investigation
        // 2. Send to a dead-letter queue
        // 3. Write to a failure log file

        // Note: This is just an example showing the pattern.
        // In practice, with built-in retry in batch_write(), you would just
        // check the result.failed_puts after the operation completes.
    }

    Ok(())
}

/// Example: Mixed batch write (both create and delete) with retry
async fn batch_write_mixed_with_retry<T>(
    to_create: Vec<T>,
    to_delete: Vec<T>,
    max_retries: usize,
) -> Result<(), dynamo_table::Error>
where
    T: DynamoTable + Clone,
    T::PK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
    T::SK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
{
    let mut unprocessed_creates = to_create;
    let unprocessed_deletes = to_delete;
    let mut retry_count = 0;
    let mut delay_ms = 100;

    while (!unprocessed_creates.is_empty() || !unprocessed_deletes.is_empty())
        && retry_count < max_retries
    {
        println!(
            "Attempt {}: Creating {} items, deleting {} items",
            retry_count + 1,
            unprocessed_creates.len(),
            unprocessed_deletes.len()
        );

        let result = batch_write::<T>(
            unprocessed_creates.clone(),
            unprocessed_deletes.clone(),
        )
        .await?;

        // Check results
        let unprocessed_create_count = result.failed_puts.len();
        let unprocessed_delete_count = result.failed_deletes.len();

        if unprocessed_create_count == 0 && unprocessed_delete_count == 0 {
            println!("✓ All operations completed successfully!");
            return Ok(());
        }

        println!(
            "⚠ Unprocessed: {} creates, {} deletes",
            unprocessed_create_count, unprocessed_delete_count
        );

        // For deletes, we need to reconstruct the items from keys
        // Note: This is a limitation - you may want to keep the original delete list
        unprocessed_creates = result.failed_puts;

        // Handle unprocessed deletes
        // The result contains (partition_key, sort_key) tuples
        // You'll need to reconstruct the items if you want to retry

        if retry_count < max_retries - 1 {
            println!("Waiting {}ms before retry...", delay_ms);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            delay_ms *= 2;
            retry_count += 1;
        } else {
            break;
        }
    }

    Ok(())
}

/// Example: Monitoring batch write metrics
async fn batch_write_with_metrics<T>(items: Vec<T>) -> Result<(), dynamo_table::Error>
where
    T: DynamoTable + Clone,
    T::PK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
    T::SK: std::fmt::Display + Clone + Send + Sync + std::fmt::Debug,
{
    let total_items = items.len();
    let result = batch_write::<T>(items, vec![]).await?;

    // Check capacity consumption
    let total_capacity: f64 = result
        .consumed_capacity
        .iter()
        .map(|cc| cc.capacity_units().unwrap_or(0.0))
        .sum();

    println!("Batch Write Metrics:");
    println!("  Total items: {}", total_items);
    println!("  Consumed capacity: {} units", total_capacity);
    println!("  Batches processed: {}", result.consumed_capacity.len());
    println!(
        "  Unprocessed items: {}",
        result.failed_puts.len()
    );

    // Calculate success rate
    let processed = total_items - result.failed_puts.len();
    let success_rate = (processed as f64 / total_items as f64) * 100.0;
    println!("  Success rate: {:.1}%", success_rate);

    // Check for item collection metrics (useful for tables with LSI)
    if !result.item_collection_metrics.is_empty() {
        println!("  Item collections affected: {}", result.item_collection_metrics.len());
    }

    Ok(())
}

// Note: This is an example file showing patterns, not a runnable program
fn main() {
    println!("See function implementations above for batch_write patterns");
}
