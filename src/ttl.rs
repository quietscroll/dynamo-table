use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};

/// Time to live struct that correctly forces numeric serialization for DynamoDB
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ttl(i64);

impl Ttl {
    /// Initialize Time To Live from UTC now
    /// Returns None if the duration causes an overflow or safety issue
    pub fn new(duration: Duration) -> Option<Self> {
        let target_time = OffsetDateTime::now_utc().checked_add(duration)?;

        // DynamoDB safety rule: Items with timestamps older than 5 years are ignored.
        // Prevent accidental past or extreme negative values.
        if target_time < OffsetDateTime::now_utc() - Duration::days(365 * 5) {
            return None;
        }

        Some(Self(target_time.unix_timestamp()))
    }

    /// Construct a TTL directly from a Unix timestamp (seconds since epoch)
    pub fn from_unix_timestamp(timestamp: i64) -> Self {
        Self(timestamp)
    }

    /// Construct a TTL set to expire after the given number of seconds from now.
    /// Returns `None` if `Duration::seconds(seconds)` or timestamp addition overflows or violates safety bounds.
    pub fn from_secs(seconds: i64) -> Option<Self> {
        Self::new(Duration::seconds(seconds))
    }

    /// Construct a TTL set to expire after the given number of days from now.
    /// Returns `None` if `Duration::days(days)` or timestamp addition overflows or violates safety bounds.
    pub fn from_days(days: i64) -> Option<Self> {
        Self::new(Duration::days(days))
    }

    /// Explicitly expose the internal unix timestamp as a primitive i64 number
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

// Convert directly to primitive i64 for SDK numeric parameters
impl From<Ttl> for i64 {
    fn from(ttl: Ttl) -> i64 {
        ttl.0
    }
}

impl From<i64> for Ttl {
    fn from(timestamp: i64) -> Self {
        Self(timestamp)
    }
}

impl From<OffsetDateTime> for Ttl {
    fn from(time: OffsetDateTime) -> Self {
        Self(time.unix_timestamp())
    }
}

impl TryFrom<Ttl> for OffsetDateTime {
    type Error = time::error::ComponentRange;

    fn try_from(val: Ttl) -> Result<Self, Self::Error> {
        OffsetDateTime::from_unix_timestamp(val.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttl_from_unix_timestamp() {
        let ttl = Ttl::from_unix_timestamp(1700000000);
        assert_eq!(ttl.as_i64(), 1700000000);
        assert_eq!(i64::from(ttl), 1700000000);
    }

    #[test]
    fn test_ttl_from_i64() {
        let ttl: Ttl = 1700000000i64.into();
        assert_eq!(ttl.as_i64(), 1700000000);
    }

    #[test]
    fn test_ttl_from_secs_and_days() {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let ttl_secs = Ttl::from_secs(3600).unwrap();
        assert!(ttl_secs.as_i64() >= now + 3598 && ttl_secs.as_i64() <= now + 3602);

        let ttl_days = Ttl::from_days(7).unwrap();
        let expected = now + 7 * 86400;
        assert!(ttl_days.as_i64() >= expected - 5 && ttl_days.as_i64() <= expected + 5);
    }

    #[test]
    fn test_ttl_offset_date_time_conversion() {
        let now = OffsetDateTime::now_utc();
        let ttl = Ttl::from(now);
        assert_eq!(ttl.as_i64(), now.unix_timestamp());

        let converted: OffsetDateTime = ttl.try_into().unwrap();
        assert_eq!(converted.unix_timestamp(), now.unix_timestamp());
    }
}
