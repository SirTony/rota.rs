pub mod scheduler;
pub mod scheduling;
pub mod task;

use chrono::{DateTime, Utc};
use justerror::Error;

pub use crate::scheduler::*;
pub use async_trait::async_trait;
pub use tokio_util::sync::CancellationToken;

pub type Result<T> = std::result::Result<T, crate::Error>;

#[Error]
pub enum Error {
    #[cfg(feature = "cron")]
    Cron(
        #[from]
        #[cfg(feature = "cron")]
        cron::error::Error,
    ),

    #[error(desc = "the cancellation signal was raised")]
    Cancelled,

    #[error(desc = "the given chrono::Duration contains an invalid value: {0}")]
    InvalidInterval(#[from] chrono::OutOfRangeError),

    #[error(desc = "the end of the given date range is before the start")]
    InvalidDateRange {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}
