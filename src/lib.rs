mod ext;

pub mod job;
pub mod scheduler;
pub mod scheduling;

use chrono::{DateTime, Utc};
use justerror::Error;

pub use crate::scheduler::*;
pub use tokio_util::sync::CancellationToken;

#[Error]
pub enum SchedulerError {
    Cron(#[from] cron::error::Error),

    #[error(desc = "the job scheduler is already initialized")]
    AlreadyInitialized,

    #[error(desc = "the job scheduler hasn't been initialized yet")]
    NotInitialized,

    #[error(desc = "the job scheduler has been shut down")]
    Terminated,

    #[error(desc = "the cancellation signal was raised")]
    Cancelled,

    #[error(desc = "requested job was not found")]
    JobNotFound(uuid::Uuid),

    #[error(desc = "error in {job}: {error}")]
    Internal {
        job: JobHandle,
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error(desc = "the given chrono::Duration contains an invalid value: {0}")]
    InvalidInterval(#[from] chrono::OutOfRangeError),

    #[error(desc = "the end of the given date range is before the start")]
    InvalidDateRange {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}
