mod ext;

pub mod scheduler;
pub mod scheduling;
pub mod task;

use chrono::{DateTime, Utc};
use justerror::Error;
use task::{TaskError, TaskId};

pub use crate::scheduler::*;
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

    #[error(desc = "the scheduler is already initialized")]
    AlreadyInitialized,

    #[error(desc = "the scheduler hasn't been initialized yet")]
    NotInitialized,

    #[error(desc = "the scheduler has been shut down")]
    Terminated,

    #[error(desc = "the scheduler has not been started")]
    NotStarted,

    #[error(desc = "the scheduler has not been stopped")]
    AlreadyRunning,

    #[error(desc = "the cancellation signal was raised")]
    Cancelled,

    #[error(desc = "requested task was not found")]
    TaskNotFound(uuid::Uuid),

    #[error(desc = "error in {id}: {error}")]
    Internal { id: TaskId, error: TaskError },

    #[error(desc = "the given chrono::Duration contains an invalid value: {0}")]
    InvalidInterval(#[from] chrono::OutOfRangeError),

    #[error(desc = "the end of the given date range is before the start")]
    InvalidDateRange {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
}
