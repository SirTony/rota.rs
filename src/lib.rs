mod ext;

pub mod job;
pub mod scheduler;
pub mod scheduling;

use justerror::Error;

pub use crate::scheduler::*;
pub use uuid::Uuid;

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
    JobNotFound(Uuid),

    #[error(desc = "error in {job}: {error}")]
    Internal {
        job: JobHandle,
        error: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error(desc = "the given chrono::Duration contains an invalid value: {0}")]
    InvalidInterval(#[from] chrono::OutOfRangeError),
}
