use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::scheduling::Schedule;

pub use async_trait::async_trait;

/// The result type for all jobs in the scheduler.
/// Jobs may not return data, but their errors must be bubbled up to the scheduler to be handled.
pub type JobResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the job scheduler.
/// This trait is for jobs that contain no `async` code. If an `async` context is needed, implement `AsyncExecutable` instead.
pub trait Executable {
    /// Contains the logic of the job that will be run by the scheduler at the appropriate time.
    /// # Arguments
    ///  * `id` - ff
    fn execute(&mut self, id: Uuid, ct: CancellationToken) -> JobResult;
}

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the job scheduler.
/// This trait is for jobs that contain `async` code. If an `async` context is not needed, implement `Executable` instead.
#[async_trait]
pub trait AsyncExecutable {
    async fn execute(&mut self, id: Uuid, ct: CancellationToken) -> JobResult;
}

impl<F: 'static + Send + Sync + FnMut(Uuid, CancellationToken) -> JobResult> Executable for F {
    fn execute(&mut self, id: Uuid, ct: CancellationToken) -> JobResult {
        (*self)(id, ct)
    }
}

#[async_trait]
impl<F: 'static + Send + Sync + FnMut(Uuid, CancellationToken) -> JobResult> AsyncExecutable for F {
    async fn execute(&mut self, id: Uuid, ct: CancellationToken) -> JobResult {
        (*self)(id, ct)
    }
}

#[derive(Clone)]
pub(crate) enum JobKind {
    Sync(Arc<Mutex<dyn Executable + Send + Sync>>),
    Async(Arc<Mutex<dyn AsyncExecutable + Send + Sync>>),
}

#[derive(Clone)]
pub struct Job {
    pub(crate) kind: JobKind,
    pub(crate) schedule: Arc<RwLock<dyn Schedule + Send + Sync>>,
}

impl Job {
    pub fn new_sync<S, E>(schedule: S, exec: E) -> Self
    where
        S: Schedule + Send + Sync + 'static,
        E: Executable + Send + Sync + 'static,
    {
        Self {
            kind: JobKind::Sync(Arc::new(Mutex::new(exec))),
            schedule: Arc::new(RwLock::new(schedule)),
        }
    }

    pub fn new_async<S, E>(schedule: S, exec: E) -> Self
    where
        S: Schedule + Send + Sync + 'static,
        E: AsyncExecutable + Send + Sync + 'static,
    {
        Self {
            kind: JobKind::Async(Arc::new(Mutex::new(exec))),
            schedule: Arc::new(RwLock::new(schedule)),
        }
    }
}
