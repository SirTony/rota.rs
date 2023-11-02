use std::{future::Future, sync::Arc};

use tokio::{
    select,
    sync::{Mutex, RwLock},
};
use tokio_util::sync::CancellationToken;

use crate::{scheduling::Schedule, JobHandle, SchedulerError};

pub use async_trait::async_trait;

/// The result type for all jobs in the scheduler.
/// Jobs may not return data, but their errors must be bubbled up to the scheduler to be handled.
pub type JobResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the job scheduler.
/// This trait is for jobs that contain no `async` code. If an `async` context is needed, implement [AsyncExecutable] instead.
pub trait Executable {
    /// Contains the logic of the job that will be run by the scheduler at the appropriate time.
    /// # Arguments
    fn execute(&mut self, handle: JobHandle, ct: CancellationToken) -> JobResult;
}

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the job scheduler.
/// This trait is for jobs that contain `async` code. If an `async` context is not needed, implement [Executable] instead.
#[async_trait]
pub trait AsyncExecutable {
    async fn execute(&mut self, handle: JobHandle, ct: CancellationToken) -> JobResult;
}

impl<F: 'static + Send + Sync + FnMut(JobHandle, CancellationToken) -> JobResult> Executable for F {
    fn execute(&mut self, handle: JobHandle, ct: CancellationToken) -> JobResult {
        (*self)(handle, ct)
    }
}

#[async_trait]
impl<Fun, Fut> AsyncExecutable for Fun
where
    Fun: 'static + Send + Sync + FnMut(JobHandle, CancellationToken) -> Fut,
    Fut: Future<Output = JobResult> + Send + Sync,
{
    async fn execute(&mut self, handle: JobHandle, ct: CancellationToken) -> JobResult {
        let c_ct = ct.clone();

        select! {
            job_result = (*self)(handle, ct) => job_result,
            _ = c_ct.cancelled() => Err( SchedulerError::Cancelled.into() ),
        }
    }
}

#[derive(Clone)]
pub(crate) enum JobKind {
    Sync(Arc<Mutex<dyn Executable + Send + Sync>>),
    Async(Arc<Mutex<dyn AsyncExecutable + Send + Sync>>),
}

#[derive(Clone)]
/// Represents a task that can be run repeatedly according to a specified schedule.
/// A job is created by the user and then ownership is transferred to the scheduler when adding the job.
/// All jobs are created exactly once and only dropped once the scheduler is shut down, or the job's schedule reaches some end point.
/// Jobs may be created from user-defined `struct`s or functions (including anonymous functions). See [`Job::new_sync()`] or [`Job::new_async()`] for examples.
/// Because functions don't carry state beyond what they capture from their local scope or global variables, it is recommended to use a `struct` if persistent state is needed.
/// Jobs also may not return data (beyond error states), so if passing data around is needed it is recommended to use channels.
pub struct Job {
    pub(crate) kind: JobKind,
    pub(crate) schedule: Arc<RwLock<dyn Schedule + Send + Sync>>,
}

impl Job {
    /// Creates a new job that executes synchronously.
    ///
    /// The scheduler will spawn a new thread for the job when it is run; this function provides a means to create jobs that don't use `async`/`await`.
    /// If an `async` context is needed, please use [`Job::new_async()`] instead.
    ///
    /// # Examples
    /// ----------
    ///
    /// Jobs may be created from anonymous functions.
    ///
    /// ```
    /// use rota::*;
    /// use rota::job::*;
    /// use rota::scheduling::*;
    ///
    /// let once = LimitedRunSchedule::once( AlwaysSchedule );
    /// let job = Job::new_sync( once, | handle, ct | {
    ///     println!( "Hello, World!" );
    ///     Ok( () )
    /// } );
    /// ```
    /// --------
    ///
    /// Jobs may also be created from named functions.
    ///
    /// ```
    /// use rota::*;
    /// use rota::job::*;
    /// use rota::scheduling::*;
    ///
    /// fn hello_world( handle: JobHandle, ct: CancellationToken ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     println!( "Hello, World!" );
    ///     Ok( () )
    /// }
    ///
    /// let once = LimitedRunSchedule::once( AlwaysSchedule );
    /// let job = Job::new_sync( once, hello_world );
    /// ```
    /// --------
    ///
    /// Jobs that require persistent state or the ability to share data should be implemented as a `struct`.
    /// ```
    /// use rota::*;
    /// use rota::job::*;
    /// use rota::scheduling::*;
    /// use tokio::sync::mpsc::*;
    /// use std::time::Duration;
    ///
    /// struct Foo {
    ///     a: u32,
    ///     b: u32,
    /// }
    ///
    /// struct Producer {
    ///     chan: Sender<Foo>
    /// }
    ///
    /// impl Executable for Producer {
    ///     fn execute( &mut self, handle: JobHandle, ct: CancellationToken ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///         /* do some processing and send off a result */
    ///         Ok( () )
    ///     }
    /// }
    ///
    /// struct Consumer {
    ///     chan: Receiver<Foo>,
    ///     foos: Vec<Foo>
    /// }
    ///
    /// impl Executable for Consumer {
    ///     fn execute( &mut self, handle: JobHandle, ct: CancellationToken ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///         /* receive some data and do more processing */
    ///         Ok( () )
    ///     }
    /// }
    ///
    /// let ( sender, receiver ) = channel( 100 );
    /// let sender = Producer { chan: sender };
    /// let receiver = Consumer { chan: receiver, foos: Vec::new() };
    ///
    /// let every_5s = IntervalSchedule::new( Duration::from_secs( 5 ) );
    /// let every_30s = IntervalSchedule::new( Duration::from_secs( 30 ) );
    ///
    /// let sender_job = Job::new_sync( every_5s, sender );
    /// let receiver_job = Job::new_sync( every_30s, receiver );
    /// ```
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

    /// Creates a new job that executes asynchronously.
    ///
    /// The scheduler will spawn a new thread for the job when it is run, and this function provides a means of creating jobs that use `async`/`await`.
    /// If an `async` context is not necessary, please consider using [`Job::new_sync()`] instead.
    ///
    /// This function is not able to create jobs from anonymous functions, as `async` closures are currently an unstable feature.
    /// Creating new jobs from named `async` functions is still supported.
    ///
    /// # Examples
    /// ----------
    ///
    /// Jobs may be created from named functions.
    ///
    /// ```
    /// use rota::*;
    /// use rota::job::*;
    /// use rota::scheduling::*;
    ///
    /// async fn hello_world( handle: JobHandle, ct: CancellationToken ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///     println!( "Hello, World!" );
    ///     Ok( () )
    /// }
    ///
    /// let once = LimitedRunSchedule::once( AlwaysSchedule );
    /// let job = Job::new_async( once, hello_world );
    /// ```
    /// --------
    ///
    /// Jobs that require persistent state or the ability to share data should be implemented as a `struct`.
    /// ```
    /// use rota::*;
    /// use rota::job::*;
    /// use rota::scheduling::*;
    /// use tokio::sync::mpsc::*;
    /// use std::time::Duration;
    /// use async_trait::async_trait;
    ///
    /// struct Foo {
    ///     a: u32,
    ///     b: u32,
    /// }
    ///
    /// struct Producer {
    ///     chan: Sender<Foo>
    /// }
    ///
    /// #[async_trait]
    /// impl AsyncExecutable for Producer {
    ///     async fn execute( &mut self, handle: JobHandle, ct: CancellationToken ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///         /* do some processing and send off a result */
    ///         Ok( () )
    ///     }
    /// }
    ///
    /// struct Consumer {
    ///     chan: Receiver<Foo>,
    ///     foos: Vec<Foo>
    /// }
    ///
    /// #[async_trait]
    /// impl AsyncExecutable for Consumer {
    ///     async fn execute( &mut self, handle: JobHandle, ct: CancellationToken ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ///         /* receive some data and do more processing */
    ///         Ok( () )
    ///     }
    /// }
    ///
    /// let ( sender, receiver ) = channel( 100 );
    /// let sender = Producer { chan: sender };
    /// let receiver = Consumer { chan: receiver, foos: Vec::new() };
    ///
    /// let every_5s = IntervalSchedule::new( Duration::from_secs( 5 ) );
    /// let every_30s = IntervalSchedule::new( Duration::from_secs( 30 ) );
    ///
    /// let sender_job = Job::new_async( every_5s, sender );
    /// let receiver_job = Job::new_async( every_30s, receiver );
    /// ```
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
