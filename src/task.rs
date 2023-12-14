use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use tokio::{
    select,
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use uuid::Uuid;

use crate::scheduling::Schedule;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The result type for all tasks in the scheduler.
/// Tasks may not return data, but their errors must be bubbled up to the scheduler to be handled.
pub type Result = std::result::Result<(), Error>;

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the task scheduler.
#[async_trait]
pub trait AsyncExecutable {
    async fn execute(&mut self, task: Task, ct: CancellationToken) -> Result;
}

#[async_trait]
impl<Fun, Fut> AsyncExecutable for Fun
where
    Fun: 'static + Send + Sync + FnMut(Task, CancellationToken) -> Fut,
    Fut: Future<Output = Result> + Send + Sync,
{
    async fn execute(&mut self, task: Task, ct: CancellationToken) -> Result {
        let c_ct = ct.clone();

        select! {
            task_result = (*self)(task, ct) => task_result,
            _ = c_ct.cancelled() => Err( crate::Error::Cancelled.into() ),
        }
    }
}

#[async_trait]
/// Represents a callback that will be invoked when a task encounters an error.
pub trait AsyncErrorCallback {
    async fn on_error(&mut self, task: Task, e: Error);
}

#[async_trait]
impl<Fun, Fut> AsyncErrorCallback for Fun
where
    Fun: Send + Sync + 'static + FnMut(Task, Error) -> Fut,
    Fut: Send + Sync + 'static + Future<Output = ()>,
{
    async fn on_error(&mut self, task: Task, e: Error) {
        (*self)(task, e).await
    }
}

/*
    you may be wondering why I chose to spend the time writing out this builder
    when crates like typed-builder exist and provide the same compile-time builder validation
    this does without having to faff about with all this boilerplate nonsense.

    I reinvented this particular wheel for 1 main reason:

    this library uses generics on the public-facing API and then wraps things up with Box<dyn ...>
    before constructing the final Arc<Mutex<...>> or Arc<RwLock<...>> that's used internally
    and neithed typed-builder nor derive-builder make doing that easy with optional generic fields

    We only need a handful different fields on this struct so it's not too much to quickly write out the builder
    manually when it providers a nicer public API that's consistent with the rest of this crate.

    Should this builder substantially outgrow its current form, revisiting the NIH syndrome approach
    will be necessary.
*/
/// Used to construct a [`Task`] with a fluent API.
/// The builder is verified at compile-time to ensure that all required fields are set before building,
/// and that no fields are set more than once.
/// The [`TaskBuilder::build()`] method does not become available until all required fields are set.
/// In order to create a new builder, use [`Task::builder()`].
pub struct TaskBuilder<
    const __HAS_EXEC: bool = false,
    const __HAS_SCHEDULE: bool = false,
    const __HAS_ERR_FN: bool = false,
    const __HAS_NAME: bool = false,
> {
    exec: Option<Arc<Mutex<dyn AsyncExecutable + Send + Sync>>>,
    schedule: Option<Arc<RwLock<dyn Schedule + Send + Sync>>>,
    err_fn: Option<Arc<Mutex<dyn AsyncErrorCallback + Send + Sync>>>,
    name: Option<String>,
}

impl<const __HAS_SCHEDULE: bool, const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<false, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn executable<E: AsyncExecutable + Send + Sync + 'static>(
        self,
        exec: E,
    ) -> TaskBuilder<true, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME> {
        TaskBuilder::<true, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME> {
            exec: Some(Arc::new(Mutex::new(exec))),
            schedule: self.schedule,
            err_fn: self.err_fn,
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<__HAS_EXEC, false, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn schedule<S: Schedule + Send + Sync + 'static>(
        self,
        schedule: S,
    ) -> TaskBuilder<__HAS_EXEC, true, __HAS_ERR_FN, __HAS_NAME> {
        TaskBuilder::<__HAS_EXEC, true, __HAS_ERR_FN, __HAS_NAME> {
            exec: self.exec,
            schedule: Some(Arc::new(RwLock::new(schedule))),
            err_fn: self.err_fn,
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_SCHEDULE: bool, const __HAS_NAME: bool>
    TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, false, __HAS_NAME>
{
    pub fn on_error<Fun>(self, f: Fun) -> TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, true, __HAS_NAME>
    where
        Fun: AsyncErrorCallback + Send + Sync + 'static,
    {
        TaskBuilder::<__HAS_EXEC, __HAS_SCHEDULE, true, __HAS_NAME> {
            exec: self.exec,
            schedule: self.schedule,
            err_fn: Some(Arc::new(Mutex::new(f))),
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_SCHEDULE: bool, const __HAS_ERR_FN: bool>
    TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, __HAS_ERR_FN, false>
{
    pub fn name<S: Into<String>>(
        self,
        name: S,
    ) -> TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, __HAS_ERR_FN, true> {
        TaskBuilder::<__HAS_EXEC, __HAS_SCHEDULE, __HAS_ERR_FN, true> {
            exec: self.exec,
            schedule: self.schedule,
            err_fn: self.err_fn,
            name: Some(name.into()),
        }
    }
}

impl<const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<true, true, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn build(self) -> Task {
        Task {
            exec: self
                .exec
                .expect("the maintainer forgot to set the exec field"),
            schedule: self
                .schedule
                .expect("the maintainer forgot to set the schedule field"),
            err_fn: self.err_fn,
            name: self.name.map(Arc::new),
            is_paused: Arc::new(AtomicBool::new(false)),
            is_valid: Arc::new(AtomicBool::new(false)),
            should_remove: Arc::new(AtomicBool::new(false)),
            activity: None,
        }
    }
}

pub(crate) struct Activity {
    pub handle: JoinHandle<()>,
    pub ct: CancellationToken,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
/// Represents the current status of a task.
pub enum Status {
    /// The task is invalid and cannot be operated on.
    Invalid,

    /// The task is paused and will not be run until it is resumed.
    /// Paused tasks will still have their schedules advanced as normal.
    Paused,

    /// The task is ready and waiting to be run.
    Ready,

    /// The task is currently running.
    Active,

    /// The task has finished running and has exited normally.
    Finished,

    /// The task has been cancelled.
    Cancelled,

    /// The task cannot be run again and is awaiting removal from the scheduler.
    AwaitingRemoval,
}

#[derive(Clone)]
/// Represents a task that can be run repeatedly according to a specified schedule.
/// A task is created by the user and then ownership is transferred to the scheduler when adding the task.
/// All tasks are created exactly once and only dropped once the scheduler is shut down, or the task's schedule reaches some end point.
/// Tasks may be created from user-defined `struct`s or functions (including anonymous functions). See [`Task::new_sync()`] or [`Task::new_async()`] for examples.
/// Because functions don't carry state beyond what they capture from their local scope or global variables, it is recommended to use a `struct` if persistent state is needed.
/// Tasks also may not return data (beyond error states), so if passing data around is needed it is recommended to use channels.
pub struct Task {
    pub(crate) exec: Arc<Mutex<dyn AsyncExecutable + Send + Sync>>,
    pub(crate) schedule: Arc<RwLock<dyn Schedule + Send + Sync>>,
    pub(crate) err_fn: Option<Arc<Mutex<dyn AsyncErrorCallback + Send + Sync>>>,
    pub(crate) name: Option<Arc<String>>,
    pub(crate) is_paused: Arc<AtomicBool>,
    pub(crate) is_valid: Arc<AtomicBool>,
    pub(crate) should_remove: Arc<AtomicBool>,
    pub(crate) activity: Option<Arc<Activity>>,
}

impl Task {
    /// Creates a new [`TaskBuilder`] with no fields set.
    pub fn builder() -> TaskBuilder {
        TaskBuilder {
            exec: None,
            schedule: None,
            err_fn: None,
            name: None,
        }
    }

    /// Returns the current status of the task.
    /// See [`Status`] for more information.
    pub async fn status(&self) -> Status {
        if !self.is_valid.load(Ordering::SeqCst) && !self.should_remove.load(Ordering::SeqCst) {
            Status::Invalid
        } else if self.is_paused.load(Ordering::SeqCst) {
            Status::Paused
        } else if self.should_remove.load(Ordering::SeqCst)
            || self.schedule.read().await.is_finished().await
        {
            Status::AwaitingRemoval
        } else if let Some(activity) = &self.activity {
            if activity.ct.is_cancelled() {
                Status::Cancelled
            } else if activity.handle.is_finished() {
                Status::Finished
            } else {
                Status::Active
            }
        } else if self.schedule.read().await.is_ready().await
            && !self.is_paused.load(Ordering::SeqCst)
            && self.activity.is_none()
            && !self.should_remove.load(Ordering::SeqCst)
        {
            Status::Ready
        } else {
            Status::Invalid
        }
    }

    /// Returns the name of the task, if one was set.
    pub fn name(&self) -> Option<String> {
        self.name.as_ref().map(|name| name.to_string())
    }

    /// Pauses the task. The task will not be run until it is resumed.
    /// Paused tasks will still have their schedules advanced as normal.
    /// If the task is currently running, it will not be cancelled.
    pub fn pause(&self) {
        self.is_paused.store(true, Ordering::SeqCst)
    }

    /// Resumes normal operation of the task.
    pub fn resume(&self) {
        self.is_paused.store(false, Ordering::SeqCst)
    }

    /// Cancels the task and aborts its execution, if it is currently running.
    pub fn cancel(&mut self) {
        if let Some(activity) = self.activity.take() {
            activity.ct.cancel();
            activity.handle.abort();
        }
    }

    /// Signals the scheduler that this task should be removed.
    /// The task is not removed immediately, but will be removed on the next iteration of the scheduler's background loop.
    pub async fn remove(&mut self) {
        self.pause();
        self.cancel();

        self.should_remove.store(true, Ordering::SeqCst);
        self.is_valid.store(false, Ordering::SeqCst);
    }
}

impl Task {
    // / Creates a new task that executes synchronously.
    // /
    // / The scheduler will spawn a new thread for the task when it is run; this function provides a means to create tasks that don't use `async`/`await`.
    // / If an `async` context is needed, please use [`Task::new_async()`] instead.
    // /
    // / # Examples
    // / ----------
    // /
    // / Tasks may be created from anonymous functions.
    // /
    // / ```
    // / use rota::*;
    // / use rota::task::*;
    // / use rota::scheduling::*;
    // /
    // / let once = LimitedRun::once( Always );
    // / let task = ScheduledTask::new_sync( once, | id, ct | {
    // /     println!( "Hello, World!" );
    // /     Ok( () )
    // / } );
    // / ```
    // / --------
    // /
    // / Tasks may also be created from named functions.
    // /
    // / ```
    // / use rota::*;
    // / use rota::task::*;
    // / use rota::scheduling::*;
    // /
    // / fn hello_world( id: Id, ct: CancellationToken ) -> rota::task::Result {
    // /     println!( "Hello, World!" );
    // /     Ok( () )
    // / }
    // /
    // / let once = LimitedRun::once( Always );
    // / let task = ScheduledTask::new_sync( once, hello_world );
    // / ```
    // / --------
    // /
    // / Tasks that require persistent state or the ability to share data should be implemented as a `struct`.
    // / ```
    // / use rota::*;
    // / use rota::task::*;
    // / use rota::scheduling::*;
    // / use tokio::sync::mpsc::*;
    // / use std::time::Duration;
    // /
    // / struct Foo {
    // /     a: u32,
    // /     b: u32,
    // / }
    // /
    // / struct Producer {
    // /     chan: Sender<Foo>
    // / }
    // /
    // / impl Executable for Producer {
    // /     fn execute( &mut self, id: Id, ct: CancellationToken ) -> rota::task::Result {
    // /         /* do some processing and send off a result */
    // /         Ok( () )
    // /     }
    // / }
    // /
    // / struct Consumer {
    // /     chan: Receiver<Foo>,
    // /     foos: Vec<Foo>
    // / }
    // /
    // / impl Executable for Consumer {
    // /     fn execute( &mut self, id: Id, ct: CancellationToken ) -> rota::task::Result {
    // /         /* receive some data and do more processing */
    // /         Ok( () )
    // /     }
    // / }
    // /
    // / let ( sender, receiver ) = channel( 100 );
    // / let sender = Producer { chan: sender };
    // / let receiver = Consumer { chan: receiver, foos: Vec::new() };
    // /
    // / let every_5s = Interval::new( Duration::from_secs( 5 ) );
    // / let every_30s = Interval::new( Duration::from_secs( 30 ) );
    // /
    // / let sender_task = ScheduledTask::new_sync( every_5s, sender );
    // / let receiver_task = ScheduledTask::new_sync( every_30s, receiver );
    // / ```
    // pub fn new_sync<S, E>(schedule: S, exec: E) -> Self
    // where
    //     S: Schedule + Send + Sync + 'static,
    //     E: Executable + Send + Sync + 'static,
    // {
    //     Self {
    //         exec: TaskExecutable::Sync(Arc::new(Mutex::new(exec))),
    //         schedule: Arc::new(RwLock::new(schedule)),
    //         err_fn: None,
    //         name: None,
    //         is_paused: Arc::new(AtomicBool::new(false)),
    //     }
    // }

    // / Creates a new task that executes asynchronously.
    // /
    // / The scheduler will spawn a new thread for the task when it is run, and this function provides a means of creating tasks that use `async`/`await`.
    // / If an `async` context is not necessary, please consider using [`Task::new_sync()`] instead.
    // /
    // / This function is not able to create tasks from anonymous functions, as `async` closures are currently an unstable feature.
    // / Creating new tasks from named `async` functions is still supported.
    // /
    // / # Examples
    // / ----------
    // /
    // / Tasks may be created from named functions.
    // /
    // / ```
    // / use rota::*;
    // / use rota::task::*;
    // / use rota::scheduling::*;
    // /
    // / async fn hello_world( id: Id, ct: CancellationToken ) -> rota::task::Result {
    // /     println!( "Hello, World!" );
    // /     Ok( () )
    // / }
    // /
    // / let once = LimitedRun::once( Always );
    // / let task = ScheduledTask::new_async( once, hello_world );
    // / ```
    // / --------
    // /
    // / Tasks that require persistent state or the ability to share data should be implemented as a `struct`.
    // / ```
    // / use rota::*;
    // / use rota::task::*;
    // / use rota::scheduling::*;
    // / use tokio::sync::mpsc::*;
    // / use std::time::Duration;
    // / use async_trait::async_trait;
    // /
    // / struct Foo {
    // /     a: u32,
    // /     b: u32,
    // / }
    // /
    // / struct Producer {
    // /     chan: Sender<Foo>
    // / }
    // /
    // / #[async_trait]
    // / impl AsyncExecutable for Producer {
    // /     async fn execute( &mut self, id: Id, ct: CancellationToken ) -> rota::task::Result {
    // /         /* do some processing and send off a result */
    // /         Ok( () )
    // /     }
    // / }
    // /
    // / struct Consumer {
    // /     chan: Receiver<Foo>,
    // /     foos: Vec<Foo>
    // / }
    // /
    // / #[async_trait]
    // / impl AsyncExecutable for Consumer {
    // /     async fn execute( &mut self, id: Id, ct: CancellationToken ) -> rota::task::Result {
    // /         /* receive some data and do more processing */
    // /         Ok( () )
    // /     }
    // / }
    // /
    // / let ( sender, receiver ) = channel( 100 );
    // / let sender = Producer { chan: sender };
    // / let receiver = Consumer { chan: receiver, foos: Vec::new() };
    // /
    // / let every_5s = Interval::new( Duration::from_secs( 5 ) );
    // / let every_30s = Interval::new( Duration::from_secs( 30 ) );
    // /
    // / let sender_task = ScheduledTask::new_async( every_5s, sender );
    // / let receiver_task = ScheduledTask::new_async( every_30s, receiver );
    // / ```
    // pub fn new_async<S, E>(schedule: S, exec: E) -> Self
    // where
    //     S: Schedule + Send + Sync + 'static,
    //     E: AsyncExecutable + Send + Sync + 'static,
    // {
    //     Self {
    //         exec: TaskExecutable::Async(Arc::new(Mutex::new(exec))),
    //         schedule: Arc::new(RwLock::new(schedule)),
    //         err_fn: None,
    //         name: None,
    //         is_paused: Arc::new(AtomicBool::new(false)),
    //     }
    // }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
/// Represents the unique identifier for a task.
pub struct Id(pub(crate) Uuid);

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
