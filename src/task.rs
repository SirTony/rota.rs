use std::{future::Future, sync::Arc};

use tokio::{
    select,
    sync::{Mutex, RwLock},
};
use tokio_util::sync::CancellationToken;

use crate::{scheduling::Schedule, Error, TaskId};

pub use async_trait::async_trait;

pub type TaskError = Box<dyn std::error::Error + Send + Sync>;

/// The result type for all tasks in the scheduler.
/// Tasks may not return data, but their errors must be bubbled up to the scheduler to be handled.
pub type TaskResult = Result<(), TaskError>;

pub type ErrFn = Box<dyn FnMut(TaskId, TaskError) + Send + Sync>;

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the task scheduler.
/// This trait is for tasks that contain no `async` code. If an `async` context is needed, implement [AsyncExecutable] instead.
pub trait Executable {
    /// Contains the logic of the task that will be run by the scheduler at the appropriate time.
    /// # Arguments
    fn execute(&mut self, handle: TaskId, ct: CancellationToken) -> TaskResult;
}

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the task scheduler.
/// This trait is for tasks that contain `async` code. If an `async` context is not needed, implement [Executable] instead.
#[async_trait]
pub trait AsyncExecutable {
    async fn execute(&mut self, handle: TaskId, ct: CancellationToken) -> TaskResult;
}

impl<F: 'static + Send + Sync + FnMut(TaskId, CancellationToken) -> TaskResult> Executable for F {
    fn execute(&mut self, handle: TaskId, ct: CancellationToken) -> TaskResult {
        (*self)(handle, ct)
    }
}

#[async_trait]
impl<Fun, Fut> AsyncExecutable for Fun
where
    Fun: 'static + Send + Sync + FnMut(TaskId, CancellationToken) -> Fut,
    Fut: Future<Output = TaskResult> + Send + Sync,
{
    async fn execute(&mut self, handle: TaskId, ct: CancellationToken) -> TaskResult {
        let c_ct = ct.clone();

        select! {
            task_result = (*self)(handle, ct) => task_result,
            _ = c_ct.cancelled() => Err( Error::Cancelled.into() ),
        }
    }
}

#[derive(Clone)]
pub(crate) enum TaskExecutable {
    Sync(Arc<Mutex<dyn Executable + Send + Sync>>),
    Async(Arc<Mutex<dyn AsyncExecutable + Send + Sync>>),
}

/*
    you may be wondering why I chose to spend the time writing out this builder
    when crates like typed-builder exist and provide the same compile-time builder validation
    this does without having to faff about with all this nonsense.

    I reinvented this particular wheel for 1 main reason:

    this library uses generics on the public-facing API and then wraps things up with Box<dyn ...>
    before constructing the final Arc<Mutex<...>> or Arc<RwLock<...>> that's used internally
    and neithed typed-builder nor derive-builder make doing that easy.

    We only need 3 different fields on this struct so it's not too much to quickly write out the builder
    manually when it providers a nicer public API that's consistent with the rest of this crate
*/
pub struct TaskBuilder<
    const __HAS_EXEC: bool = false,
    const __HAS_SCHEDULE: bool = false,
    const __HAS_ERR_FN: bool = false,
> {
    exec: Option<TaskExecutable>,
    schedule: Option<Arc<RwLock<dyn Schedule + Send + Sync>>>,
    err_fn: Option<Arc<Mutex<ErrFn>>>,
}

impl<const __HAS_SCHEDULE: bool, const __HAS_ERR_FN: bool>
    TaskBuilder<false, __HAS_SCHEDULE, __HAS_ERR_FN>
{
    pub fn executable<E: Executable + Send + Sync + 'static>(
        self,
        exec: E,
    ) -> TaskBuilder<true, __HAS_SCHEDULE, __HAS_ERR_FN> {
        TaskBuilder::<true, __HAS_SCHEDULE, __HAS_ERR_FN> {
            exec: Some(TaskExecutable::Sync(Arc::new(Mutex::new(exec)))),
            schedule: self.schedule,
            err_fn: self.err_fn,
        }
    }

    pub fn async_executable<E: AsyncExecutable + Send + Sync + 'static>(
        self,
        exec: E,
    ) -> TaskBuilder<true, __HAS_SCHEDULE, __HAS_ERR_FN> {
        TaskBuilder::<true, __HAS_SCHEDULE, __HAS_ERR_FN> {
            exec: Some(TaskExecutable::Async(Arc::new(Mutex::new(exec)))),
            schedule: self.schedule,
            err_fn: self.err_fn,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_ERR_FN: bool>
    TaskBuilder<__HAS_EXEC, false, __HAS_ERR_FN>
{
    pub fn schedule<S: Schedule + Send + Sync + 'static>(
        self,
        schedule: S,
    ) -> TaskBuilder<__HAS_EXEC, true, __HAS_ERR_FN> {
        TaskBuilder::<__HAS_EXEC, true, __HAS_ERR_FN> {
            exec: self.exec,
            schedule: Some(Arc::new(RwLock::new(schedule))),
            err_fn: self.err_fn,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_SCHEDULE: bool>
    TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, false>
{
    pub fn on_error<F: FnMut(TaskId, TaskError) + Send + Sync + 'static>(
        self,
        f: F,
    ) -> TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, true> {
        TaskBuilder::<__HAS_EXEC, __HAS_SCHEDULE, true> {
            exec: self.exec,
            schedule: self.schedule,
            err_fn: Some(Arc::new(Mutex::new(Box::new(f)))),
        }
    }
}

impl<const __HAS_ERR_FN: bool> TaskBuilder<true, true, __HAS_ERR_FN> {
    pub fn build(self) -> Task {
        Task {
            exec: self
                .exec
                .expect("the maintainer forgot to set the exec field"),
            schedule: self
                .schedule
                .expect("the maintainer forgot to set the schedule field"),
            err_fn: self.err_fn,
        }
    }
}

#[derive(Clone)]
/// Represents a task that can be run repeatedly according to a specified schedule.
/// A task is created by the user and then ownership is transferred to the scheduler when adding the task.
/// All tasks are created exactly once and only dropped once the scheduler is shut down, or the task's schedule reaches some end point.
/// Tasks may be created from user-defined `struct`s or functions (including anonymous functions). See [`Task::new_sync()`] or [`Task::new_async()`] for examples.
/// Because functions don't carry state beyond what they capture from their local scope or global variables, it is recommended to use a `struct` if persistent state is needed.
/// Tasks also may not return data (beyond error states), so if passing data around is needed it is recommended to use channels.
pub struct Task {
    pub(crate) exec: TaskExecutable,
    pub(crate) schedule: Arc<RwLock<dyn Schedule + Send + Sync>>,
    pub(crate) err_fn: Option<Arc<Mutex<ErrFn>>>,
}

impl Task {
    pub fn builder() -> TaskBuilder<false, false, false> {
        TaskBuilder {
            exec: None,
            schedule: None,
            err_fn: None,
        }
    }
}

impl Task {
    /// Creates a new task that executes synchronously.
    ///
    /// The scheduler will spawn a new thread for the task when it is run; this function provides a means to create tasks that don't use `async`/`await`.
    /// If an `async` context is needed, please use [`Task::new_async()`] instead.
    ///
    /// # Examples
    /// ----------
    ///
    /// Tasks may be created from anonymous functions.
    ///
    /// ```
    /// use rota::*;
    /// use rota::task::*;
    /// use rota::scheduling::*;
    ///
    /// let once = LimitedRunSchedule::once( AlwaysSchedule::new() );
    /// let task = Task::new_sync( once, | handle, ct | {
    ///     println!( "Hello, World!" );
    ///     Ok( () )
    /// } );
    /// ```
    /// --------
    ///
    /// Tasks may also be created from named functions.
    ///
    /// ```
    /// use rota::*;
    /// use rota::task::*;
    /// use rota::scheduling::*;
    ///
    /// fn hello_world( handle: TaskId, ct: CancellationToken ) -> TaskResult {
    ///     println!( "Hello, World!" );
    ///     Ok( () )
    /// }
    ///
    /// let once = LimitedRunSchedule::once( AlwaysSchedule::new() );
    /// let task = Task::new_sync( once, hello_world );
    /// ```
    /// --------
    ///
    /// Tasks that require persistent state or the ability to share data should be implemented as a `struct`.
    /// ```
    /// use rota::*;
    /// use rota::task::*;
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
    ///     fn execute( &mut self, handle: TaskId, ct: CancellationToken ) -> TaskResult {
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
    ///     fn execute( &mut self, handle: TaskId, ct: CancellationToken ) -> TaskResult {
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
    /// let sender_task = Task::new_sync( every_5s, sender );
    /// let receiver_task = Task::new_sync( every_30s, receiver );
    /// ```
    pub fn new_sync<S, E>(schedule: S, exec: E) -> Self
    where
        S: Schedule + Send + Sync + 'static,
        E: Executable + Send + Sync + 'static,
    {
        Self {
            exec: TaskExecutable::Sync(Arc::new(Mutex::new(exec))),
            schedule: Arc::new(RwLock::new(schedule)),
            err_fn: None,
        }
    }

    /// Creates a new task that executes asynchronously.
    ///
    /// The scheduler will spawn a new thread for the task when it is run, and this function provides a means of creating tasks that use `async`/`await`.
    /// If an `async` context is not necessary, please consider using [`Task::new_sync()`] instead.
    ///
    /// This function is not able to create tasks from anonymous functions, as `async` closures are currently an unstable feature.
    /// Creating new tasks from named `async` functions is still supported.
    ///
    /// # Examples
    /// ----------
    ///
    /// Tasks may be created from named functions.
    ///
    /// ```
    /// use rota::*;
    /// use rota::task::*;
    /// use rota::scheduling::*;
    ///
    /// async fn hello_world( handle: TaskId, ct: CancellationToken ) -> TaskResult {
    ///     println!( "Hello, World!" );
    ///     Ok( () )
    /// }
    ///
    /// let once = LimitedRunSchedule::once( AlwaysSchedule::new() );
    /// let task = Task::new_async( once, hello_world );
    /// ```
    /// --------
    ///
    /// Tasks that require persistent state or the ability to share data should be implemented as a `struct`.
    /// ```
    /// use rota::*;
    /// use rota::task::*;
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
    ///     async fn execute( &mut self, handle: TaskId, ct: CancellationToken ) -> TaskResult {
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
    ///     async fn execute( &mut self, handle: TaskId, ct: CancellationToken ) -> TaskResult {
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
    /// let sender_task = Task::new_async( every_5s, sender );
    /// let receiver_task = Task::new_async( every_30s, receiver );
    /// ```
    pub fn new_async<S, E>(schedule: S, exec: E) -> Self
    where
        S: Schedule + Send + Sync + 'static,
        E: AsyncExecutable + Send + Sync + 'static,
    {
        Self {
            exec: TaskExecutable::Async(Arc::new(Mutex::new(exec))),
            schedule: Arc::new(RwLock::new(schedule)),
            err_fn: None,
        }
    }

    pub fn on_error<F>(&mut self, err_fn: F)
    where
        F: FnMut(TaskId, TaskError) + Send + Sync + 'static,
    {
        self.err_fn = Some(Arc::new(Mutex::new(Box::new(err_fn))));
    }
}
