use std::{
    collections::HashMap,
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

use crate::scheduling::{AsyncSchedule, SyncSchedule};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// The result type for all tasks in the scheduler.
/// Tasks may not return data, but their errors must be bubbled up to the scheduler to be handled.
pub type Result = std::result::Result<(), Error>;

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the task scheduler.
/// This trait is for tasks that contain no `async` code. If an `async` context is needed, implement [AsyncExecutable] instead.
pub trait SyncExecutable {
    /// Contains the logic of the task that will be run by the scheduler at the appropriate time.
    /// # Arguments
    fn execute(&mut self, id: Id, ct: CancellationToken) -> Result;
}

/// Implementing this for any arbitrary `struct` or `enum` will allow it to be added to the task scheduler.
/// This trait is for tasks that contain `async` code. If an `async` context is not needed, implement [SyncExecutable] instead.
#[async_trait]
pub trait AsyncExecutable {
    async fn execute(&mut self, id: Id, ct: CancellationToken) -> Result;
}

impl<F: 'static + Send + Sync + FnMut(Id, CancellationToken) -> Result> SyncExecutable for F {
    fn execute(&mut self, id: Id, ct: CancellationToken) -> Result {
        (*self)(id, ct)
    }
}

#[async_trait]
impl<Fun, Fut> AsyncExecutable for Fun
where
    Fun: 'static + Send + Sync + FnMut(Id, CancellationToken) -> Fut,
    Fut: Future<Output = Result> + Send + Sync,
{
    async fn execute(&mut self, id: Id, ct: CancellationToken) -> Result {
        let c_ct = ct.clone();

        select! {
            task_result = (*self)(id, ct) => task_result,
            _ = c_ct.cancelled() => Err( crate::Error::Cancelled.into() ),
        }
    }
}

#[derive(Clone)]
pub(crate) enum TaskExecutable {
    Sync(Arc<Mutex<dyn SyncExecutable + Send + Sync>>),
    Async(Arc<Mutex<dyn AsyncExecutable + Send + Sync>>),
}

impl TaskExecutable {
    pub async fn execute(&mut self, id: Id, ct: CancellationToken) -> Result {
        match self {
            TaskExecutable::Sync(x) => x.lock().await.execute(id, ct),
            TaskExecutable::Async(x) => x.lock().await.execute(id, ct).await,
        }
    }
}

pub trait SyncErrorCallback {
    fn on_error(&mut self, id: Id, e: Error);
}

impl<Fun> SyncErrorCallback for Fun
where
    Fun: FnMut(Id, Error),
{
    fn on_error(&mut self, id: Id, e: Error) {
        (*self)(id, e)
    }
}

#[async_trait]
pub trait AsyncErrorCallback {
    async fn on_error(&mut self, id: Id, e: Error);
}

#[async_trait]
impl<Fun, Fut> AsyncErrorCallback for Fun
where
    Fun: Send + Sync + 'static + FnMut(Id, Error) -> Fut,
    Fut: Send + Sync + 'static + Future<Output = ()>,
{
    async fn on_error(&mut self, id: Id, e: Error) {
        (*self)(id, e).await
    }
}

#[derive(Clone)]
pub(crate) enum TaskErrorFn {
    Sync(Arc<Mutex<dyn SyncErrorCallback + Send + Sync>>),
    Async(Arc<Mutex<dyn AsyncErrorCallback + Send + Sync>>),
}

impl TaskErrorFn {
    pub async fn on_error(&mut self, id: Id, e: Error) {
        match self {
            TaskErrorFn::Sync(x) => x.lock().await.on_error(id, e),
            TaskErrorFn::Async(x) => x.lock().await.on_error(id, e).await,
        }
    }
}

#[derive(Clone)]
pub(crate) enum TaskSchedule {
    Sync(Arc<RwLock<dyn SyncSchedule + Send + Sync>>),
    Async(Arc<RwLock<dyn AsyncSchedule + Send + Sync>>),
}

impl TaskSchedule {
    // pub async fn next(&self) -> Option<DateTime<Utc>> {
    //     match self {
    //         TaskSchedule::Sync(x) => x.read().await.next(),
    //         TaskSchedule::Async(x) => x.read().await.next().await,
    //     }
    // }

    pub async fn advance(&mut self) {
        match self {
            TaskSchedule::Sync(x) => x.write().await.advance(),
            TaskSchedule::Async(x) => x.write().await.advance().await,
        }
    }

    pub async fn is_ready(&self) -> bool {
        match self {
            TaskSchedule::Sync(x) => x.read().await.is_ready(),
            TaskSchedule::Async(x) => x.read().await.is_ready().await,
        }
    }

    pub async fn is_finished(&self) -> bool {
        match self {
            TaskSchedule::Sync(x) => x.read().await.is_finished(),
            TaskSchedule::Async(x) => x.read().await.is_finished().await,
        }
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
pub struct TaskBuilder<
    const __HAS_EXEC: bool = false,
    const __HAS_SCHEDULE: bool = false,
    const __HAS_ERR_FN: bool = false,
    const __HAS_NAME: bool = false,
> {
    exec: Option<TaskExecutable>,
    schedule: Option<TaskSchedule>,
    err_fn: Option<TaskErrorFn>,
    name: Option<String>,
}

impl<const __HAS_SCHEDULE: bool, const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<false, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn sync_executable<E: SyncExecutable + Send + Sync + 'static>(
        self,
        exec: E,
    ) -> TaskBuilder<true, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME> {
        TaskBuilder::<true, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME> {
            exec: Some(TaskExecutable::Sync(Arc::new(Mutex::new(exec)))),
            schedule: self.schedule,
            err_fn: self.err_fn,
            name: self.name,
        }
    }

    pub fn async_executable<E: AsyncExecutable + Send + Sync + 'static>(
        self,
        exec: E,
    ) -> TaskBuilder<true, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME> {
        TaskBuilder::<true, __HAS_SCHEDULE, __HAS_ERR_FN, __HAS_NAME> {
            exec: Some(TaskExecutable::Async(Arc::new(Mutex::new(exec)))),
            schedule: self.schedule,
            err_fn: self.err_fn,
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<__HAS_EXEC, false, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn sync_schedule<S: SyncSchedule + Send + Sync + 'static>(
        self,
        schedule: S,
    ) -> TaskBuilder<__HAS_EXEC, true, __HAS_ERR_FN, __HAS_NAME> {
        TaskBuilder::<__HAS_EXEC, true, __HAS_ERR_FN, __HAS_NAME> {
            exec: self.exec,
            schedule: Some(TaskSchedule::Sync(Arc::new(RwLock::new(schedule)))),
            err_fn: self.err_fn,
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<__HAS_EXEC, false, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn async_schedule<S: AsyncSchedule + Send + Sync + 'static>(
        self,
        schedule: S,
    ) -> TaskBuilder<__HAS_EXEC, true, __HAS_ERR_FN, __HAS_NAME> {
        TaskBuilder::<__HAS_EXEC, true, __HAS_ERR_FN, __HAS_NAME> {
            exec: self.exec,
            schedule: Some(TaskSchedule::Async(Arc::new(RwLock::new(schedule)))),
            err_fn: self.err_fn,
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_SCHEDULE: bool, const __HAS_NAME: bool>
    TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, false, __HAS_NAME>
{
    // TODO - we need an async variant of this
    pub fn on_error_sync<Fun>(
        self,
        f: Fun,
    ) -> TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, true, __HAS_NAME>
    where
        Fun: SyncErrorCallback + Send + Sync + 'static,
    {
        TaskBuilder::<__HAS_EXEC, __HAS_SCHEDULE, true, __HAS_NAME> {
            exec: self.exec,
            schedule: self.schedule,
            err_fn: Some(TaskErrorFn::Sync(Arc::new(Mutex::new(f)))),
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_SCHEDULE: bool, const __HAS_NAME: bool>
    TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, false, __HAS_NAME>
{
    // TODO - we need an async variant of this
    pub fn on_error_async<Fun, Fut>(
        self,
        f: Fun,
    ) -> TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, true, __HAS_NAME>
    where
        Fun: AsyncErrorCallback + Send + Sync + 'static,
    {
        TaskBuilder::<__HAS_EXEC, __HAS_SCHEDULE, true, __HAS_NAME> {
            exec: self.exec,
            schedule: self.schedule,
            err_fn: Some(TaskErrorFn::Async(Arc::new(Mutex::new(f)))),
            name: self.name,
        }
    }
}

impl<const __HAS_EXEC: bool, const __HAS_SCHEDULE: bool, const __HAS_ERR_FN: bool>
    TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, __HAS_ERR_FN, false>
{
    pub fn name<S: ToString>(
        self,
        name: S,
    ) -> TaskBuilder<__HAS_EXEC, __HAS_SCHEDULE, __HAS_ERR_FN, true> {
        TaskBuilder::<__HAS_EXEC, __HAS_SCHEDULE, __HAS_ERR_FN, true> {
            exec: self.exec,
            schedule: self.schedule,
            err_fn: self.err_fn,
            name: Some(name.to_string()),
        }
    }
}

impl<const __HAS_ERR_FN: bool, const __HAS_NAME: bool>
    TaskBuilder<true, true, __HAS_ERR_FN, __HAS_NAME>
{
    pub fn build(self) -> ScheduledTask {
        ScheduledTask {
            exec: self
                .exec
                .expect("the maintainer forgot to set the exec field"),
            schedule: self
                .schedule
                .expect("the maintainer forgot to set the schedule field"),
            err_fn: self.err_fn,
            name: self.name.map(Arc::new),
            is_paused: Arc::new(AtomicBool::new(false)),
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
pub struct ScheduledTask {
    pub(crate) exec: TaskExecutable,
    pub(crate) schedule: TaskSchedule,
    pub(crate) err_fn: Option<TaskErrorFn>,
    pub(crate) name: Option<Arc<String>>,
    pub(crate) is_paused: Arc<AtomicBool>,
}

impl ScheduledTask {
    pub fn builder() -> TaskBuilder {
        TaskBuilder {
            exec: None,
            schedule: None,
            err_fn: None,
            name: None,
        }
    }
}

impl ScheduledTask {
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

pub(crate) struct ActiveTask {
    pub handle: JoinHandle<()>,
    pub ct: CancellationToken,
    pub task: ScheduledTask,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct Id(pub(crate) Uuid);

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task({})", self.0)
    }
}

impl From<Handle> for Id {
    fn from(value: Handle) -> Self {
        Self(value.id)
    }
}

#[derive(Clone)]
pub struct Handle {
    pub(crate) id: Uuid,
    pub(crate) waiting: Arc<RwLock<HashMap<Uuid, ScheduledTask>>>,
    pub(crate) active: Arc<RwLock<HashMap<Uuid, ActiveTask>>>,
}

impl Handle {
    pub async fn name(&self) -> Option<Arc<String>> {
        if let Some(task) = self.waiting.read().await.get(&self.id) {
            task.name.clone()
        } else if let Some(active) = self.active.read().await.get(&self.id) {
            active.task.name.clone()
        } else {
            None
        }
    }

    pub async fn is_valid(&self) -> bool {
        self.waiting.read().await.contains_key(&self.id)
            || self.active.read().await.contains_key(&self.id)
    }

    pub async fn is_ready(&self) -> bool {
        if let Some(task) = self.waiting.read().await.get(&self.id) {
            task.schedule.is_ready().await
        } else {
            false
        }
    }

    pub async fn is_waiting(&self) -> bool {
        self.waiting.read().await.contains_key(&self.id)
    }

    pub async fn is_active(&self) -> bool {
        self.active.read().await.contains_key(&self.id)
    }

    pub async fn is_paused(&self) -> bool {
        if let Some(task) = self.waiting.read().await.get(&self.id) {
            task.is_paused.load(Ordering::SeqCst)
        } else if let Some(active) = self.active.read().await.get(&self.id) {
            active.task.is_paused.load(Ordering::SeqCst)
        } else {
            true
        }
    }

    pub async fn pause(&self) {
        if let Some(task) = self.waiting.read().await.get(&self.id) {
            task.is_paused.store(true, Ordering::SeqCst)
        } else if let Some(active) = self.active.read().await.get(&self.id) {
            active.task.is_paused.store(true, Ordering::SeqCst)
        }
    }

    pub async fn resume(&self) {
        if let Some(task) = self.waiting.read().await.get(&self.id) {
            task.is_paused.store(false, Ordering::SeqCst)
        } else if let Some(active) = self.active.read().await.get(&self.id) {
            active.task.is_paused.store(false, Ordering::SeqCst)
        }
    }

    pub async fn cancel(&mut self) {
        if let Some(active) = self.active.write().await.get(&self.id) {
            active.ct.cancel();
            active.handle.abort();
        }
    }

    pub async fn remove(&mut self) {
        self.pause().await;
        self.cancel().await;

        self.waiting.write().await.remove(&self.id);
        self.active.write().await.remove(&self.id);
    }

    pub fn downgrade(self) -> Id {
        self.into()
    }
}
