use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use paste::paste;
use tokio::{
    runtime::Handle,
    sync::{OnceCell, RwLock, RwLockReadGuard, RwLockWriteGuard},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ext::FutureEx,
    job::{AsyncExecutable, Executable, Job, JobKind},
    scheduling::{AlwaysSchedule, LimitedRunSchedule},
    SchedulerError,
};

macro_rules! declare_static {
    ( $(static $name: ident: $inner_type: ty = $init_body: block);+ ) => {
        $(
            paste! {
                static $name: OnceCell<RwLock<$inner_type>> = OnceCell::const_new();

                #[allow(dead_code)]
                async fn [<get_ $name:lower>]<'reader>() -> Result<RwLockReadGuard<'reader, $inner_type>, SchedulerError> {
                    if let Some( [<$name:lower>] ) = $name.get() {
                        Ok( [<$name:lower>] .read().await )
                    } else {
                        Err( SchedulerError::NotInitialized )
                    }
                }

                #[allow(dead_code)]
                async fn [<get_ $name:lower _mut>]<'writer>() -> Result<RwLockWriteGuard<'writer, $inner_type>, SchedulerError> {
                    if let Some( [<$name:lower>]  ) = $name.get() {
                        Ok( [<$name:lower>] .write().await )
                    } else {
                        Err( SchedulerError::NotInitialized )
                    }
                }

                fn [<is_ $name:lower _initialized>]() -> bool {
                    $name.initialized()
                }

                async fn [<init_ $name:lower _impl>]() -> Result<(), SchedulerError> {
                    if [<is_ $name:lower _initialized>]() {
                        Err( SchedulerError::AlreadyInitialized )
                    } else {
                        println!( "initializing {}", stringify!( $name ) );
                        let value = $init_body;

                        match $name.set( RwLock::new( value ) ) {
                            Ok( x ) => Ok( x ),
                            Err( _ ) => unreachable!()
                        }
                    }
                }
            }
        )+

        paste!{
            pub fn is_ready() -> bool {
                let mut is_not_init = true;
                $( is_not_init = is_not_init && ( [<is_ $name:lower _initialized>]() == false ); )+

                is_not_init
            }

            pub fn is_initialized() -> bool {
                let mut is_init = true;
                $( is_init = is_init && [<is_ $name:lower _initialized>](); )+

                is_init
            }

            pub async fn init() -> Result<(), SchedulerError> {
                if is_initialized() {
                    Err( SchedulerError::AlreadyInitialized )
                } else if is_terminated().await? {
                    Err( SchedulerError::Terminated )
                } else {
                    $( [<init_ $name:lower _impl>]().await?; )+
                    println!( "everything is initialized" );
                    Ok(())
                }
            }
        }
    }
}

declare_static! {
    static SCHEDULER: JobScheduler = {
        JobScheduler {
            jobs: HashMap::new(),
            ct: CancellationToken::new(),
            is_running: AtomicBool::new( true ),
        }
    };

    static ACTIVE_JOBS: HashMap<Uuid, ActiveJob> = { HashMap::new() };

    static JOB_SPAWNER: JoinHandle<Result<(), SchedulerError>> = {
        let ct = get_scheduler().await?.ct.clone();
        let sleep = Duration::from_millis( 1 );



        tokio::spawn( async move {
            while !is_cancelled().await? {
                spawn_jobs().await?;
                tokio::time::sleep( sleep ).with_cancellation(ct.clone()).await?;
            }

            Ok(())
        } )
    };

    static JANITOR: JoinHandle<Result<(), SchedulerError>> = {
        let ct = get_scheduler().await?.ct.clone();
        let sleep = Duration::from_millis( 1 );



        tokio::spawn( async move {
            while !is_cancelled().await? {
                cleanup_jobs().await?;
                tokio::time::sleep( sleep ).with_cancellation(ct.clone()).await?;
            }

            Ok(())
        } )
    };

    static STUBS: Vec<Uuid> = { Vec::new() };

    static ERROR_HANDLERS: HashMap<Uuid, Box<dyn FnMut( JobHandle, Box<dyn std::error::Error + Send + Sync> ) + Send + Sync + 'static>> = { HashMap::new() }
}

pub async fn is_cancelled() -> Result<bool, SchedulerError> {
    Ok(get_scheduler().await?.ct.is_cancelled())
}

pub async fn is_terminated() -> Result<bool, SchedulerError> {
    Ok(!is_ready() && is_initialized() && is_cancelled().await?)
}

/// Shuts down the scheduler, cancels all currently running jobs, and frees the jobs from memory.
/// The future returned by this function resolves once the scheduler and all active jobs have stopped, so `await`-ing [wait_for_shutdown()] is not necessary.
/// **Once the scheduler has been shut down, it may not be re-used.** The scheduler cannot be re-initialized, and any calls to scheduler functions will result in an error after thins function is called.
/// If temporarily suspending the scheduler is desired, [pause()] should be used instead.
pub async fn shutdown() -> Result<(), SchedulerError> {
    get_scheduler().await?.ct.cancel();

    for (_, job) in get_active_jobs_mut().await?.drain() {
        job.ct.cancel();
        job.task.abort();

        // intentionall ignore the result.
        // we don't care if we get a JoinError because the job
        // is about to be dropped anyway
        let _ = job.task.await;
    }

    get_job_spawner().await?.abort();
    get_janitor().await?.abort();
    wait_for_shutdown().await?;

    get_scheduler_mut().await?.jobs.clear();

    Ok(())
}

/// The future returned by this function resolves once the scheduler and all jobs have stopped.
/// This may be useful for command line applications that need to keep the process alive while the scheduler does its job.
pub async fn wait_for_shutdown() -> Result<(), SchedulerError> {
    // it's important to clone the token and then await the cloned token to drop the RwLockReadGuard ASAP
    // otherwise await-ing this function would deadlock the whole scheduler
    let ct = get_scheduler().await?.ct.clone();
    ct.cancelled().await;

    let sleep = Duration::from_millis(1);

    while get_active_jobs().await?.len() > 0 {
        tokio::time::sleep(sleep).await;
    }

    while !get_job_spawner().await?.is_finished() && !get_janitor().await?.is_finished() {
        tokio::time::sleep(sleep).await;
    }

    Ok(())
}

/// Temporarily suspends the scheduler and prevents any jobs from being executed.
/// Jobs that are already running will not be cancelled.
/// This will not completely suspend all operation of the scheduler, as some internal bookkeeping will still take place,
/// such as cleaning up jobs. Pausing the scheduler only stops jobs from being run.
pub async fn pause() -> Result<(), SchedulerError> {
    get_scheduler_mut()
        .await?
        .is_running
        .store(false, Ordering::SeqCst);
    Ok(())
}

/// Resumes normal operation of the scheduler.
pub async fn resume() -> Result<(), SchedulerError> {
    get_scheduler_mut()
        .await?
        .is_running
        .store(true, Ordering::SeqCst);
    Ok(())
}

/// Returns whether or not the scheduler is currently paused.
pub async fn is_paused() -> Result<bool, SchedulerError> {
    Ok(get_scheduler_mut().await?.is_running.load(Ordering::SeqCst))
}

/// Adds a new job to the scheduler. Returns the job's id on success.
pub async fn add_job(job: Job) -> Result<JobHandle, SchedulerError> {
    println!("attempting to schedule job");
    let id = generate_job_id().await?;
    let mut scheduler = get_scheduler_mut().await?;

    scheduler.jobs.insert(id, job);
    println!("adding job {}", id);
    Ok(JobHandle(id))
}

/// Adds an executable job as a background task.
/// Background tasks begin running immediately and run only once.
pub async fn add_background_task_sync<E: Executable + Send + Sync + 'static>(
    exec: E,
) -> Result<JobHandle, SchedulerError> {
    let once = LimitedRunSchedule::once(AlwaysSchedule);
    let job = Job::new_sync(once, exec);

    add_job(job).await
}

/// Adds an executable job as a background task.
/// Background tasks begin running immediately and run only once.
pub async fn add_background_task_async<E: AsyncExecutable + Send + Sync + 'static>(
    exec: E,
) -> Result<JobHandle, SchedulerError> {
    let once = LimitedRunSchedule::once(AlwaysSchedule);
    let job = Job::new_async(once, exec);

    add_job(job).await
}

/// Creates a stub for a job that can then be used to register the job with the scheduler.
/// This reserves a unique ID and allows a handle for the job to be created before the job is actually registered,
/// which may be useful for pre-configuration.
pub async fn create_stub() -> Result<JobStub, SchedulerError> {
    let id = generate_job_id().await?;
    let mut stubs = get_stubs_mut().await?;

    stubs.push(id);

    Ok(JobStub(id))
}

async fn find_ready_jobs() -> Result<HashMap<Uuid, Job>, SchedulerError> {
    let mut scheduler = get_scheduler_mut().await?;
    let mut ids = Vec::new();

    for (id, job) in scheduler.jobs.iter() {
        if job.schedule.read().await.is_ready() {
            ids.push(*id);
        }
    }

    let mut map = HashMap::new();
    for id in ids.into_iter() {
        if let Some((id, job)) = scheduler.jobs.remove_entry(&id) {
            map.insert(id, job);
        }
    }

    Ok(map)
}

/// Attempts to find a job being handled by the scheduler.
pub async fn find_job(id: &Uuid) -> Result<JobHandle, SchedulerError> {
    if get_scheduler().await?.jobs.contains_key(id) || get_active_jobs().await?.contains_key(id) {
        let handle = JobHandle(*id);
        Ok(handle)
    } else {
        Err(SchedulerError::JobNotFound(*id))
    }
}

async fn find_completed_jobs() -> Result<HashMap<Uuid, ActiveJob>, SchedulerError> {
    let mut active = get_active_jobs_mut().await?;

    let mut ids = Vec::new();
    for (id, job) in active.iter() {
        if job.ct.is_cancelled() || job.task.is_finished() {
            ids.push(*id);
        }
    }

    let mut map = HashMap::new();
    for id in ids.into_iter() {
        if let Some((id, job)) = active.remove_entry(&id) {
            map.insert(id, job);
        }
    }

    Ok(map)
}

async fn find_dead_jobs() -> Result<HashMap<Uuid, Job>, SchedulerError> {
    let mut scheduler = get_scheduler_mut().await?;
    let mut ids = Vec::new();

    for (id, job) in scheduler.jobs.iter() {
        if job.schedule.read().await.is_finished() {
            ids.push(*id);
        }
    }

    let mut map = HashMap::new();
    for id in ids.into_iter() {
        if let Some((id, job)) = scheduler.jobs.remove_entry(&id) {
            map.insert(id, job);
        }
    }

    Ok(map)
}

async fn generate_job_id() -> Result<Uuid, SchedulerError> {
    let scheduler = get_scheduler().await?;
    let active_jobs = get_active_jobs().await?;
    let stubs = get_stubs().await?;

    let mut id = Uuid::new_v4();
    while scheduler.jobs.contains_key(&id) || active_jobs.contains_key(&id) || stubs.contains(&id) {
        id = Uuid::new_v4();
    }

    Ok(id)
}

async fn spawn_jobs() -> Result<(), SchedulerError> {
    let jobs = find_ready_jobs().await?;
    let ct = get_scheduler().await?.ct.child_token();

    for (id, job) in jobs.into_iter() {
        if get_scheduler_mut().await?.is_running.load(Ordering::SeqCst) {
            let c_job = job.clone();
            let c_ct = ct.clone();

            let task = tokio::spawn(async move {
                println!("executing job {}", id);
                let handle = JobHandle(id);

                let result = match c_job.kind {
                    JobKind::Sync(exec) => {
                        let mut lock = exec.lock().await;
                        lock.execute(handle, c_ct)
                    }
                    JobKind::Async(exec) => {
                        let mut lock = exec.lock().await;
                        lock.execute(handle, c_ct).await
                    }
                }
                .map_err(|e| SchedulerError::Internal {
                    job: JobHandle(id),
                    error: e,
                });

                c_job.schedule.write().await.advance();

                result
            });

            let job = ActiveJob {
                job,
                ct: ct.clone(),
                task,
            };
            get_active_jobs_mut().await?.insert(id, job);
        } else {
            // WHATIF - should we advance the schedules while the scheduler is paused?
            //          on the one hand, while the scheduler is paused, it should be paused
            //          on the other hand, if it paused for a significant period of time, some schedules (like Cron) may fire rapidly multiple times once the scheduler resumes
            job.schedule.write().await.advance();
        }
    }

    Ok(())
}

async fn cleanup_jobs() -> Result<(), SchedulerError> {
    let jobs = find_completed_jobs().await?;
    let ct = get_scheduler().await?.ct.clone();

    // move completed jobs back into the scheduler
    for (id, job) in jobs.into_iter() {
        match job.task.with_cancellation(ct.clone()).await {
            Ok(_) => {}
            Err(SchedulerError::Cancelled) => {}
            Err(SchedulerError::Internal { job, error }) => {
                let mut handlers = get_error_handlers_mut().await?;
                let handler = handlers.get_mut(&id);
                if let Some(err_fn) = handler {
                    (*err_fn)(job, error);
                }
            }

            Err(e) => return Err(e),
        }

        let mut scheduler = get_scheduler_mut().await?;
        scheduler.jobs.insert(id, job.job);
    }

    // find all jobs with schedules that are finished so we can drop them
    let jobs = find_dead_jobs().await?;
    let mut handlers = get_error_handlers_mut().await?;
    for (id, job) in jobs.into_iter() {
        println!("deleting job {}", id);
        drop(job);
        handlers.remove(&id);
    }

    Ok(())
}

struct ActiveJob {
    task: JoinHandle<Result<(), SchedulerError>>,
    ct: CancellationToken,
    job: Job,
}

struct JobScheduler {
    jobs: HashMap<Uuid, Job>,
    ct: CancellationToken,
    is_running: AtomicBool,
}

#[derive(Debug)]
pub struct JobStub(Uuid);

impl Drop for JobStub {
    fn drop(&mut self) {
        with(get_stubs_mut(), move |mut stubs| {
            if let Some(idx) = stubs.iter().position(|x| *x == self.0) {
                with(get_error_handlers_mut(), move |mut handlers| {
                    handlers.remove(&self.0);
                });

                stubs.remove(idx);
            }
        });

        fn with<Fut, Fun, T, E>(future: Fut, func: Fun)
        where
            Fut: Future<Output = Result<T, E>>,
            Fun: FnOnce(T),
            E: Error,
        {
            let handle = Handle::current();
            let guard = handle.enter();

            if let Ok(output) = handle.block_on(future) {
                func(output);
            }

            drop(guard);
        }
    }
}

impl JobStub {
    pub fn as_handle(&self) -> JobHandle {
        JobHandle(self.0)
    }

    pub async fn on_error<F>(&self, func: F) -> Result<(), SchedulerError>
    where
        F: FnMut(JobHandle, Box<dyn Error + Send + Sync>) + Send + Sync + 'static,
    {
        let mut handlers = get_error_handlers_mut().await?;
        handlers.insert(self.0, Box::new(func));

        Ok(())
    }

    pub async fn register(self, job: Job) -> Result<(), SchedulerError> {
        let mut scheduler = get_scheduler_mut().await?;
        let mut stubs = get_stubs_mut().await?;

        scheduler.jobs.insert(self.0, job);
        if let Some(idx) = stubs.iter().position(|x| *x == self.0) {
            stubs.remove(idx);
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct JobHandle(Uuid);

impl std::fmt::Display for JobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Job({})", self.0)
    }
}

impl JobHandle {
    /// The unique ID of the job within the scheduler.
    pub fn id(&self) -> &Uuid {
        &self.0
    }

    /// Determines if this handle is still valid.
    /// The handle is considered valid if the job is currently running, or waiting to be run.
    /// The handle is considered invalid if the job's schedule has finished or the job has been manually removed from the scheduler.
    pub async fn is_valid(&self) -> Result<bool, SchedulerError> {
        Ok(get_active_jobs().await?.contains_key(&self.0)
            || get_scheduler().await?.jobs.contains_key(&self.0))
    }

    /// Determines whether or not the job is currently running.
    pub async fn is_running(&self) -> Result<bool, SchedulerError> {
        Ok(get_active_jobs().await?.contains_key(&self.0))
    }

    /// Determines whether or not the job is not currently running, but is ready to be run.
    pub async fn is_ready(&self) -> Result<bool, SchedulerError> {
        if let Some(job) = get_scheduler().await?.jobs.get(&self.0) {
            let lock = job.schedule.read().await;
            Ok(lock.is_ready())
        } else {
            Ok(false)
        }
    }

    /// Determines if cancellation has been requested.
    /// If the job is currently running, this function indicates whether or not the job itself has been cancelled.
    /// If the job is not currently running, this function indicates whether or not the scheduler has been cancelled, which would also signal cancellation for all jobs.
    pub async fn is_cancelled(&self) -> Result<bool, SchedulerError> {
        if let Some(job) = get_active_jobs().await?.get(&self.0) {
            Ok(job.ct.is_cancelled())
        } else {
            Ok(get_scheduler().await?.ct.is_cancelled())
        }
    }

    /// Cancels the job if it's currently running, otherwise do nothing.
    pub async fn cancel(&self) -> Result<(), SchedulerError> {
        if let Some(job) = get_active_jobs().await?.get(&self.0) {
            job.ct.cancel();
            job.task.abort();
        }

        Ok(())
    }

    /// Removes the job from the scheduler, immediately cancelling the job if it's currently running.
    /// This will drop the job and remove it from memory.
    pub async fn delete(&self) -> Result<(), SchedulerError> {
        while self.is_valid().await? {
            if self.is_running().await? {
                self.cancel().await?;
                self.wait_for_exit().await?;
                let mut active = get_active_jobs_mut().await?;
                active.remove(&self.0);
            } else {
                let mut scheduler = get_scheduler_mut().await?;
                scheduler.jobs.remove(&self.0);
            }
        }

        Ok(())
    }

    /// Returns the job's current cancellation token if the job is currently executing.
    /// It is generally not useful to store the result of this function as the token is only valid
    /// while the job is executing, and a new token is created each time a job begins exection.
    pub async fn cancellation_token(&self) -> Result<Option<CancellationToken>, SchedulerError> {
        if let Some(job) = get_active_jobs().await?.get(&self.0) {
            Ok(Some(job.ct.clone()))
        } else {
            Ok(None)
        }
    }

    /// Returns a future that resolves when the job has finished running.
    pub async fn wait_for_exit(&self) -> Result<(), SchedulerError> {
        let sleep = Duration::from_millis(1);
        while let Some(ct) = self.cancellation_token().await? {
            if !self.is_running().await? {
                break;
            }
            tokio::time::sleep(sleep).with_cancellation(ct).await?;
        }

        Ok(())
    }
}
