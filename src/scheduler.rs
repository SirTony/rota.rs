use std::{collections::HashMap, time::Duration, error::Error};

use paste::paste;
use tokio::{
    sync::{OnceCell, RwLock, RwLockReadGuard, RwLockWriteGuard},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    ext::FutureEx,
    job::{Job, JobKind, Executable, AsyncExecutable},
    SchedulerError, scheduling::{LimitedRunSchedule, AlwaysSchedule},
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

            pub async fn is_terminated() -> Result<bool, SchedulerError> {
                let x = !is_ready() && is_initialized() && get_scheduler().await?.ct.is_cancelled();

                Ok( x )
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
            ct: CancellationToken::new()
        }
    };

    static ACTIVE_JOBS: HashMap<Uuid, ActiveJob> = { HashMap::new() };

    static JOB_SPAWNER: JoinHandle<Result<(), SchedulerError>> = {
        let ct = get_scheduler().await?.ct.clone();
        let sleep = Duration::from_millis( 1 );

        let spawner = tokio::spawn( async move {
            while !is_cancelled().await? {
                spawn_jobs().await?;
                tokio::time::sleep( sleep ).with_cancellation(ct.clone()).await?;
            }

            Ok(())
        } );

        spawner
    };

    static JANITOR: JoinHandle<Result<(), SchedulerError>> = {
        let ct = get_scheduler().await?.ct.clone();
        let sleep = Duration::from_millis( 1 );

        let janitor = tokio::spawn( async move {
            while !is_cancelled().await? {
                cleanup_jobs().await?;
                tokio::time::sleep( sleep ).with_cancellation(ct.clone()).await?;
            }

            Ok(())
        } );

        janitor
    };

    static ERR_FN: Option<Box<dyn FnMut( JobHandle, Box<dyn Error + Send + Sync> ) + Send + Sync + 'static>> = {
        None
    }
}

pub async fn is_cancelled() -> Result<bool, SchedulerError> {
    let scheduler = get_scheduler().await?;
    Ok(scheduler.ct.is_cancelled())
}

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

pub async fn wait_for_shutdown() -> Result<(), SchedulerError> {
    // it's important to clone the token and then await the cloned token to drop the RwLockReadGuard ASAP
    // otherwise await-ing this function would deadlock the whole scheduler
    let ct = get_scheduler().await?.ct.clone();
    ct.cancelled().await;

    let active = get_active_jobs().await?;
    let spawner = get_job_spawner().await?;
    let janitor = get_janitor().await?;

    let sleep = Duration::from_millis(1);

    while active.len() > 0 {
        tokio::time::sleep(sleep).await;
    }

    while !spawner.is_finished() && !janitor.is_finished() {
        tokio::time::sleep(sleep).await;
    }

    Ok(())
}

pub async fn set_err_fn<F: FnMut( JobHandle, Box<dyn Error + Send + Sync> ) + Send + Sync + 'static>( func: F ) -> Result<(), SchedulerError> {
    let mut err_fn = get_err_fn_mut().await?;
    err_fn.replace(Box::new(func));

    Ok(())
}

/// Adds a new job to the scheduler. Returns the job's id on success.
pub async fn add_job(job: Job) -> Result<JobHandle, SchedulerError> {
    println!("attempting to schedule job");
    let id = generate_job_id().await?;
    let mut scheduler = get_scheduler_mut().await?;

    scheduler.jobs.insert(id.clone(), job);
    println!("adding job {}", id);
    Ok(JobHandle(id))
}

/// Adds an executable job as a background task.
/// Background tasks begin running immediately and run only once.
pub async fn add_background_task_sync<E: Executable + Send + Sync + 'static>( exec: E ) -> Result<JobHandle, SchedulerError> {
    let once = LimitedRunSchedule::once( AlwaysSchedule );
    let job = Job::new_sync(once, exec);

    add_job(job).await
}

/// Adds an executable job as a background task.
/// Background tasks begin running immediately and run only once.
pub async fn add_background_task_async<E: AsyncExecutable + Send + Sync + 'static>( exec: E ) -> Result<JobHandle, SchedulerError> {
    let once = LimitedRunSchedule::once( AlwaysSchedule );
    let job = Job::new_async(once, exec);

    add_job(job).await
}

async fn find_ready_jobs() -> Result<HashMap<Uuid, Job>, SchedulerError> {
    let mut scheduler = get_scheduler_mut().await?;
    let mut ids = Vec::new();

    for (id, job) in scheduler.jobs.iter() {
        if job.schedule.read().await.is_ready() {
            ids.push(id.clone());
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

pub async fn find_job(id: &Uuid) -> Result<JobHandle, SchedulerError> {
    let exists = get_scheduler().await?.jobs.contains_key(id) || get_active_jobs().await?.contains_key(id);

    if exists {
        let handle = JobHandle(id.clone());
        Ok(handle)
    } else {
        Err(SchedulerError::JobNotFound(id.clone()))
    }
}

async fn find_completed_jobs() -> Result<HashMap<Uuid, ActiveJob>, SchedulerError> {
    let mut active = get_active_jobs_mut().await?;

    let mut ids = Vec::new();
    for (id, job) in active.iter() {
        if job.ct.is_cancelled() || job.task.is_finished() {
            ids.push(id.clone());
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
            ids.push(id.clone());
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

    let mut id = Uuid::new_v4();
    while scheduler.jobs.contains_key(&id) || active_jobs.contains_key(&id) {
        id = Uuid::new_v4();
    }

    Ok(id)
}

async fn spawn_jobs() -> Result<(), SchedulerError> {
    let jobs = find_ready_jobs().await?;
    let ct = get_scheduler().await?.ct.child_token();

    for (id, job) in jobs.into_iter() {
        let c_id = id.clone();
        let c_job = job.clone();
        let c_ct = ct.clone();

        let task = tokio::spawn(async move {
            println!("executing job {}", c_id);

            let result = match c_job.kind {
                JobKind::Sync(exec) => {
                    let mut lock = exec.lock().await;
                    lock.execute(c_id, c_ct)
                }
                JobKind::Async(exec) => {
                    let mut lock = exec.lock().await;
                    lock.execute(c_id, c_ct).await
                }
            }
            .map_err(|e| SchedulerError::Internal {
                job: JobHandle(c_id),
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
            Err(SchedulerError::Cancelled) => {},
            Err( SchedulerError::Internal { job, error } ) => {
                let mut err_fn = get_err_fn_mut().await?;
                if let Some( ref mut err_fn ) = *err_fn {
                    (*err_fn)(job, error);
                }
            },

            Err( e ) => return Err( e ),
        }

        let mut scheduler = get_scheduler_mut().await?;
        scheduler.jobs.insert(id, job.job);
    }

    // find all jobs with schedules that are finished so we can drop them
    let jobs = find_dead_jobs().await?;
    for ( id, job ) in jobs.into_iter() {
        println!( "deleting job {}", id );
        drop( job );
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
}

#[derive(Debug)]
pub struct JobHandle(Uuid);

impl std::fmt::Display for JobHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Job({})", self.0 )
    }
}

impl JobHandle {
    /// The unique ID of the job within the scheduler.
    pub fn id(&self) -> &Uuid {
        &self.0
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

    /// Returns the job's current cancellation token if the job is currently executing.
    /// It is generally not useful to store the result of this function as the token is only valid
    /// while the job is executing, and a new token is created each time a job begins exection.
    pub async fn cancellation_token( &self ) -> Result<Option<CancellationToken>, SchedulerError> {
        if let Some(job) = get_active_jobs().await?.get(&self.0) {
            Ok( Some( job.ct.clone() ) )
        } else {
            Ok(None)
        }
    }
}