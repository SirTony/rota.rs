use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{debug, trace};

use tokio::{select, sync::RwLock, task::JoinHandle};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use uuid::Uuid;

use crate::task::{Activity, Id, Task};

#[cfg(feature = "global")]
use tokio::sync::{OnceCell, RwLockReadGuard, RwLockWriteGuard};

#[cfg(feature = "global")]
static GLOBAL_SCHEDULER: OnceCell<RwLock<Scheduler>> = OnceCell::const_new();

#[cfg(feature = "global")]
fn ensure_initialized() {
    if !GLOBAL_SCHEDULER.initialized() {
        let _ = GLOBAL_SCHEDULER.set(RwLock::new(Scheduler::new()));
    }
}

#[cfg(feature = "global")]
pub async fn get_scheduler<'read>() -> RwLockReadGuard<'read, Scheduler> {
    ensure_initialized();
    GLOBAL_SCHEDULER.get().unwrap().read().await
}

#[cfg(feature = "global")]
pub async fn get_scheduler_mut<'write>() -> RwLockWriteGuard<'write, Scheduler> {
    ensure_initialized();
    GLOBAL_SCHEDULER.get().unwrap().write().await
}

pub struct Scheduler {
    tasks: Arc<RwLock<HashMap<Uuid, Task>>>,
    ct: CancellationToken,
    is_running: Arc<AtomicBool>,
    spawner: Option<JoinHandle<()>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    const BACKGROUND_LOOP_DELAY: Duration = Duration::from_millis(10);

    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            ct: CancellationToken::new(),
            is_running: Arc::new(AtomicBool::new(false)),
            spawner: None,
        }
    }

    pub async fn add_task(&mut self, task: Task) -> Id {
        let id = self.generate_task_id().await;
        trace!("adding task {}", id);
        self.tasks.write().await.insert(id, task);

        Id(id)
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    #[allow(clippy::let_underscore_future)]
    pub async fn start(&mut self) {
        if !self.is_running() {
            trace!("Scheduler::start()");
            if self.ct.is_cancelled() {
                self.ct = CancellationToken::new();
            }
        }

        let ct = self.ct.clone();
        let is_running = self.is_running.clone();
        let tasks = self.tasks.clone();
        let child = self.ct.child_token();

        let spawner = tokio::spawn(async move {
            trace!("launching spawner");
            while !ct.is_cancelled() {
                if is_running.load(Ordering::SeqCst) {
                    let mut tasks = tasks.write().await;
                    let mut to_remove = Vec::new();

                    for (id, task) in tasks.iter_mut() {
                        match task.status().await {
                            crate::task::Status::Invalid => {
                                to_remove.push(*id);
                            }
                            crate::task::Status::Paused => {
                                // continue ticking the schedule while paused
                                let mut sched = task.schedule.write().await;
                                if sched.is_ready().await {
                                    sched.advance().await;
                                }
                            }
                            crate::task::Status::Ready => {
                                debug!("spawning task {}", id);

                                let task = task.clone();
                                let mut task2 = task.clone();
                                let child = child.clone();
                                let child2 = child.clone();
                                let handle = tokio::spawn(async move {
                                    match task
                                        .exec
                                        .lock()
                                        .await
                                        .execute(task.clone(), child.clone())
                                        .await
                                    {
                                        Ok(_) => {}
                                        Err(e) => {
                                            if let Some(ref callback) = task.err_fn {
                                                callback
                                                    .lock()
                                                    .await
                                                    .on_error(task.clone(), e)
                                                    .await;
                                            }
                                        }
                                    };
                                });

                                task2.activity = Some(Arc::new(Activity { handle, ct: child2 }));
                            }
                            crate::task::Status::Active => { /* do nothing */ }
                            crate::task::Status::Finished => {
                                std::mem::drop(task.activity.take());
                                task.schedule.write().await.advance().await;
                            }
                            crate::task::Status::Cancelled => {
                                if let Some(activity) = &task.activity {
                                    activity.ct.cancel();
                                    activity.handle.abort();
                                }

                                std::mem::drop(task.activity.take());
                            }
                            crate::task::Status::AwaitingRemoval => {
                                if let Some(activity) = &task.activity {
                                    activity.ct.cancel();
                                    activity.handle.abort();
                                }

                                std::mem::drop(task.activity.take());
                                to_remove.push(*id);
                            }
                        }
                    }

                    for id in to_remove {
                        if let Some(mut task) = tasks.remove(&id) {
                            if let Some(activity) = task.activity.take() {
                                activity.ct.cancel();
                                activity.handle.abort();
                            }

                            std::mem::drop(task);
                        }
                    }
                }

                select! {
                    _ = tokio::time::sleep(Self::BACKGROUND_LOOP_DELAY) => { continue; },
                    _ = ct.cancelled() => { break; }
                }
            }

            trace!("terminating spawner");
        });

        let _ = self.spawner.insert(spawner);
        self.is_running.store(true, Ordering::SeqCst);
    }

    pub async fn stop(&mut self) {
        if self.is_running() {
            trace!("Scheduler::stop()");
            self.is_running.store(false, Ordering::SeqCst);
            if let Some(handle) = self.spawner.take() {
                handle.abort();
            }
        }
    }

    pub async fn shutdown(&mut self) {
        trace!("Scheduler::shutdown()");
        self.stop().await;
        self.ct.cancel();

        for (_, mut task) in self.tasks.write().await.drain() {
            task.cancel();
            drop(task);
        }
    }

    pub fn wait_for_exit(&self) -> WaitForCancellationFutureOwned {
        trace!("Scheduler::wait_for_exit()");
        self.ct.clone().cancelled_owned()
    }

    pub async fn get_task(&self, Id(id): Id) -> Option<Task> {
        self.tasks.read().await.get(&id).cloned()
    }

    pub async fn find_task_by_name<S: AsRef<str>>(&self, name: S) -> Option<Task> {
        let name = name.as_ref();
        self.tasks
            .read()
            .await
            .values()
            .filter(|x| x.name.is_some())
            .find(|task| task.name.as_ref().unwrap().as_str() == name)
            .cloned()
    }

    async fn generate_task_id(&self) -> Uuid {
        let mut id = Uuid::new_v4();

        while self.tasks.read().await.contains_key(&id) {
            id = Uuid::new_v4();
        }

        id
    }
}
