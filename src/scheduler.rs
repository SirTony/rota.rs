use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use log::{debug, trace};

use tokio::{sync::RwLock, task::JoinHandle};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use uuid::Uuid;

use crate::{
    ext::FutureEx,
    task::{ActiveTask, Handle, Id, ScheduledTask},
    Result,
};

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
    waiting_tasks: Arc<RwLock<HashMap<Uuid, ScheduledTask>>>,
    active_tasks: Arc<RwLock<HashMap<Uuid, ActiveTask>>>,
    ct: CancellationToken,
    is_running: Arc<AtomicBool>,
    spawner: Option<JoinHandle<Result<()>>>,
    cleaner: Option<JoinHandle<Result<()>>>,
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
            waiting_tasks: Arc::new(RwLock::new(HashMap::new())),
            active_tasks: Arc::new(RwLock::new(HashMap::new())),
            ct: CancellationToken::new(),
            is_running: Arc::new(AtomicBool::new(false)),
            spawner: None,
            cleaner: None,
        }
    }

    pub async fn add_task(&mut self, task: ScheduledTask) -> Id {
        let id = self.generate_task_id().await;
        trace!("adding task {}", id);
        self.waiting_tasks.write().await.insert(id, task);

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
            let tasks = self.waiting_tasks.clone();
            let tasks2 = self.waiting_tasks.clone();
            let active = self.active_tasks.clone();
            let active2 = self.active_tasks.clone();
            let ct = self.ct.clone();
            let ct2 = ct.clone();
            let is_running = self.is_running.clone();
            let is_running2 = is_running.clone();

            let spawner = tokio::spawn(async move {
                trace!("launching spawner");
                while !ct.is_cancelled() {
                    if is_running.load(Ordering::SeqCst) {
                        let mut tasks = tasks.write().await;
                        let mut active = active.write().await;

                        let ids = {
                            let mut ids = Vec::new();

                            for (id, task) in tasks.iter() {
                                if !task.is_paused.load(Ordering::SeqCst)
                                    && task.schedule.is_ready().await
                                {
                                    ids.push(*id);
                                }
                            }

                            ids
                        };

                        if !ids.is_empty() {
                            debug!("getting ready to spawn {} tasks", ids.len())
                        }

                        for id in ids.into_iter() {
                            debug!("spawning task {}", id);
                            let task_id = Id(id);
                            let ct = ct.child_token();
                            let ct2 = ct.clone();
                            if let Some((id, task)) = tasks.remove_entry(&id) {
                                let mut exec = task.exec.clone();
                                let task2 = task.clone();
                                let handle = tokio::spawn(async move {
                                    match exec.execute(task_id, ct).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            if let Some(mut callback) = task2.err_fn {
                                                callback.on_error(task_id, e).await;
                                            }
                                        }
                                    };
                                });

                                let task = ActiveTask {
                                    task,
                                    handle,
                                    ct: ct2,
                                };
                                active.insert(id, task);
                            }
                        }
                    }

                    tokio::time::sleep(Self::BACKGROUND_LOOP_DELAY)
                        .with_cancellation(ct.clone())
                        .await?;
                }

                trace!("terminating spawner");
                Ok(())
            });

            let cleaner = tokio::spawn(async move {
                trace!("launching cleaner");
                while !ct2.is_cancelled() {
                    if is_running2.load(Ordering::SeqCst) {
                        let mut tasks = tasks2.write().await;
                        let mut active = active2.write().await;

                        let ids = {
                            let mut ids = Vec::new();

                            for (id, task) in active.iter() {
                                if task.handle.is_finished() {
                                    ids.push(*id);
                                }
                            }

                            ids
                        };

                        if !ids.is_empty() {
                            debug!("preparing to cleanup {} tasks", ids.len());
                        }

                        for id in ids.into_iter() {
                            debug!("cleaning up task {}", id);
                            if let Some(mut task) = active.remove(&id) {
                                if task.task.schedule.is_finished().await {
                                    std::mem::drop(task);
                                } else {
                                    task.task.schedule.advance().await;
                                    tasks.insert(id, task.task);
                                }
                            }
                        }
                    }

                    tokio::time::sleep(Self::BACKGROUND_LOOP_DELAY)
                        .with_cancellation(ct2.clone())
                        .await?;
                }
                trace!("terminating cleaner");
                Ok(())
            });

            let _ = self.spawner.insert(spawner);
            let _ = self.cleaner.insert(cleaner);
            self.is_running.store(true, Ordering::SeqCst);
        }
    }

    pub async fn stop(&mut self) {
        if self.is_running() {
            trace!("Scheduler::stop()");
            self.is_running.store(false, Ordering::SeqCst);
            if let Some(handle) = self.spawner.take() {
                handle.abort();
            }
            if let Some(handle) = self.cleaner.take() {
                handle.abort();
            }
        }
    }

    pub async fn shutdown(&mut self) {
        trace!("Scheduler::shutdown()");
        self.stop().await;
        self.ct.cancel();

        for (_, task) in self.waiting_tasks.write().await.drain() {
            drop(task);
        }

        for (_, task) in self.active_tasks.write().await.drain() {
            task.ct.cancel();
            task.handle.abort();
            drop(task);
        }
    }

    pub fn wait_for_exit(&self) -> WaitForCancellationFutureOwned {
        trace!("Scheduler::wait_for_exit()");
        self.ct.clone().cancelled_owned()
    }

    pub async fn get_task(&self, Id(id): Id) -> Option<Handle> {
        let waiting = self.waiting_tasks.read().await;
        let active = self.active_tasks.read().await;

        if waiting.contains_key(&id) || active.contains_key(&id) {
            Some(Handle {
                id,
                waiting: self.waiting_tasks.clone(),
                active: self.active_tasks.clone(),
            })
        } else {
            None
        }
    }

    async fn generate_task_id(&self) -> Uuid {
        let mut id = Uuid::new_v4();

        while self.active_tasks.read().await.contains_key(&id)
            || self.waiting_tasks.read().await.contains_key(&id)
        {
            id = Uuid::new_v4();
        }

        id
    }
}
