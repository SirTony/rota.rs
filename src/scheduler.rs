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

use crate::task::{execute_task, Id, Task};

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

#[derive(Clone)]
pub struct Scheduler {
    tasks: Arc<RwLock<HashMap<Uuid, Task>>>,
    ct: CancellationToken,
    is_running: Arc<AtomicBool>,
    spawner: Option<Arc<JoinHandle<()>>>,
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

        let this = self.clone();
        let spawner = tokio::spawn(async move {
            trace!("launching spawner");
            while !this.ct.is_cancelled() {
                if this.is_running.load(Ordering::SeqCst) {
                    let mut tasks = this.tasks.write().await;
                    let mut to_remove = Vec::new();

                    for (id, task) in tasks.iter_mut() {
                        if task.is_paused() || task.is_active().await {
                            continue;
                        } else if task.is_awaiting_removal().await {
                            to_remove.push(*id);
                        } else if task.schedule().await.is_ready().await {
                            debug!("spawning task {}", id);
                            execute_task(task, this.ct.child_token()).await;
                        }
                    }

                    for id in to_remove {
                        if let Some(task) = tasks.get(&id) {
                            // we want to skip this one if the task is active because
                            // otherwise we may accidentally remove a task that
                            // is currently running but the schedule indicates
                            // it won't run again and has not been manually removed
                            // with Task::remove(). Skipping allows the task to exit
                            // gracefully and be cleaned up in a future iteration.
                            if task.is_active().await {
                                continue;
                            }
                            if let Some(task) = tasks.remove(&id) {
                                debug!("removing task {}", id);
                                std::mem::drop(task);
                            }
                        }
                    }
                }

                select! {
                    _ = tokio::time::sleep(Self::BACKGROUND_LOOP_DELAY) => { continue; },
                    _ = this.ct.cancelled() => { break; }
                }
            }

            trace!("terminating spawner");
        });

        let _ = self.spawner.insert(Arc::new(spawner));
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
            .filter(|x| x.name().is_some())
            .find(|task| task.name().unwrap() == name)
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
