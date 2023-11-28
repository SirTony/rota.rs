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

use crate::{
    ext::FutureEx,
    task::{ActiveTask, Task, TaskExecutable, TaskHandle, TaskId},
    Error, Result,
};

pub struct Scheduler {
    waiting_tasks: Arc<RwLock<HashMap<Uuid, Task>>>,
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

    pub async fn add_task(&mut self, task: Task) -> TaskId {
        let id = self.generate_task_id().await;
        trace!("adding task {}", id);
        self.waiting_tasks.write().await.insert(id, task);

        TaskId(id)
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
                                    && task.schedule.read().await.is_ready()
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
                            let task_id = TaskId(id);
                            let ct = ct.child_token();
                            let ct2 = ct.clone();
                            if let Some((id, task)) = tasks.remove_entry(&id) {
                                let kind = task.exec.clone();
                                let handle = tokio::spawn(async move {
                                    match kind {
                                        TaskExecutable::Sync(exec) => {
                                            let mut exec = exec.lock().await;
                                            match exec.execute(task_id, ct.clone()) {
                                                Ok(x) => Ok(x),
                                                Err(e) => Err(Error::Internal {
                                                    id: task_id,
                                                    error: e,
                                                }),
                                            }
                                        }
                                        TaskExecutable::Async(exec) => {
                                            let mut exec = exec.lock().await;
                                            select! {
                                                task_result = exec.execute(task_id, ct.clone()) => match task_result {
                                                    Ok( x ) => Ok( x ),
                                                    Err( e ) => Err( Error::Internal { id: task_id, error: e } )
                                                },
                                                _ = ct.cancelled() => Err( Error::Cancelled )
                                            }
                                        }
                                    }
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
                            if let Some(task) = active.remove(&id) {
                                task.task.schedule.write().await.advance();
                                tasks.insert(id, task.task);
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

    pub async fn get_task(&self, TaskId(id): TaskId) -> Option<TaskHandle> {
        let waiting = self.waiting_tasks.read().await;
        let active = self.active_tasks.read().await;

        if waiting.contains_key(&id) || active.contains_key(&id) {
            Some(TaskHandle {
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
