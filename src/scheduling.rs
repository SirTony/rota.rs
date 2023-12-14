use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

#[cfg(feature = "cron")]
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[cfg(feature = "cron")]
use cron::OwnedScheduleIterator;

use crate::Error;

#[async_trait]
pub trait Schedule {
    async fn next(&self) -> Option<DateTime<Utc>>;
    async fn advance(&mut self);

    async fn is_ready(&self) -> bool {
        if let Some(when) = self.next().await {
            Utc::now() >= when
        } else {
            false
        }
    }

    async fn is_finished(&self) -> bool {
        self.next().await.is_none()
    }
}

/// Defines a schedule that is always ready to execute.
pub struct Always;

#[async_trait]
impl Schedule for Always {
    async fn next(&self) -> Option<DateTime<Utc>> {
        Some(DateTime::<Utc>::MIN_UTC)
    }

    async fn advance(&mut self) {}

    async fn is_ready(&self) -> bool {
        true
    }

    async fn is_finished(&self) -> bool {
        false
    }
}

pub struct Interval {
    interval: Duration,
    next: DateTime<Utc>,
}

impl Interval {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            next: Utc::now() + interval,
        }
    }
}

#[async_trait]
impl Schedule for Interval {
    async fn next(&self) -> Option<DateTime<Utc>> {
        Some(self.next)
    }

    async fn advance(&mut self) {
        self.next = Utc::now() + self.interval;
    }

    async fn is_finished(&self) -> bool {
        false
    }
}

impl From<Duration> for Interval {
    fn from(value: Duration) -> Self {
        Self::new(value)
    }
}

impl TryFrom<chrono::Duration> for Interval {
    type Error = Error;

    fn try_from(value: chrono::Duration) -> Result<Self, Self::Error> {
        let std = value.to_std()?;
        let me = Self::new(std);

        Ok(me)
    }
}

#[cfg(feature = "cron")]
pub struct Cron {
    it: OwnedScheduleIterator<Utc>,
    next: Option<DateTime<Utc>>,
}

#[cfg(feature = "cron")]
impl Cron {
    pub fn parse<S: AsRef<str>>(expr: S) -> Result<Self, Error> {
        let mut it = cron::Schedule::from_str(expr.as_ref())?.upcoming_owned(Utc);
        let next = it.next();

        Ok(Self { it, next })
    }
}

#[async_trait]
#[cfg(feature = "cron")]
impl Schedule for Cron {
    async fn next(&self) -> Option<DateTime<Utc>> {
        self.next
    }

    async fn advance(&mut self) {
        self.next = self.it.next()
    }
}

/// An async schedule that is ready immediately.
/// After the initial run, the underlying schedule is followed.
pub struct Immediate<S: Schedule + Send + Sync + 'static> {
    schedule: S,
    first_run: AtomicBool,
    created_at: DateTime<Utc>,
}

impl<S: Schedule + Send + Sync + 'static> Immediate<S> {
    pub fn new(schedule: S) -> Self {
        Self {
            schedule,
            first_run: AtomicBool::new(true),
            created_at: Utc::now(),
        }
    }
}

#[async_trait]
impl<S: Schedule + Send + Sync + 'static> Schedule for Immediate<S> {
    async fn next(&self) -> Option<DateTime<Utc>> {
        if self.first_run.load(Ordering::SeqCst) {
            Some(self.created_at)
        } else {
            self.schedule.next().await
        }
    }

    async fn advance(&mut self) {
        if self.first_run.load(Ordering::SeqCst) {
            self.first_run.store(false, Ordering::SeqCst)
        } else {
            self.schedule.advance().await
        }
    }
}

impl<S: Schedule + Send + Sync + 'static> From<S> for Immediate<S> {
    fn from(schedule: S) -> Self {
        Self::new(schedule)
    }
}

/// A schedule that runs a maximum number of times.
pub struct LimitedRun<S: Schedule + Send + Sync> {
    schedule: S,
    max_runs: u64,
    count: AtomicU64,
}

impl<S: Schedule + Send + Sync> LimitedRun<S> {
    pub fn new(schedule: S, max_runs: u64) -> Self {
        Self {
            schedule,
            max_runs,
            count: AtomicU64::new(0),
        }
    }

    pub fn once(schedule: S) -> Self {
        Self::new(schedule, 1)
    }

    pub fn twice(schedule: S) -> Self {
        Self::new(schedule, 2)
    }

    pub fn thrice(schedule: S) -> Self {
        Self::new(schedule, 3)
    }
}

#[async_trait]
impl<S: Schedule + Send + Sync> Schedule for LimitedRun<S> {
    async fn next(&self) -> Option<DateTime<Utc>> {
        if self.count.load(Ordering::SeqCst) >= self.max_runs {
            None
        } else {
            self.schedule.next().await
        }
    }

    async fn advance(&mut self) {
        let count = self.count.load(Ordering::SeqCst);
        if count < self.max_runs {
            self.count.store(count + 1, Ordering::SeqCst);
            self.schedule.advance().await;
        }
    }
}

pub struct NotBefore<S: Schedule + Send + Sync + 'static> {
    start: DateTime<Utc>,
    base: S,
}

impl<S: Schedule + Send + Sync + 'static> NotBefore<S> {
    pub fn new(start: DateTime<Utc>, base: S) -> Self {
        Self { start, base }
    }
}

#[async_trait]
impl<S: Schedule + Send + Sync + 'static> Schedule for NotBefore<S> {
    async fn next(&self) -> Option<DateTime<Utc>> {
        if self.base.next().await.map_or(false, |x| x < self.start) {
            Some(self.start)
        } else {
            self.base.next().await
        }
    }

    async fn advance(&mut self) {
        self.base.advance().await
    }
}

pub struct NotAfter<S: Schedule> {
    end: DateTime<Utc>,
    base: S,
}

impl<S: Schedule + Send + Sync + 'static> NotAfter<S> {
    pub fn new(end: DateTime<Utc>, base: S) -> Self {
        Self { end, base }
    }
}

#[async_trait]
impl<S: Schedule + Send + Sync + 'static> Schedule for NotAfter<S> {
    async fn next(&self) -> Option<DateTime<Utc>> {
        if self.base.next().await.map_or(false, |x| x > self.end) {
            None
        } else {
            self.base.next().await
        }
    }

    async fn advance(&mut self) {
        self.base.advance().await
    }
}

pub struct DateRange<S: Schedule + Send + Sync + 'static> {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    base: S,
}

impl<S: Schedule + Send + Sync + 'static> DateRange<S> {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, base: S) -> Result<Self, Error> {
        if end < start {
            Err(Error::InvalidDateRange { start, end })
        } else {
            Ok(Self { start, end, base })
        }
    }
}

#[async_trait]
impl<S: Schedule + Send + Sync + 'static> Schedule for DateRange<S> {
    async fn next(&self) -> Option<DateTime<Utc>> {
        self.base
            .next()
            .await
            .map(|x| x.max(self.start))
            .map(|x| x.min(self.end))
    }

    async fn advance(&mut self) {
        self.base.advance().await
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduling::{Always, Schedule};

    use super::LimitedRun;

    #[tokio::test]
    async fn always() {
        let s = Always;
        assert!(s.is_ready().await, "always not ready");
        assert!(!s.is_finished().await, "always is finished");
    }

    #[tokio::test]
    async fn once() {
        let mut s = LimitedRun::once(Always);
        assert!(s.is_ready().await, "once is not ready");
        s.advance().await;
        assert!(s.is_finished().await, "once is not finished");
    }
}
