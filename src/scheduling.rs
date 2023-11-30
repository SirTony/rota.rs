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

pub trait SyncSchedule {
    fn next(&self) -> Option<DateTime<Utc>>;
    fn advance(&mut self);

    fn is_ready(&self) -> bool {
        if let Some(when) = self.next() {
            Utc::now() >= when
        } else {
            false
        }
    }

    fn is_finished(&self) -> bool {
        self.next().is_none()
    }
}

#[async_trait]
pub trait AsyncSchedule {
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

impl SyncSchedule for Always {
    fn next(&self) -> Option<DateTime<Utc>> {
        Some(DateTime::<Utc>::MIN_UTC)
    }

    fn advance(&mut self) {}

    fn is_ready(&self) -> bool {
        true
    }

    fn is_finished(&self) -> bool {
        false
    }
}

#[async_trait]
impl AsyncSchedule for Always {
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

impl SyncSchedule for Interval {
    fn next(&self) -> Option<DateTime<Utc>> {
        Some(self.next)
    }

    fn advance(&mut self) {
        self.next = Utc::now() + self.interval;
    }

    fn is_finished(&self) -> bool {
        false
    }
}

#[async_trait]
impl AsyncSchedule for Interval {
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

#[cfg(feature = "cron")]
impl SyncSchedule for Cron {
    fn next(&self) -> Option<DateTime<Utc>> {
        self.next
    }

    fn advance(&mut self) {
        self.next = self.it.next()
    }
}

#[async_trait]
#[cfg(feature = "cron")]
impl AsyncSchedule for Cron {
    async fn next(&self) -> Option<DateTime<Utc>> {
        self.next
    }

    async fn advance(&mut self) {
        self.next = self.it.next()
    }
}

// pub struct Immediate;

// impl Immediate {
//     pub fn new_sync<S: SyncSchedule>( schedule: S ) -> ImmediateSync<S> {
//         ImmediateSync::new(schedule)
//     }

//     pub fn new_async<S: AsyncSchedule + Send + Sync + 'static>( schedule: S ) -> ImmediateAsync<S> {
//         ImmediateAsync::new(schedule)
//     }
// }

/// A sync schedule that is ready immediately.
/// After the initial run, the underlying schedule is followed.
pub struct ImmediateSync<S: SyncSchedule> {
    schedule: S,
    first_run: AtomicBool,
    created_at: DateTime<Utc>,
}

impl<S: SyncSchedule> ImmediateSync<S> {
    pub fn new(schedule: S) -> Self {
        Self {
            schedule,
            first_run: AtomicBool::new(true),
            created_at: Utc::now(),
        }
    }
}

impl<S: SyncSchedule> SyncSchedule for ImmediateSync<S> {
    fn next(&self) -> Option<DateTime<Utc>> {
        if self.first_run.load(Ordering::SeqCst) {
            Some(self.created_at)
        } else {
            self.schedule.next()
        }
    }

    fn advance(&mut self) {
        if self.first_run.load(Ordering::SeqCst) {
            self.first_run.store(false, Ordering::SeqCst)
        } else {
            self.schedule.advance()
        }
    }
}

impl<S: SyncSchedule> From<S> for ImmediateSync<S> {
    fn from(schedule: S) -> Self {
        Self::new(schedule)
    }
}

/// An async schedule that is ready immediately.
/// After the initial run, the underlying schedule is followed.
pub struct ImmediateAsync<S: AsyncSchedule + Send + Sync + 'static> {
    schedule: S,
    first_run: AtomicBool,
    created_at: DateTime<Utc>,
}

impl<S: AsyncSchedule + Send + Sync + 'static> ImmediateAsync<S> {
    pub fn new(schedule: S) -> Self {
        Self {
            schedule,
            first_run: AtomicBool::new(true),
            created_at: Utc::now(),
        }
    }
}

#[async_trait]
impl<S: AsyncSchedule + Send + Sync + 'static> AsyncSchedule for ImmediateAsync<S> {
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

impl<S: AsyncSchedule + Send + Sync + 'static> From<S> for ImmediateAsync<S> {
    fn from(schedule: S) -> Self {
        Self::new(schedule)
    }
}

/// A schedule that runs a maximum number of times.
pub struct LimitedRunSync<S: SyncSchedule> {
    schedule: S,
    max_runs: u64,
    count: AtomicU64,
}

impl<S: SyncSchedule> LimitedRunSync<S> {
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

impl<S: SyncSchedule> SyncSchedule for LimitedRunSync<S> {
    fn next(&self) -> Option<DateTime<Utc>> {
        if self.count.load(Ordering::SeqCst) >= self.max_runs {
            None
        } else {
            self.schedule.next()
        }
    }

    fn advance(&mut self) {
        let count = self.count.load(Ordering::SeqCst);
        if count < self.max_runs {
            self.count.store(count + 1, Ordering::SeqCst);
            self.schedule.advance();
        }
    }
}

/// A schedule that runs a maximum number of times.
pub struct LimitedRunAsync<S: AsyncSchedule + Send + Sync> {
    schedule: S,
    max_runs: u64,
    count: AtomicU64,
}

impl<S: AsyncSchedule + Send + Sync> LimitedRunAsync<S> {
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
impl<S: AsyncSchedule + Send + Sync> AsyncSchedule for LimitedRunAsync<S> {
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

pub struct NotBeforeSync<S: SyncSchedule> {
    start: DateTime<Utc>,
    base: S,
}

impl<S: SyncSchedule> NotBeforeSync<S> {
    pub fn new(start: DateTime<Utc>, base: S) -> Self {
        Self { start, base }
    }
}

impl<S: SyncSchedule> SyncSchedule for NotBeforeSync<S> {
    fn next(&self) -> Option<DateTime<Utc>> {
        if self.base.next().map_or(false, |x| x < self.start) {
            Some(self.start)
        } else {
            self.base.next()
        }
    }

    fn advance(&mut self) {
        self.base.advance()
    }
}

pub struct NotBeforeAsync<S: AsyncSchedule + Send + Sync + 'static> {
    start: DateTime<Utc>,
    base: S,
}

impl<S: AsyncSchedule + Send + Sync + 'static> NotBeforeAsync<S> {
    pub fn new(start: DateTime<Utc>, base: S) -> Self {
        Self { start, base }
    }
}

#[async_trait]
impl<S: AsyncSchedule + Send + Sync + 'static> AsyncSchedule for NotBeforeAsync<S> {
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

pub struct NotAfterSync<S: SyncSchedule> {
    end: DateTime<Utc>,
    base: S,
}

impl<S: SyncSchedule> NotAfterSync<S> {
    pub fn new(end: DateTime<Utc>, base: S) -> Self {
        Self { end, base }
    }
}

impl<S: SyncSchedule> SyncSchedule for NotAfterSync<S> {
    fn next(&self) -> Option<DateTime<Utc>> {
        if self.base.next().map_or(false, |x| x > self.end) {
            None
        } else {
            self.base.next()
        }
    }

    fn advance(&mut self) {
        self.base.advance()
    }
}

pub struct NotAfterAsync<S: AsyncSchedule> {
    end: DateTime<Utc>,
    base: S,
}

impl<S: AsyncSchedule + Send + Sync + 'static> NotAfterAsync<S> {
    pub fn new(end: DateTime<Utc>, base: S) -> Self {
        Self { end, base }
    }
}

#[async_trait]
impl<S: AsyncSchedule + Send + Sync + 'static> AsyncSchedule for NotAfterAsync<S> {
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

pub struct DateRangeSync<S: SyncSchedule> {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    base: S,
}

impl<S: SyncSchedule> DateRangeSync<S> {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, base: S) -> Result<Self, Error> {
        if end < start {
            Err(Error::InvalidDateRange { start, end })
        } else {
            Ok(Self { start, end, base })
        }
    }
}

impl<S: SyncSchedule> SyncSchedule for DateRangeSync<S> {
    fn next(&self) -> Option<DateTime<Utc>> {
        self.base
            .next()
            .map(|x| x.max(self.start))
            .map(|x| x.min(self.end))
    }

    fn advance(&mut self) {
        self.base.advance()
    }
}

pub struct DateRangeAsync<S: AsyncSchedule + Send + Sync + 'static> {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    base: S,
}

impl<S: AsyncSchedule + Send + Sync + 'static> DateRangeAsync<S> {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, base: S) -> Result<Self, Error> {
        if end < start {
            Err(Error::InvalidDateRange { start, end })
        } else {
            Ok(Self { start, end, base })
        }
    }
}

#[async_trait]
impl<S: AsyncSchedule + Send + Sync + 'static> AsyncSchedule for DateRangeAsync<S> {
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
    use crate::scheduling::{Always, SyncSchedule};

    use super::LimitedRunSync;

    #[test]
    fn always() {
        let s = Always;
        assert!(s.is_ready(), "always not ready");
        assert!(!s.is_finished(), "always is finished");
    }

    #[test]
    fn once() {
        let mut s = LimitedRunSync::once(Always);
        assert!(s.is_ready(), "once is not ready");
        s.advance();
        assert!(s.is_finished(), "once is not finished");
    }
}
