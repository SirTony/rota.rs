use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

#[cfg(feature = "cron")]
use std::str::FromStr;

use chrono::{DateTime, Utc};

#[cfg(feature = "cron")]
use cron::OwnedScheduleIterator;

use crate::Error;

pub trait Schedule {
    fn next(&self) -> Option<&DateTime<Utc>>;
    fn advance(&mut self);

    fn is_ready(&self) -> bool {
        if let Some(when) = self.next() {
            Utc::now() >= *when
        } else {
            false
        }
    }

    fn is_finished(&self) -> bool {
        self.next().is_none()
    }
}

/// Defines a schedule that is always ready to execute.
pub struct Always(DateTime<Utc>);

impl Always {
    pub fn new() -> Self {
        Self(DateTime::<Utc>::MIN_UTC)
    }
}

impl Default for Always {
    fn default() -> Self {
        Self::new()
    }
}

impl Schedule for Always {
    fn next(&self) -> Option<&DateTime<Utc>> {
        Some(&self.0)
    }

    fn advance(&mut self) {}

    fn is_ready(&self) -> bool {
        true
    }

    fn is_finished(&self) -> bool {
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

impl Schedule for Interval {
    fn next(&self) -> Option<&DateTime<Utc>> {
        Some(&self.next)
    }

    fn advance(&mut self) {
        self.next = Utc::now() + self.interval;
    }

    fn is_finished(&self) -> bool {
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
impl Schedule for Cron {
    fn next(&self) -> Option<&DateTime<Utc>> {
        self.next.as_ref()
    }

    fn advance(&mut self) {
        self.next = self.it.next()
    }
}

/// A schedule that is ready immediately.
/// After the initial run, the underlying schedule is followed.
pub struct Immediate<S: Schedule> {
    schedule: S,
    first_run: AtomicBool,
    created_at: DateTime<Utc>,
}

impl<S: Schedule> Immediate<S> {
    pub fn new(schedule: S) -> Self {
        Self {
            schedule,
            first_run: AtomicBool::new(true),
            created_at: Utc::now(),
        }
    }
}

impl<S: Schedule> Schedule for Immediate<S> {
    fn next(&self) -> Option<&DateTime<Utc>> {
        if self.first_run.load(Ordering::SeqCst) {
            Some(&self.created_at)
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

impl<S: Schedule> From<S> for Immediate<S> {
    fn from(schedule: S) -> Self {
        Self::new(schedule)
    }
}

/// A schedule that runs a maximum number of times.
pub struct LimitedRun<S: Schedule> {
    schedule: S,
    max_runs: u64,
    count: AtomicU64,
}

impl<S: Schedule> LimitedRun<S> {
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

impl<S: Schedule> Schedule for LimitedRun<S> {
    fn next(&self) -> Option<&DateTime<Utc>> {
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

pub struct NotBefore<S: Schedule> {
    start: DateTime<Utc>,
    base: S,
}

impl<S: Schedule> NotBefore<S> {
    pub fn new(start: DateTime<Utc>, base: S) -> Self {
        Self { start, base }
    }
}

impl<S: Schedule> Schedule for NotBefore<S> {
    fn next(&self) -> Option<&DateTime<Utc>> {
        if self.base.next().map_or(false, |x| x < &self.start) {
            Some(&self.start)
        } else {
            self.base.next()
        }
    }

    fn advance(&mut self) {
        self.base.advance()
    }
}

pub struct NotAfter<S: Schedule> {
    end: DateTime<Utc>,
    base: S,
}

impl<S: Schedule> NotAfter<S> {
    pub fn new(end: DateTime<Utc>, base: S) -> Self {
        Self { end, base }
    }
}

impl<S: Schedule> Schedule for NotAfter<S> {
    fn next(&self) -> Option<&DateTime<Utc>> {
        if self.base.next().map_or(false, |x| x > &self.end) {
            None
        } else {
            self.base.next()
        }
    }

    fn advance(&mut self) {
        self.base.advance()
    }
}

pub struct DateRange<S: Schedule> {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    base: S,
}

impl<S: Schedule> DateRange<S> {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, base: S) -> Result<Self, Error> {
        if end < start {
            Err(Error::InvalidDateRange { start, end })
        } else {
            Ok(Self { start, end, base })
        }
    }
}

impl<S: Schedule> Schedule for DateRange<S> {
    fn next(&self) -> Option<&DateTime<Utc>> {
        self.base
            .next()
            .map(|x| x.max(&self.start))
            .map(|x| x.min(&self.end))
    }

    fn advance(&mut self) {
        self.base.advance()
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduling::{Always, Schedule};

    use super::LimitedRun;

    #[test]
    fn always() {
        let s = Always::new();
        assert!(s.is_ready(), "always not ready");
        assert!(!s.is_finished(), "always is finished");
    }

    #[test]
    fn once() {
        let mut s = LimitedRun::once(Always::new());
        assert!(s.is_ready(), "once is not ready");
        s.advance();
        assert!(s.is_finished(), "once is not finished");
    }
}
