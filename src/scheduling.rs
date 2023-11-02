use std::{
    str::FromStr,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::Duration,
};

use chrono::{DateTime, Utc};
use cron::OwnedScheduleIterator;

use crate::SchedulerError;

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
pub struct AlwaysSchedule;

impl Schedule for AlwaysSchedule {
    fn next(&self) -> Option<&DateTime<Utc>> {
        None
    }

    fn advance(&mut self) {}

    fn is_ready(&self) -> bool {
        true
    }

    fn is_finished(&self) -> bool {
        false
    }
}

pub struct IntervalSchedule {
    interval: Duration,
    next: DateTime<Utc>,
}

impl IntervalSchedule {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            next: Utc::now() + interval,
        }
    }
}

impl Schedule for IntervalSchedule {
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

impl From<Duration> for IntervalSchedule {
    fn from(value: Duration) -> Self {
        Self::new(value)
    }
}

impl TryFrom<chrono::Duration> for IntervalSchedule {
    type Error = SchedulerError;

    fn try_from(value: chrono::Duration) -> Result<Self, Self::Error> {
        let std = value.to_std()?;
        let me = Self::new(std);

        Ok(me)
    }
}

pub struct CronSchedule {
    it: OwnedScheduleIterator<Utc>,
    next: Option<DateTime<Utc>>,
}

impl CronSchedule {
    pub fn parse<S: AsRef<str>>(expr: S) -> Result<Self, SchedulerError> {
        let mut it = cron::Schedule::from_str(expr.as_ref())?.upcoming_owned(Utc);
        let next = it.next();

        Ok(Self { it, next })
    }
}

impl Schedule for CronSchedule {
    fn next(&self) -> Option<&DateTime<Utc>> {
        self.next.as_ref()
    }

    fn advance(&mut self) {
        self.next = self.it.next()
    }
}

/// A schedule that is ready immediately.
/// After the initial run, the underlying schedule is followed.
pub struct ImmediateSchedule<S: Schedule> {
    schedule: S,
    first_run: AtomicBool,
    created_at: DateTime<Utc>,
}

impl<S: Schedule> ImmediateSchedule<S> {
    pub fn new(schedule: S) -> Self {
        Self {
            schedule,
            first_run: AtomicBool::new(true),
            created_at: Utc::now(),
        }
    }
}

impl<S: Schedule> Schedule for ImmediateSchedule<S> {
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

impl<S: Schedule> From<S> for ImmediateSchedule<S> {
    fn from(schedule: S) -> Self {
        Self::new(schedule)
    }
}

/// A schedule that runs a maximum number of times.
pub struct LimitedRunSchedule<S: Schedule> {
    schedule: S,
    max_runs: u64,
    count: AtomicU64,
}

impl<S: Schedule> LimitedRunSchedule<S> {
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

impl<S: Schedule> Schedule for LimitedRunSchedule<S> {
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

pub struct NotBeforeSchedule<S: Schedule> {
    start: DateTime<Utc>,
    base: S,
}

impl<S: Schedule> NotBeforeSchedule<S> {
    pub fn new(start: DateTime<Utc>, base: S) -> Self {
        Self { start, base }
    }
}

impl<S: Schedule> Schedule for NotBeforeSchedule<S> {
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

pub struct NotAfterSchedule<S: Schedule> {
    end: DateTime<Utc>,
    base: S,
}

impl<S: Schedule> NotAfterSchedule<S> {
    pub fn new(end: DateTime<Utc>, base: S) -> Self {
        Self { end, base }
    }
}

impl<S: Schedule> Schedule for NotAfterSchedule<S> {
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

pub struct DateRangeSchedule<S: Schedule> {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    base: S,
}

impl<S: Schedule> DateRangeSchedule<S> {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>, base: S) -> Result<Self, SchedulerError> {
        if end < start {
            Err(SchedulerError::InvalidDateRange { start, end })
        } else {
            Ok(Self { start, end, base })
        }
    }
}

impl<S: Schedule> Schedule for DateRangeSchedule<S> {
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
