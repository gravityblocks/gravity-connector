use flux::timing::{Duration, Instant, Nanos};
use gravity_types::wire::WireStats;

/// Longest stage times of the connector loop since the last stats interval,
/// which logs, sends and resets them.
#[derive(Default)]
pub(crate) struct LoopStats {
    pub(crate) loop_gap: Duration,
    pub(crate) progress: Duration,
    pub(crate) tpu: Duration,
    pub(crate) workers: Duration,
    pub(crate) ping: Duration,
    pub(crate) crank: Duration,
    pub(crate) network: Duration,
    pub(crate) relay_events: Duration,
    pub(crate) dispatch: Duration,
    pub(crate) flush: Duration,
}

impl LoopStats {
    pub(crate) fn to_wire(&self) -> WireStats<'static> {
        WireStats::new([
            ("loop_gap_max_ns", ns(self.loop_gap)),
            ("progress_max_ns", ns(self.progress)),
            ("tpu_max_ns", ns(self.tpu)),
            ("workers_max_ns", ns(self.workers)),
            ("ping_max_ns", ns(self.ping)),
            ("crank_max_ns", ns(self.crank)),
            ("network_max_ns", ns(self.network)),
            ("relay_events_max_ns", ns(self.relay_events)),
            ("dispatch_max_ns", ns(self.dispatch)),
            ("flush_max_ns", ns(self.flush)),
        ])
    }
}

fn ns(duration: Duration) -> u64 {
    Nanos::from(duration).0
}

/// Times consecutive stages with one clock read each.
pub(crate) struct Lap(Instant);

impl Lap {
    pub(crate) const fn new(start: Instant) -> Self {
        Self(start)
    }

    /// Keeps the time since the last lap in `longest`, if it is longer.
    pub(crate) fn record(&mut self, longest: &mut Duration) {
        let now = Instant::now();
        *longest = (*longest).max(now.saturating_sub(self.0));
        self.0 = now;
    }
}
