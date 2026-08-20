use flux::timing::Nanos;
use gravity_types::{LeaderState, SlotProgress};

#[derive(Clone, Copy, Default)]
pub struct ConnectorProgressTracker {
    pub current_slot: u64,
    pub leader_state: LeaderState,
    pub first_progress_observed_at: Nanos,
}

impl ConnectorProgressTracker {
    pub fn update(&mut self, progress: SlotProgress) -> bool {
        if self.current_slot != progress.slot_num || self.first_progress_observed_at == Nanos::ZERO
        {
            self.first_progress_observed_at = progress.observed_at;
        }
        self.current_slot = progress.slot_num;
        self.leader_state.update(progress.slot_num, progress.next_leadership.into())
    }
}
