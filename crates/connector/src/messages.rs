use flux::timing::Nanos;
use gravity_types::{LeaderState, NextLeaderRange, SlotProgress, WARMUP_SLOTS, wire::SlotNum};

const PRE_WARMUP_RETENTION_SLOTS: u64 = 5;

#[derive(Clone, Copy, Default)]
pub struct ConnectorProgressTracker {
    pub current_slot: SlotNum,
    pub leader_state: LeaderState,
    pub first_progress_observed_at: Nanos,
    next_sequencing_slot: Option<SlotNum>,
}

impl ConnectorProgressTracker {
    pub fn retain_for_scheduling(&self) -> bool {
        self.next_sequencing_slot.is_some_and(|next| {
            self.current_slot.saturating_add(WARMUP_SLOTS + PRE_WARMUP_RETENTION_SLOTS) >= next
        })
    }

    pub fn update(&mut self, progress: SlotProgress) -> bool {
        if self.current_slot != progress.slot_num || self.first_progress_observed_at == Nanos::ZERO
        {
            self.first_progress_observed_at = progress.observed_at;
        }
        self.current_slot = progress.slot_num;
        let next_leadership: Option<NextLeaderRange> = progress.next_leadership.into();
        self.next_sequencing_slot = next_leadership.map(|next| next.start);
        self.leader_state.update(progress.slot_num, next_leadership)
    }
}
