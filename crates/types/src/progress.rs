use agave_scheduler_bindings::ProgressMessage;
use flux::{timing::Nanos, type_hash_derive::TypeHash};
use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::{ffi_safety::FfiOption, wire::SlotNum};

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite, Serialize, Deserialize, TypeHash)]
#[repr(C)]
pub struct NextLeaderRange {
    pub start: u64,
    /// Inclusive
    pub end: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TypeHash, SchemaRead, SchemaWrite)]
#[repr(C)]
pub struct SlotProgress {
    pub slot_num: SlotNum,
    pub observed_at: Nanos,
    pub current_slot_progress: u8,
    pub slot_duration_override_ms: FfiOption<u64>,
    pub next_leadership: FfiOption<NextLeaderRange>,
    pub latest_blockhash: FfiOption<[u8; 32]>,
}

impl SlotProgress {
    pub fn from_agave_progress(
        msg: ProgressMessage,
        slot_duration_override_ms: Option<u64>,
    ) -> Self {
        let next_leadership = if msg.next_leader_slot < u64::MAX && msg.leader_range_end < u64::MAX
        {
            Some(NextLeaderRange { start: msg.next_leader_slot, end: msg.leader_range_end })
        } else {
            None
        };
        let latest_blockhash =
            if msg.latest_blockhash == [0; 32] { None } else { Some(msg.latest_blockhash) };

        Self {
            slot_num: msg.current_slot,
            observed_at: Nanos::now(),
            current_slot_progress: msg.current_slot_progress,
            slot_duration_override_ms: slot_duration_override_ms.into(),
            next_leadership: next_leadership.into(),
            latest_blockhash: latest_blockhash.into(),
        }
    }
}

/// Start accepting a few slots before we're leader.
pub const WARMUP_SLOTS: u64 = 6;
/// Keep accepting a few slots after last leader slot.
const COOLDOWN_SLOTS: u64 = 2;
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderState {
    #[default]
    Inactive,
    Warmup,
    Sequencing,
    /// Variable tracks when cooldown started. Needs to be tracked in
    /// the `LeaderState`, since the `ProgressMessage` doesn't tell us about
    /// previous leaderships.
    Cooldown(SlotNum),
}
impl From<LeaderState> for u8 {
    fn from(val: LeaderState) -> Self {
        match val {
            LeaderState::Inactive => 0,
            LeaderState::Warmup => 1,
            LeaderState::Sequencing => 2,
            LeaderState::Cooldown(_) => 3,
        }
    }
}
impl LeaderState {
    /// Return a bool indicating if we have exited a leadership
    pub fn update(&mut self, current_slot: SlotNum, next_leader: Option<NextLeaderRange>) -> bool {
        let prev_state = *self;
        *self = if next_leader
            .is_some_and(|nl| (nl.start <= current_slot) && (current_slot <= nl.end))
        {
            Self::Sequencing
        } else if current_slot + WARMUP_SLOTS >= next_leader.map_or(SlotNum::MAX, |next| next.start)
        {
            Self::Warmup
        } else if Self::Sequencing == *self {
            Self::Cooldown(current_slot)
        } else if let Self::Cooldown(cooldown_start) = *self &&
            current_slot < cooldown_start + COOLDOWN_SLOTS
        {
            *self
        } else {
            Self::Inactive
        };
        prev_state != Self::Inactive && *self == Self::Inactive
    }
}
