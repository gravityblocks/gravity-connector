use std::num::Wrapping;

use agave_scheduler_bindings::ProgressMessage;
use flux::{
    timing::Nanos,
    type_hash_derive::{TypeHash, type_hash_lock},
};
use serde::{Deserialize, Serialize};
use tracing::warn;
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::{ffi_safety::FfiOption, wire::SlotNum};

/// Only sent to indicate a new slot has begun, and inform about future
/// leaderships
#[derive(Debug, Clone, Copy, Serialize, Deserialize, TypeHash, SchemaRead, SchemaWrite)]
#[type_hash_lock(hash = 18189279246232912046)]
#[repr(C)]
pub struct SlotMessage {
    pub slot_num: SlotNum,
    pub slot_start: Nanos,
    pub slot_expected_length: Nanos,
    pub next_leadership: FfiOption<NextLeaderRange>,
}

#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite, Serialize, Deserialize, TypeHash)]
#[repr(C)]
pub struct NextLeaderRange {
    pub start: u64,
    /// Inclusive
    pub end: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TypeHash, SchemaRead, SchemaWrite)]
#[type_hash_lock(hash = 14225699826065326468)]
#[repr(C)]
pub struct SlotMessageV2 {
    pub slot_num: SlotNum,
    pub slot_start: Nanos,
    pub slot_expected_length: Nanos,
    pub next_leadership: FfiOption<NextLeaderRange>,
    // set only when sequencing
    pub latest_blockhash: FfiOption<[u8; 32]>,
}

impl SlotMessageV2 {
    /// Should only call with the first progress message in a slot, otherwise
    /// the `slot_start` will be incorrect. Only considers the percentage
    /// progress in the agave message if it is a backwards message
    pub fn from_agave_progress(
        msg: ProgressMessage,
        slot_expected_length: Nanos,
        backwards: bool,
    ) -> Self {
        let next_leadership = if msg.next_leader_slot < u64::MAX && msg.leader_range_end < u64::MAX
        {
            Some(NextLeaderRange { start: msg.next_leader_slot, end: msg.leader_range_end })
        } else {
            None
        };
        let slot_start = if backwards {
            // TODO:  Using unadjusted, static slot_expected_length as opposed to dynamic
            // adjusted slot length which gets adjusted after this function is called
            Nanos::now() -
                Nanos(
                    (slot_expected_length.0 as f64 * msg.current_slot_progress as f64 / 100.0)
                        as u64,
                )
        } else {
            if msg.current_slot_progress > 5 {
                warn!(
                    "Making Slot Message with Agave Progress Message that is {}% complete. Slot start will be incorrect!",
                    msg.current_slot_progress
                );
            }
            Nanos::now()
        };
        let latest_blockhash =
            if msg.latest_blockhash == [0; 32] { None } else { Some(msg.latest_blockhash) };
        Self {
            slot_num: msg.current_slot,
            slot_start,
            slot_expected_length,
            next_leadership: next_leadership.into(),
            latest_blockhash: latest_blockhash.into(),
        }
    }
}

impl From<SlotMessageV2> for SlotMessage {
    fn from(msg: SlotMessageV2) -> Self {
        Self {
            slot_num: msg.slot_num,
            slot_start: msg.slot_start,
            slot_expected_length: msg.slot_expected_length,
            next_leadership: msg.next_leadership,
        }
    }
}

impl From<SlotMessage> for SlotMessageV2 {
    fn from(msg: SlotMessage) -> Self {
        Self {
            slot_num: msg.slot_num,
            slot_start: msg.slot_start,
            slot_expected_length: msg.slot_expected_length,
            next_leadership: msg.next_leadership,
            latest_blockhash: FfiOption::None,
        }
    }
}

/// Start accepting a few slots before we're leader.
const WARMUP_SLOTS: u64 = 6;
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

#[derive(Clone, Copy, Default)]
pub struct ProgressTracker {
    pub current_slot: SlotNum,
    pub leader_state: LeaderState,
    pub slot_start: Nanos,
    pub slot_expected_duration: Nanos,
    pub next_leadership: Option<NextLeaderRange>,
    /// Monotonic counter bumped on every slot message
    pub unique_slot_index: Wrapping<u64>,
}
impl ProgressTracker {
    /// Return a bool indicating if we have exited a leadership
    #[inline]
    pub fn update_from_slot_message(&mut self, msg: impl Into<SlotMessage>) -> bool {
        let msg = msg.into();
        self.slot_start = msg.slot_start;
        self.current_slot = msg.slot_num;
        self.next_leadership = msg.next_leadership.into();
        self.slot_expected_duration = msg.slot_expected_length;
        self.unique_slot_index += 1;
        self.leader_state.update(self.current_slot, self.next_leadership)
    }
    /// Value between 0 and 100 inclusive for how far into the slot we believe
    /// we are. Can get pinned to 100 if the slot is over-running.
    #[inline(always)]
    pub fn slot_percentage_complete(&self) -> f64 {
        (self.slot_start.elapsed_saturating().as_millis() / self.slot_expected_duration.as_millis() * 100.0)
            .clamp(0.0, 100.0)
    }

    #[inline(always)]
    pub fn leadership_slots_remaining(&self) -> u64 {
        if self.leader_state == LeaderState::Sequencing {
            self.next_leadership.unwrap().end - self.current_slot
        } else {
            0
        }
    }

    /// Can get clamped at 0 if slot is overrunning
    #[inline(always)]
    pub fn remaining_slot_time(&self) -> Nanos {
        self.slot_expected_duration.saturating_sub(self.slot_start.elapsed_saturating())
    }
}
