use std::collections::BinaryHeap;

use flux::timing::{Duration, Instant};
use gravity_types::{
    ExecutionResult, MiniBlockUuid, NotIncludedReason, SigPrefix,
    wire::{NodeOrderRef, SlotNum, WireMiniBlockGraph},
};
use tracing::warn;

#[derive(Debug, PartialEq, Eq)]
pub enum DagError {
    BadShape,
}

const EXPECTED_NODES_PER_GRAPH: usize = 1024;
const EXPECTED_SUCCESSORS_PER_NODE: usize = 8;

struct Node {
    successors: Vec<u32>,
    preds_remaining: u32,
    depth: u32,
    order_ref: NodeOrderRef,
    live: bool,
    in_flight: bool,
}

impl Node {
    fn empty() -> Self {
        Self {
            successors: Vec::with_capacity(EXPECTED_SUCCESSORS_PER_NODE),
            preds_remaining: 0,
            depth: 0,
            order_ref: NodeOrderRef::Tx { sig: SigPrefix::new([0; 16]) },
            live: false,
            in_flight: false,
        }
    }
}

/// Runtime walk of a builder-planned mini-block DAG (CSR): hands out one ready
/// order at a time, deepest critical path first, unlocking successors on
/// completion.
pub struct Dag {
    /// Node pool, reused across graphs to keep the per-node allocations.
    nodes: Vec<Node>,
    /// Nodes of the loaded graph: `nodes[..len]`.
    len: usize,
    /// `(depth, node_idx)`, deepest first.
    ready: BinaryHeap<(u32, u32)>,
    remaining: usize,
    preds_scratch: Vec<u32>,
    /// `(slot, uuid)` of the loaded graph.
    loaded: Option<(SlotNum, MiniBlockUuid)>,

    expected_graph_index: u32,

    cooling: Vec<(Instant, u32)>,
    retry_cooldown: Duration,
}

impl Default for Dag {
    fn default() -> Self {
        Self::new()
    }
}

impl Dag {
    pub fn new() -> Self {
        Self {
            nodes: Vec::with_capacity(EXPECTED_NODES_PER_GRAPH),
            len: 0,
            ready: BinaryHeap::with_capacity(EXPECTED_NODES_PER_GRAPH),
            remaining: 0,
            preds_scratch: Vec::with_capacity(EXPECTED_NODES_PER_GRAPH),
            loaded: None,
            expected_graph_index: 0,
            cooling: Vec::new(),
            retry_cooldown: Duration::from_micros(200),
        }
    }

    pub fn graph_id(&self) -> Option<(SlotNum, MiniBlockUuid)> {
        self.loaded
    }

    pub fn load(&mut self, graph: &WireMiniBlockGraph) -> Result<(), DagError> {
        self.clear();

        if graph.index_in_slot != self.expected_graph_index {
            warn!(
                graph_index = graph.index_in_slot,
                expected_index = self.expected_graph_index,
                "unexpected graph index",
            );
        }
        self.expected_graph_index += 1;

        let n = graph.nodes.len();
        if graph.row_offsets.len() != n + 1 ||
            graph.row_offsets[0] != 0 ||
            graph.row_offsets[n] as usize != graph.col_indices.len() ||
            graph.row_offsets.windows(2).any(|w| w[0] > w[1])
        {
            return Err(DagError::BadShape);
        }

        self.preds_scratch.clear();
        self.preds_scratch.resize(n, 0);
        for &target in &graph.col_indices {
            if target as usize >= n {
                return Err(DagError::BadShape);
            }
            self.preds_scratch[target as usize] += 1;
        }

        while self.nodes.len() < n {
            self.nodes.push(Node::empty());
        }
        for (i, cn) in graph.nodes.iter().enumerate() {
            let start = graph.row_offsets[i] as usize;
            let end = graph.row_offsets[i + 1] as usize;
            let node = &mut self.nodes[i];
            node.successors.clear();
            node.successors.extend_from_slice(&graph.col_indices[start..end]);
            node.preds_remaining = self.preds_scratch[i];
            node.depth = cn.depth;
            node.order_ref = cn.order_ref;
            node.live = true;
            node.in_flight = false;
        }

        for (i, &p) in self.preds_scratch.iter().enumerate() {
            if p == 0 {
                self.ready.push((self.nodes[i].depth, i as u32));
            }
        }
        self.len = n;
        self.remaining = n;
        self.loaded = Some((graph.slot, graph.uuid));
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    pub fn order_ref(&self, idx: u32) -> NodeOrderRef {
        self.nodes[idx as usize].order_ref
    }

    pub fn take_ready(&mut self) -> Option<u32> {
        self.release_cooled();
        let idx = self.ready.pop().map(|(_, idx)| idx)?;
        self.nodes[idx as usize].in_flight = true;
        Some(idx)
    }

    fn release_cooled(&mut self) {
        if self.cooling.is_empty() {
            return;
        }
        let now = Instant::now();
        let Self { cooling, ready, nodes, .. } = self;
        cooling.retain(|&(eligible_at, idx)| {
            if now < eligible_at {
                return true;
            }
            let node = &nodes[idx as usize];
            if node.live {
                ready.push((node.depth, idx));
            }
            false
        });
    }

    /// Live orders never sent to a worker.
    pub fn undispatched(&self) -> impl Iterator<Item = NodeOrderRef> + '_ {
        self.nodes[..self.len].iter().filter(|n| n.live && !n.in_flight).map(|n| n.order_ref)
    }

    pub fn apply_results(&mut self, idx: u32, results: &[ExecutionResult]) {
        if should_retry(results) {
            self.retry(idx);
        } else {
            self.complete(idx);
        }
    }

    fn retry(&mut self, idx: u32) {
        let node = &mut self.nodes[idx as usize];
        if node.live {
            node.in_flight = false;
            self.cooling.push((Instant::now() + self.retry_cooldown, idx));
        }
    }

    pub fn complete(&mut self, idx: u32) {
        let i = idx as usize;
        if !self.nodes[i].live {
            return;
        }
        self.nodes[i].live = false;
        // Take/put-back so the successor vec keeps its allocation.
        let mut successors = std::mem::take(&mut self.nodes[i].successors);
        for &s in &successors {
            let sn = &mut self.nodes[s as usize];
            sn.preds_remaining = sn.preds_remaining.saturating_sub(1);
            if sn.preds_remaining == 0 {
                self.ready.push((sn.depth, s));
            }
        }
        successors.clear();
        self.nodes[i].successors = successors;
        self.remaining -= 1;
    }

    /// Drops the loaded graph; the node pool is kept for reuse.
    pub fn clear(&mut self) {
        self.len = 0;
        self.ready.clear();
        self.cooling.clear();
        self.remaining = 0;
        self.loaded = None;
    }

    pub fn on_new_slot(&mut self) {
        self.clear();
        self.expected_graph_index = 0;
    }
}

fn should_retry(results: &[ExecutionResult]) -> bool {
    results.iter().any(|r| {
        matches!(
            r,
            ExecutionResult::NotIncluded(
                NotIncludedReason::BANK_NOT_AVAILABLE |
                    NotIncludedReason::ACCOUNT_IN_USE |
                    // Rejected by the leader's estimate-based cost admission (QoS
                    // charges the *requested* cost up front and only adjusts down
                    // to actual after a batch commits): in-flight commits release
                    // `reserved - actual` back to the tracker, freeing the space
                    // the builder budgeted with. If the limit is genuinely hit
                    // (builder cost accounting bug), the node retries until the
                    // slot ends and everything behind it is returned to the
                    // builder as SLOT_ENDED.
                    NotIncludedReason::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT |
                    NotIncludedReason::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
            )
        )
    })
}
