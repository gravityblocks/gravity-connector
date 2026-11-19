use std::{cmp::Reverse, collections::BinaryHeap, ops::Deref};

use flux::timing::{Duration, Instant, Nanos};
use gravity_types::{
    ExecutionResult, MiniBlockUuid, NotIncludedReason, SigPrefix,
    wire::{GraphLink, NodeOrderRef, SlotNum, WireMiniBlockGraph},
};
use tracing::warn;

pub(crate) struct ValidatedGraph {
    graph: WireMiniBlockGraph,
    /// Set for a `MiniBlockGraphIncrement`.
    link: Option<GraphLink>,
}

impl ValidatedGraph {
    pub(crate) fn new(
        graph: WireMiniBlockGraph,
        link: Option<GraphLink>,
    ) -> Result<Self, WireMiniBlockGraph> {
        let n = graph.nodes.len();
        let valid = graph.row_offsets.len() == n + 1 &&
            graph.row_offsets[0] == 0 &&
            graph.row_offsets[n] as usize == graph.col_indices.len() &&
            graph.row_offsets.is_sorted() &&
            graph.col_indices.iter().all(|&target| (target as usize) < n) &&
            link.as_ref().is_none_or(|link| {
                link.base.checked_add(n as u32).is_some() &&
                    link.prior_edges
                        .iter()
                        .all(|edge| edge.from < link.base && (edge.to as usize) < n)
            });
        if valid { Ok(Self { graph, link }) } else { Err(graph) }
    }

    /// The chain this graph starts when loaded: its round, and the chain index
    /// of its first node.
    fn chain(&self) -> (SlotNum, u32, u32) {
        (self.graph.slot, self.graph.index_in_slot, self.link.as_ref().map_or(0, |link| link.base))
    }
}

impl Deref for ValidatedGraph {
    type Target = WireMiniBlockGraph;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

const EXPECTED_NODES_PER_GRAPH: usize = 1024;
const EXPECTED_SUCCESSORS_PER_NODE: usize = 8;

struct Node {
    successors: Vec<u32>,
    preds_remaining: u32,
    depth: u32,
    order_ref: NodeOrderRef,
    /// Index into `Dag::graphs`.
    graph: u32,
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
            graph: 0,
            live: false,
            in_flight: false,
        }
    }
}

/// Runtime walk of a builder-planned mini-block DAG (CSR): hands out one ready
/// order at a time, deepest critical path first, unlocking successors on
/// completion. Increments of the loaded graph's round extend it in place, so
/// they don't wait for it to finish.
pub struct Dag {
    /// Node pool, reused across graphs to keep the per-node allocations.
    nodes: Vec<Node>,
    /// Nodes of the loaded graph: `nodes[..len]`.
    len: usize,
    /// `(depth, node_idx)`, deepest first.
    ready: BinaryHeap<ReadyKey>,
    remaining: usize,
    preds_scratch: Vec<u32>,
    /// `(slot, uuid)` and receive time of each graph loaded since the last
    /// clear.
    graphs: Vec<LoadedGraph>,
    /// The round of the loaded chain's latest graph and the chain index of
    /// node 0: node `i` is chain node `base + i`. Any loaded graph starts one,
    /// so a tail's increments can extend the round before them.
    chain: Option<(SlotNum, u32, u32)>,

    last_graph_index: Option<u32>,

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
            graphs: Vec::new(),
            chain: None,
            last_graph_index: None,
            cooling: Vec::new(),
            retry_cooldown: Duration::from_micros(200),
        }
    }

    /// Whether node `idx` is loaded from `graph_id`.
    pub fn owns(&self, idx: u32, graph_id: (SlotNum, MiniBlockUuid)) -> bool {
        (idx as usize) < self.len &&
            self.graphs[self.nodes[idx as usize].graph as usize].id == graph_id
    }

    /// `(slot, uuid)` and receive time of the graph `idx` came from.
    pub fn node_graph(&self, idx: u32) -> ((SlotNum, MiniBlockUuid), Nanos) {
        let g = &self.graphs[self.nodes[idx as usize].graph as usize];
        (g.id, g.received_at)
    }

    /// Whether `graph` can extend the loaded graphs instead of waiting for
    /// them: an increment of the loaded round, or of the next one (a tail
    /// extending the round it follows).
    pub(crate) fn extends(&self, graph: &ValidatedGraph) -> bool {
        let Some(link) = &graph.link else { return false };
        // A gap (an increment that never came) can't be numbered: it waits.
        self.chain.is_some_and(|(slot, index, base)| {
            slot == graph.slot &&
                (graph.index_in_slot == index ||
                    Some(graph.index_in_slot) == index.checked_add(1)) &&
                u64::from(base) + self.len as u64 == u64::from(link.base)
        })
    }

    pub(crate) fn load(&mut self, received_at: Nanos, graph: &ValidatedGraph) {
        self.clear();

        if let Some(last_index) = self.last_graph_index &&
            graph.index_in_slot < last_index
        {
            warn!(
                slot = graph.slot,
                graph_index = graph.index_in_slot,
                last_index,
                "graph index moved backwards",
            );
        }
        self.last_graph_index = Some(graph.index_in_slot);
        self.chain = Some(graph.chain());
        self.append(received_at, graph);
    }

    /// Adds `graph`'s nodes; its prior edges wait only on nodes still live.
    pub(crate) fn append(&mut self, received_at: Nanos, graph: &ValidatedGraph) {
        let offset = self.len;
        let n = graph.nodes.len();
        let graph_idx = self.graphs.len() as u32;
        self.graphs.push(LoadedGraph { id: (graph.slot, graph.uuid), received_at });

        self.preds_scratch.clear();
        self.preds_scratch.resize(n, 0);
        for &target in &graph.col_indices {
            self.preds_scratch[target as usize] += 1;
        }

        while self.nodes.len() < offset + n {
            self.nodes.push(Node::empty());
        }
        for (i, cn) in graph.nodes.iter().enumerate() {
            let start = graph.row_offsets[i] as usize;
            let end = graph.row_offsets[i + 1] as usize;
            let node = &mut self.nodes[offset + i];
            node.successors.clear();
            node.successors
                .extend(graph.col_indices[start..end].iter().map(|&s| s + offset as u32));
            node.depth = cn.depth;
            node.order_ref = cn.order_ref;
            node.graph = graph_idx;
            node.live = true;
            node.in_flight = false;
        }

        if let Some(link) = &graph.link {
            let chain_base = self.chain.map_or(0, |(.., base)| base);
            if let Some((_, index, _)) = &mut self.chain {
                *index = graph.index_in_slot;
            }
            for edge in &link.prior_edges {
                // Before the chain's first loaded node: completed already.
                let Some(pred) = edge.from.checked_sub(chain_base) else { continue };
                let pred = &mut self.nodes[pred as usize];
                if pred.live {
                    pred.successors.push(offset as u32 + edge.to);
                    self.preds_scratch[edge.to as usize] += 1;
                }
            }
        }

        for (i, &p) in self.preds_scratch.iter().enumerate() {
            let idx = offset + i;
            self.nodes[idx].preds_remaining = p;
            if p == 0 {
                self.ready.push(ready_key(&self.nodes[idx], idx as u32));
            }
        }
        self.len += n;
        self.remaining += n;
    }

    pub fn is_empty(&self) -> bool {
        self.remaining == 0
    }

    pub fn order_ref(&self, idx: u32) -> NodeOrderRef {
        self.nodes[idx as usize].order_ref
    }

    pub fn take_ready(&mut self) -> Option<u32> {
        self.release_cooled();
        let (.., idx) = self.ready.pop()?;
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
                ready.push(ready_key(node, idx));
            }
            false
        });
    }

    /// Live orders never sent to a worker.
    pub fn undispatched(
        &self,
    ) -> impl Iterator<Item = (NodeOrderRef, (SlotNum, MiniBlockUuid), Nanos)> + '_ {
        self.nodes[..self.len].iter().filter(|n| n.live && !n.in_flight).map(|n| {
            let g = &self.graphs[n.graph as usize];
            (n.order_ref, g.id, g.received_at)
        })
    }

    pub fn apply_results(
        &mut self,
        idx: u32,
        results: &[ExecutionResult],
        has_other_in_flight: bool,
    ) {
        if should_retry(results, has_other_in_flight) {
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
                self.ready.push(ready_key(sn, s));
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
        self.graphs.clear();
        self.chain = None;
    }

    pub fn on_new_slot(&mut self) {
        self.clear();
        self.last_graph_index = None;
    }
}

/// Earlier graphs first, then deepest critical path. An old node's depth
/// misses the successors later increments hang on it, so depths only compare
/// within a graph.
type ReadyKey = (Reverse<u32>, u32, u32);

fn ready_key(node: &Node, idx: u32) -> ReadyKey {
    (Reverse(node.graph), node.depth, idx)
}

struct LoadedGraph {
    id: (SlotNum, MiniBlockUuid),
    received_at: Nanos,
}

fn should_retry(results: &[ExecutionResult], has_other_in_flight: bool) -> bool {
    results.iter().any(|r| match r {
        ExecutionResult::NotIncluded(
            NotIncludedReason::BANK_NOT_AVAILABLE | NotIncludedReason::ACCOUNT_IN_USE,
        ) => true,
        // Other in-flight batches can release unused cost reservations on commit.
        // Without them, retrying blocks the graph without making progress.
        ExecutionResult::NotIncluded(
            NotIncludedReason::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT |
            NotIncludedReason::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT,
        ) => has_other_in_flight,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use gravity_types::wire::{CsrNode, PriorEdge};

    use super::*;

    /// `edges` are `(from, to)` within the graph.
    fn graph(
        index_in_slot: u32,
        sigs: &[u8],
        edges: &[(u32, u32)],
        link: Option<GraphLink>,
    ) -> ValidatedGraph {
        ValidatedGraph::new(wire(index_in_slot, sigs, edges), link).ok().unwrap()
    }

    fn wire(index_in_slot: u32, sigs: &[u8], edges: &[(u32, u32)]) -> WireMiniBlockGraph {
        let mut row_offsets = vec![0];
        let mut col_indices = Vec::new();
        for i in 0..sigs.len() as u32 {
            col_indices.extend(edges.iter().filter(|(f, _)| *f == i).map(|(_, t)| *t));
            row_offsets.push(col_indices.len() as u32);
        }
        let nodes = sigs
            .iter()
            .map(|&s| CsrNode {
                order_ref: NodeOrderRef::Tx { sig: SigPrefix::new([s; 16]) },
                depth: 1,
            })
            .collect();
        WireMiniBlockGraph {
            slot: 7,
            index_in_slot,
            uuid: MiniBlockUuid::new(),
            row_offsets,
            col_indices,
            nodes,
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn link(base: u32, prior: &[(u32, u32)]) -> Option<GraphLink> {
        Some(GraphLink {
            base,
            prior_edges: prior.iter().map(|&(from, to)| PriorEdge { from, to }).collect(),
        })
    }

    fn sig(dag: &Dag, idx: u32) -> u8 {
        let NodeOrderRef::Tx { sig } = dag.order_ref(idx) else { unreachable!() };
        sig.0[0]
    }

    fn drain_ready(dag: &mut Dag) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(idx) = dag.take_ready() {
            out.push(sig(dag, idx));
        }
        out.sort_unstable();
        out
    }

    fn complete_sig(dag: &mut Dag, s: u8) {
        let idx = (0..dag.len as u32).find(|&i| sig(dag, i) == s).unwrap();
        dag.complete(idx);
    }

    /// An increment's node waits only on its live prior predecessor; the rest
    /// start while the earlier graph is still running.
    #[test]
    fn increment_waits_only_on_live_predecessors() {
        let mut dag = Dag::new();
        // chain nodes 0 (sig 1) -> 1 (sig 2)
        let first = graph(3, &[1, 2], &[(0, 1)], link(0, &[]));
        dag.load(Nanos::ZERO, &first);
        assert_eq!(drain_ready(&mut dag), [1]);

        // chain 2 (sig 3) after chain 1; chain 3 (sig 4) after chain 0, done
        // before it arrives; chain 4 (sig 5) free.
        complete_sig(&mut dag, 1);
        assert_eq!(drain_ready(&mut dag), [2]);
        let second = graph(3, &[3, 4, 5], &[], link(2, &[(1, 0), (0, 1)]));
        assert!(dag.extends(&second));
        dag.append(Nanos::ZERO, &second);
        assert_eq!(drain_ready(&mut dag), [4, 5]);

        complete_sig(&mut dag, 2);
        assert_eq!(drain_ready(&mut dag), [3]);
        for s in [3, 4, 5] {
            complete_sig(&mut dag, s);
        }
        assert!(dag.is_empty());

        let idx_of = |dag: &Dag, s| (0..dag.len as u32).find(|&i| sig(dag, i) == s).unwrap();
        assert_eq!(dag.node_graph(idx_of(&dag, 2)).0, (7, first.uuid));
        assert_eq!(dag.node_graph(idx_of(&dag, 3)).0, (7, second.uuid));
        assert!(dag.owns(idx_of(&dag, 2), (7, first.uuid)));
        assert!(!dag.owns(idx_of(&dag, 2), (7, second.uuid)));
    }

    /// Earlier increments go first, whatever the later ones' depths.
    #[test]
    fn earlier_increments_go_first() {
        let mut dag = Dag::new();
        dag.load(Nanos::ZERO, &graph(3, &[1], &[], link(0, &[])));
        let mut deep = wire(3, &[2], &[]);
        deep.nodes[0].depth = 9;
        dag.append(Nanos::ZERO, &ValidatedGraph::new(deep, link(1, &[])).ok().unwrap());
        let first = dag.take_ready().unwrap();
        assert_eq!(sig(&dag, first), 1);
    }

    /// An increment after a gap waits; edges from before the first loaded
    /// node are satisfied.
    #[test]
    fn gap_waits_and_earlier_edges_are_satisfied() {
        let mut dag = Dag::new();
        dag.load(Nanos::ZERO, &graph(3, &[1], &[], link(0, &[])));
        // chain 1..3 never came.
        let late = graph(3, &[9], &[], link(3, &[(2, 0)]));
        assert!(!dag.extends(&late));

        complete_sig(&mut dag, 1);
        dag.load(Nanos::ZERO, &late);
        assert_eq!(drain_ready(&mut dag), [9]);
        assert!(dag.extends(&graph(3, &[10], &[], link(4, &[(3, 0)]))));
    }

    /// Only increments of the loaded round extend it.
    #[test]
    fn extends_only_its_round_or_the_one_before() {
        let mut dag = Dag::new();
        dag.load(Nanos::ZERO, &graph(3, &[1], &[], link(0, &[])));
        assert!(dag.extends(&graph(3, &[2], &[], link(1, &[]))));
        assert!(dag.extends(&graph(4, &[2], &[], link(1, &[]))));
        assert!(!dag.extends(&graph(5, &[2], &[], link(1, &[]))));
        assert!(!dag.extends(&graph(2, &[2], &[], link(1, &[]))));
        assert!(!dag.extends(&graph(3, &[2], &[], None)));
    }

    /// A tail's first increment extends the round it follows while that runs,
    /// waiting only on the orders it names; later increments follow the tail.
    #[test]
    fn tail_extends_the_round_before() {
        let mut dag = Dag::new();
        // Round 2, as a plain graph: 1 -> 2.
        dag.load(Nanos::ZERO, &graph(2, &[1, 2], &[(0, 1)], None));
        assert_eq!(drain_ready(&mut dag), [1]);

        // Its tail: 3 after chain 1 (sig 2), 4 free.
        let first = graph(3, &[3, 4], &[], link(2, &[(1, 0)]));
        assert!(dag.extends(&first));
        dag.append(Nanos::ZERO, &first);
        assert_eq!(drain_ready(&mut dag), [4]);
        // The tail's next increment; round 2 no longer extends.
        assert!(dag.extends(&graph(3, &[5], &[], link(4, &[]))));
        assert!(!dag.extends(&graph(2, &[5], &[], link(4, &[]))));

        complete_sig(&mut dag, 1);
        assert_eq!(drain_ready(&mut dag), [2]);
        complete_sig(&mut dag, 2);
        assert_eq!(drain_ready(&mut dag), [3]);
    }

    /// Two plain rounds never chain.
    #[test]
    fn plain_rounds_do_not_chain() {
        let mut dag = Dag::new();
        dag.load(Nanos::ZERO, &graph(2, &[1], &[], None));
        assert!(!dag.extends(&graph(3, &[2], &[], None)));
    }

    #[test]
    fn rejects_bad_links() {
        let g = |link| ValidatedGraph::new(wire(3, &[1], &[]), link).is_ok();
        assert!(g(link(2, &[(1, 0)])));
        // from must be in an earlier graph, to in this one.
        assert!(!g(link(2, &[(2, 0)])));
        assert!(!g(link(2, &[(1, 1)])));
        assert!(!g(link(u32::MAX, &[])));
    }
}
