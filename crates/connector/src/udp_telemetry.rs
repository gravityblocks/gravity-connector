use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use flux::{
    communication::{
        ReadError,
        queue::{ConsumerBare, Queue},
    },
    utils::directories::{local_share_dir, shmem_dir_queues_with_base},
};
use flux_network::{
    Token,
    udp_v2::{UdpRetransmit, UdpSend},
};
use tracing::warn;

use crate::APP_NAME;

pub(crate) const FLOW_GROUP_NAME: &str = "relay_flow";

#[derive(Default)]
pub(crate) struct UdpFlowStats {
    datagrams: u64,
    bytes: u64,
    retransmits: u64,
    retransmit_bytes: u64,
    max_retries: u64,
    telemetry_lapped: u64,
}

impl UdpFlowStats {
    pub(crate) fn wire(&self) -> [(&'static str, u64); 6] {
        [
            ("udp_datagrams", self.datagrams),
            ("udp_bytes", self.bytes),
            ("udp_retransmits", self.retransmits),
            ("udp_retransmit_bytes", self.retransmit_bytes),
            ("udp_max_retries", self.max_retries),
            ("udp_telemetry_lapped", self.telemetry_lapped),
        ]
    }
}

pub(crate) struct FlowTelemetry {
    sends: Option<ConsumerBare<UdpSend>>,
    retransmits: Option<ConsumerBare<UdpRetransmit>>,
    files: [PathBuf; 4],
}

impl FlowTelemetry {
    // this discovery part is a bit annoying
    // TODO: fix
    pub(crate) fn open(token: Token, addr: SocketAddr) -> Self {
        let dir = shmem_dir_queues_with_base(local_share_dir(), APP_NAME);
        let label = format!("{FLOW_GROUP_NAME}_{}_{addr}", token.0);
        let files = [
            dir.join(format!("udp_sends_{label}")),
            dir.join(format!("udp_retransmits_{label}")),
            dir.join(format!("timing-udp_latency_{label}")),
            dir.join(format!("latency-udp_latency_{label}")),
        ];
        Self { sends: subscribe(&files[0]), retransmits: subscribe(&files[1]), files }
    }

    pub(crate) fn drain(&mut self, stats: &mut UdpFlowStats) {
        if let Some(sends) = &mut self.sends {
            drain(sends, &mut stats.telemetry_lapped, |send: &UdpSend| {
                stats.datagrams += u64::from(send.datagrams);
                stats.bytes += u64::from(send.bytes);
            });
        }
        if let Some(retransmits) = &mut self.retransmits {
            drain(retransmits, &mut stats.telemetry_lapped, |resent: &UdpRetransmit| {
                stats.retransmits += 1;
                stats.retransmit_bytes += u64::from(resent.bytes);
                stats.max_retries = stats.max_retries.max(u64::from(resent.retries));
            });
        }
    }
}

impl Drop for FlowTelemetry {
    fn drop(&mut self) {
        for file in &self.files {
            let _ = std::fs::remove_file(file);
        }
    }
}

fn subscribe<T: Copy>(file: &Path) -> Option<ConsumerBare<T>> {
    match Queue::try_open_shared(file) {
        Ok(queue) => {
            let mut consumer = ConsumerBare::new(queue, "connector");
            // From the write head: a token number reused from an earlier run
            // may have left events behind.
            consumer.subscribe_broadcast();
            Some(consumer)
        }
        Err(err) => {
            warn!(?err, file = %file.display(), "no UDP flow telemetry");
            None
        }
    }
}

fn drain<T: Copy + Default>(
    consumer: &mut ConsumerBare<T>,
    lapped: &mut u64,
    mut on_event: impl FnMut(&T),
) {
    let mut event = T::default();
    loop {
        match consumer.try_consume(&mut event) {
            Ok(()) => on_event(&event),
            Err(ReadError::SpedPast) => {
                *lapped += 1;
                consumer.recover_after_error();
            }
            Err(ReadError::Empty) => return,
        }
    }
}
