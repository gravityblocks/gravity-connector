use flux_network::udp_v2::TelemetryEvent;

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
    pub(crate) fn record(&mut self, event: TelemetryEvent) {
        match event {
            TelemetryEvent::Send(send) => {
                self.datagrams += u64::from(send.datagrams);
                self.bytes += u64::from(send.bytes);
            }
            TelemetryEvent::Retransmit(resent) => {
                self.retransmits += 1;
                self.retransmit_bytes += u64::from(resent.bytes);
                self.max_retries = self.max_retries.max(u64::from(resent.retries));
            }
            TelemetryEvent::Latency(_) => {}
            TelemetryEvent::Lapped => self.telemetry_lapped += 1,
        }
    }

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
