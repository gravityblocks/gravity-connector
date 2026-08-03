use std::{io, net::SocketAddr};

use flux::timing::{Duration, Instant};
use gravity_types::runtime::background_runtime;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tracing::{info, warn};

use crate::RelayEndpoint;

const DNS_REFRESH_INTERVAL_SECS: u64 = 60;
const DNS_RETRY_INTERVAL_SECS: u64 = 5;

struct ResolutionResult {
    result: io::Result<Vec<SocketAddr>>,
}

/// Non-blocking DNS cache and address selector for one logical relay endpoint.
pub(crate) struct DomainHandle {
    endpoint: RelayEndpoint,
    addresses: Vec<SocketAddr>,
    next_index: usize,
    last_returned: Option<SocketAddr>,
    last_attempt: Option<Instant>,
    last_success: Option<Instant>,
    pending: bool,
    refresh_requested: bool,
    result_tx: Sender<ResolutionResult>,
    result_rx: Receiver<ResolutionResult>,
}

impl DomainHandle {
    pub(crate) fn new(endpoint: RelayEndpoint) -> Self {
        let (result_tx, result_rx) = mpsc::channel(1);
        let addresses = endpoint
            .ip_addr()
            .map(|ip| vec![SocketAddr::new(ip, endpoint.port())])
            .unwrap_or_default();
        Self {
            endpoint,
            addresses,
            next_index: 0,
            last_returned: None,
            last_attempt: None,
            last_success: None,
            pending: false,
            refresh_requested: false,
            result_tx,
            result_rx,
        }
    }

    pub(crate) fn endpoint(&self) -> &RelayEndpoint {
        &self.endpoint
    }

    /// Applies completed lookups and starts a refresh when the cache is
    /// missing, stale, or explicitly marked for refresh. This method never
    /// blocks.
    pub(crate) fn poll(&mut self) {
        while let Ok(result) = self.result_rx.try_recv() {
            self.pending = false;
            match result.result {
                Ok(addresses) if !addresses.is_empty() => {
                    self.apply_addresses(addresses);
                    self.last_success = Some(Instant::now());
                }
                Ok(_) => {
                    warn!(endpoint = %self.endpoint, "relay DNS lookup returned no addresses");
                }
                Err(err) => {
                    warn!(endpoint = %self.endpoint, ?err, "failed resolving relay endpoint");
                }
            }
        }

        if self.endpoint.ip_addr().is_some() || self.pending {
            return;
        }

        let retry_elapsed = self
            .last_attempt
            .is_none_or(|last| last.elapsed() >= Duration::from_secs(DNS_RETRY_INTERVAL_SECS));
        let cache_stale = self.addresses.is_empty() ||
            self.last_success.is_none_or(|last| {
                last.elapsed() >= Duration::from_secs(DNS_REFRESH_INTERVAL_SECS)
            });
        if retry_elapsed && (self.refresh_requested || cache_stale) {
            self.start_resolution();
        }
    }

    /// Requests an early refresh. The next [`Self::poll`] starts it, subject to
    /// retry rate limiting. Cached addresses remain available in the meantime.
    pub(crate) fn request_refresh(&mut self) {
        if self.endpoint.ip_addr().is_none() {
            self.refresh_requested = true;
        }
    }

    /// Returns and advances to the next cached address. This has no DNS or
    /// scheduling side effects.
    pub(crate) fn next_addr(&mut self) -> Option<SocketAddr> {
        if self.addresses.is_empty() {
            return None;
        }
        let addr = self.addresses[self.next_index % self.addresses.len()];
        self.next_index = (self.next_index + 1) % self.addresses.len();
        self.last_returned = Some(addr);
        Some(addr)
    }

    fn start_resolution(&mut self) {
        self.pending = true;
        self.refresh_requested = false;
        self.last_attempt = Some(Instant::now());
        let endpoint = self.endpoint.clone();
        let tx = self.result_tx.clone();
        background_runtime().spawn(async move {
            let result = resolve_endpoint(&endpoint).await;
            let _ = tx.send(ResolutionResult { result }).await;
        });
    }

    fn apply_addresses(&mut self, addresses: Vec<SocketAddr>) {
        if addresses != self.addresses {
            info!(endpoint = %self.endpoint, ?addresses, "resolved relay endpoint");
        }
        self.next_index = self
            .last_returned
            .and_then(|last| addresses.iter().position(|addr| *addr == last))
            .map_or(0, |idx| (idx + 1) % addresses.len());
        self.addresses = addresses;
    }
}

async fn resolve_endpoint(endpoint: &RelayEndpoint) -> io::Result<Vec<SocketAddr>> {
    let resolved = tokio::net::lookup_host((endpoint.host(), endpoint.port())).await?;
    let mut addresses = Vec::new();
    for addr in resolved {
        if !addresses.contains(&addr) {
            addresses.push(addr);
        }
    }
    Ok(addresses)
}
