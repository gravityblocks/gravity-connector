//! Connection-status metrics, served on `metrics_addr` as `/metrics` and
//! `/health`. Healthy means the validator and a relay are both connected.

#![allow(
    clippy::disallowed_types,
    reason = "prometheus macros use std::collections::HashMap internally"
)]

use std::{
    net::SocketAddr,
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
};

use axum::{
    body::Body,
    http::{StatusCode, header::CONTENT_TYPE},
    response::Response,
    routing::get,
};
use flux::timing::Nanos;
use gravity_types::Metadata;
use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry, TextEncoder,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry,
};
use tokio::net::TcpListener;
use tracing::{error, info};

const AGAVE_PROGRESS_TIMEOUT: Nanos = Nanos::from_secs(2);

pub static REGISTRY: LazyLock<Registry> =
    LazyLock::new(|| Registry::new_custom(Some("gravity".to_owned()), None).unwrap());

static INFO: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec_with_registry!(
        "connector_info",
        "Build and instance info, always 1",
        &["version", "commit", "branch", "instance", "identity", "client_variant"],
        REGISTRY
    )
    .unwrap()
});

/// The same verdict `/health` returns, for alerting off a scrape.
static HEALTHY: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_healthy",
        "1 while the validator and a relay are both connected and no failsafe is active",
        REGISTRY
    )
    .unwrap()
});

pub static READY: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_ready",
        "1 once the network and bridge tiles are running",
        REGISTRY
    )
    .unwrap()
});

pub static FAILSAFE_ACTIVE: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_failsafe_active",
        "1 while a failsafe is holding the connector back from starting",
        REGISTRY
    )
    .unwrap()
});

pub static SHMEM_OUTSTANDING_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_shmem_outstanding_bytes",
        "Shared-memory bytes allocated by the connector's allocator handle and not yet freed (size-class rounded; excludes agave-side allocations)",
        REGISTRY
    )
    .unwrap()
});

pub static RELAY_CONNECTED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_relay_connected",
        "1 while an authenticated relay is the active connection",
        REGISTRY
    )
    .unwrap()
});

pub static RELAYS_CONFIGURED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_relays_configured",
        "Relays the connector dials",
        REGISTRY
    )
    .unwrap()
});

pub static RELAY_CONNECTS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_with_registry!(
        "connector_relay_connects_total",
        "Relay connections that completed the bootstrap handshake",
        REGISTRY
    )
    .unwrap()
});

pub static RELAY_DISCONNECTS: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter_with_registry!(
        "connector_relay_disconnects_total",
        "Relay connections lost",
        REGISTRY
    )
    .unwrap()
});

static AGAVE_CONNECTED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_agave_connected",
        "1 while the validator is sending progress messages",
        REGISTRY
    )
    .unwrap()
});

pub static BLOCK_ENGINES_CONFIGURED: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge_with_registry!(
        "connector_block_engines_configured",
        "Jito block engine upstreams the connector subscribes to",
        REGISTRY
    )
    .unwrap()
});

static BLOCK_ENGINE_CONNECTED: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec_with_registry!(
        "connector_block_engine_connected",
        "1 while this block engine has live packet and bundle streams",
        &["url"],
        REGISTRY
    )
    .unwrap()
});

static BLOCK_ENGINE_RECONNECTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "connector_block_engine_reconnects_total",
        "Times the connector had to re-establish this block engine's streams",
        &["url"],
        REGISTRY
    )
    .unwrap()
});

static BLOCK_ENGINES_CONNECTED: AtomicI64 = AtomicI64::new(0);

pub struct BlockEngineMetrics {
    connected: IntGauge,
    reconnects: IntCounter,
    is_connected: AtomicBool,
}

impl BlockEngineMetrics {
    pub fn new(url: &str) -> Self {
        Self {
            connected: BLOCK_ENGINE_CONNECTED.with_label_values(&[url]),
            reconnects: BLOCK_ENGINE_RECONNECTS.with_label_values(&[url]),
            is_connected: AtomicBool::new(false),
        }
    }

    pub fn set_connected(&self, connected: bool) {
        if self.is_connected.swap(connected, Ordering::Relaxed) == connected {
            return;
        }
        self.connected.set(i64::from(connected));
        if connected {
            BLOCK_ENGINES_CONNECTED.fetch_add(1, Ordering::Relaxed);
        } else {
            BLOCK_ENGINES_CONNECTED.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn record_reconnect(&self) {
        self.reconnects.inc();
    }
}

/// Wall-clock nanos, 0 until first set.
static LAST_AGAVE_PROGRESS: AtomicU64 = AtomicU64::new(0);
static STARTED_AT: AtomicU64 = AtomicU64::new(0);

pub fn record_agave_progress() {
    LAST_AGAVE_PROGRESS.store(Nanos::now().0, Ordering::Relaxed);
}

pub fn set_info(instance: &str, identity: &str, client_variant: &str) {
    let meta = Metadata::get();
    INFO.with_label_values(&[
        meta.version,
        meta.commit.trim(),
        meta.branch.trim(),
        instance,
        identity,
        client_variant,
    ])
    .set(1);
}

fn init_metrics() {
    LazyLock::force(&HEALTHY);
    LazyLock::force(&READY);
    LazyLock::force(&FAILSAFE_ACTIVE);

    LazyLock::force(&SHMEM_OUTSTANDING_BYTES);

    LazyLock::force(&RELAY_CONNECTED);
    LazyLock::force(&RELAYS_CONFIGURED);
    LazyLock::force(&RELAY_CONNECTS);
    LazyLock::force(&RELAY_DISCONNECTS);

    LazyLock::force(&AGAVE_CONNECTED);

    LazyLock::force(&BLOCK_ENGINES_CONFIGURED);
}

pub fn spawn_metrics(address: SocketAddr) {
    init_metrics();
    STARTED_AT.store(Nanos::now().0, Ordering::Relaxed);

    let spawned = std::thread::Builder::new().name("metrics".into()).spawn(move || {
        match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt.block_on(serve(address)),
            Err(err) => error!(?err, "failed to build metrics runtime; metrics disabled"),
        }
    });
    if let Err(err) = spawned {
        error!(?err, "failed to spawn metrics thread; metrics disabled");
    }
}

async fn serve(address: SocketAddr) {
    let router = axum::Router::new()
        .route("/metrics", get(handle_metrics))
        .route("/health", get(handle_health));

    let listener = match TcpListener::bind(&address).await {
        Ok(listener) => listener,
        Err(err) => {
            error!(?err, %address, "failed to bind metrics listener; metrics disabled");
            return;
        }
    };
    info!(%address, "metrics server listening");

    if let Err(err) = axum::serve(listener, router).await {
        error!(?err, "metrics server stopped");
    }
}

async fn handle_metrics() -> Response {
    // Both hang off a timestamp, so they are only true at the instant of the read.
    let health = Health::read();
    HEALTHY.set(i64::from(health.healthy));
    AGAVE_CONNECTED.set(i64::from(health.agave_connected));

    let encoder = TextEncoder::new();
    let body = encoder.encode_to_string(&REGISTRY.gather()).unwrap_or_default();

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(body))
        .expect("metrics response builds")
}

async fn handle_health() -> Response {
    let health = Health::read();
    let status = if health.healthy { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let body = serde_json::to_string_pretty(&health).unwrap_or_default();

    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("health response builds")
}

fn elapsed_since(stamp: &AtomicU64) -> Nanos {
    match stamp.load(Ordering::Relaxed) {
        0 => Nanos::ZERO,
        at => Nanos(at).elapsed_saturating(),
    }
}

/// Block engines, and relays beyond the active one, are reported but kept out
/// of the verdict: the connector still works with every one of them down.
#[derive(serde::Serialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "a wire summary of independent checks, not a state machine"
)]
struct Health {
    healthy: bool,
    state: &'static str,
    version: &'static str,
    uptime_seconds: f64,
    agave_connected: bool,
    agave_progress_age_seconds: Option<f64>,
    relay_connected: bool,
    block_engines_connected: i64,
    block_engines_configured: i64,
    failsafe_active: bool,
}

impl Health {
    fn read() -> Self {
        let ready = READY.get() == 1;
        let failsafe_active = FAILSAFE_ACTIVE.get() == 1;
        let relay_connected = RELAY_CONNECTED.get() == 1;
        // A zero stamp means nothing has ever arrived; reading that as age zero
        // would report the freshest possible link for a silent validator.
        let progress_age = match LAST_AGAVE_PROGRESS.load(Ordering::Relaxed) {
            0 => None,
            at => Some(Nanos(at).elapsed_saturating()),
        };
        let agave_connected =
            ready && progress_age.is_some_and(|age| age <= AGAVE_PROGRESS_TIMEOUT);

        let state = if failsafe_active {
            "failsafe"
        } else if ready {
            "running"
        } else {
            "starting"
        };

        Self {
            healthy: agave_connected && relay_connected && !failsafe_active,
            state,
            version: Metadata::get().version,
            uptime_seconds: elapsed_since(&STARTED_AT).as_secs(),
            agave_connected,
            agave_progress_age_seconds: progress_age.map(|age| age.as_secs()),
            relay_connected,
            block_engines_connected: BLOCK_ENGINES_CONNECTED.load(Ordering::Relaxed),
            block_engines_configured: BLOCK_ENGINES_CONFIGURED.get(),
            failsafe_active,
        }
    }
}
