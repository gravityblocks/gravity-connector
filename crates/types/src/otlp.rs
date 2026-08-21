use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{
    Resource,
    logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider},
};
use reqwest::header::{
    CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST, HeaderMap, HeaderName,
    HeaderValue, TE, TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use rustc_hash::FxHashMap;
use tracing_subscriber::{
    Registry,
    filter::{LevelFilter, Targets},
    layer::Layer,
    reload,
};
use url::Url;

use crate::wire::{OtlpLogExporterConfig, OtlpLogLevel};

const MAX_ENDPOINT_LEN: usize = 2_048;
const MAX_HEADERS: usize = 32;
const MAX_HEADER_KEY_LEN: usize = 128;
const MAX_HEADER_VALUE_LEN: usize = 4_096;
const MAX_RESOURCE_ATTRIBUTES: usize = 32;
const MAX_RESOURCE_KEY_LEN: usize = 128;
const MAX_RESOURCE_VALUE_LEN: usize = 1_024;

const LOG_QUEUE_CAPACITY: usize = 2_048;
const LOG_BATCH_CAPACITY: usize = 256;
const LOG_SCHEDULE_DELAY: Duration = Duration::from_millis(500);
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);

type DynamicOtlpLayer = Box<dyn Layer<Registry> + Send + Sync>;
pub(crate) type ReloadableOtlpLayer = reload::Layer<Option<DynamicOtlpLayer>, Registry>;

#[derive(Debug, thiserror::Error)]
pub enum OtlpLogConfigError {
    #[error("OTLP log export is disabled by local configuration")]
    LocallyDisabled,
    #[error("invalid OTLP logs endpoint")]
    InvalidEndpoint,
    #[error("too many OTLP HTTP headers")]
    TooManyHeaders,
    #[error("invalid, duplicate, or reserved OTLP HTTP header")]
    InvalidHeader,
    #[error("too many OTLP resource attributes")]
    TooManyResourceAttributes,
    #[error("invalid, duplicate, or reserved OTLP resource attribute")]
    InvalidResourceAttribute,
    #[error("failed to build OTLP HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error("failed to build OTLP log exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
    #[error("failed to reload OTLP log exporter")]
    Reload,
}

/// Replaces the dormant OTLP layer installed by [`crate::init_tracing_log`].
///
/// The relay owns the active configuration. `None` removes the layer; a
/// subsequent `Some` installs a fresh provider with its own bounded queue and
/// export worker.
pub struct OtlpLogHandle {
    locally_disabled: bool,
    reload: reload::Handle<Option<DynamicOtlpLayer>, Registry>,
    provider: Option<SdkLoggerProvider>,
}

impl OtlpLogHandle {
    /// Applies configuration supplied by the active relay. `None` disables export.
    pub fn configure(
        &mut self,
        config: Option<OtlpLogExporterConfig>,
    ) -> Result<(), OtlpLogConfigError> {
        let Some(config) = config else {
            self.reload.reload(None).map_err(|_| OtlpLogConfigError::Reload)?;
            self.stop_previous();
            return Ok(());
        };

        if self.locally_disabled {
            return Err(OtlpLogConfigError::LocallyDisabled);
        }

        let config = ValidatedConfig::try_from(config)?;
        let (layer, provider) = build_otlp_layer(config)?;

        if self.reload.reload(Some(layer)).is_err() {
            spawn_shutdown(provider);
            return Err(OtlpLogConfigError::Reload);
        }

        let previous = self.provider.replace(provider);
        if let Some(previous) = previous {
            spawn_shutdown(previous);
        }
        Ok(())
    }

    fn stop_previous(&mut self) {
        if let Some(previous) = self.provider.take() {
            spawn_shutdown(previous);
        }
    }
}

pub(crate) fn new_otlp_log_layer(locally_disabled: bool) -> (ReloadableOtlpLayer, OtlpLogHandle) {
    let (layer, reload) = reload::Layer::new(None);
    let handle = OtlpLogHandle { locally_disabled, reload, provider: None };
    (layer, handle)
}

fn build_otlp_layer(
    config: ValidatedConfig,
) -> Result<(DynamicOtlpLayer, SdkLoggerProvider), OtlpLogConfigError> {
    let client = reqwest::blocking::Client::builder()
        .default_headers(config.headers)
        .timeout(EXPORT_TIMEOUT)
        .build()?;
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(config.endpoint)
        .with_timeout(EXPORT_TIMEOUT)
        .with_http_client(client)
        .build()?;

    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(LOG_QUEUE_CAPACITY)
        .with_max_export_batch_size(LOG_BATCH_CAPACITY)
        .with_scheduled_delay(LOG_SCHEDULE_DELAY)
        .build();
    let processor = BatchLogProcessor::builder(exporter).with_batch_config(batch_config).build();
    let resource = Resource::builder_empty()
        .with_service_name("gravity-connector")
        .with_attributes(
            config.resource_attributes.into_iter().map(|(key, value)| KeyValue::new(key, value)),
        )
        .build();
    let provider =
        SdkLoggerProvider::builder().with_log_processor(processor).with_resource(resource).build();
    let layer = otlp_layer(&provider, config.level);
    Ok((layer, provider))
}

fn otlp_layer(provider: &SdkLoggerProvider, level: OtlpLogLevel) -> DynamicOtlpLayer {
    OpenTelemetryTracingBridge::new(provider).with_filter(otlp_filter(level)).boxed()
}

fn spawn_shutdown(provider: SdkLoggerProvider) {
    crate::runtime::background_runtime().spawn_blocking(move || {
        if let Err(err) = provider.shutdown_with_timeout(EXPORT_TIMEOUT) {
            eprintln!("failed to shut down OTLP log exporter: {err}");
        }
    });
}

fn level_filter(level: OtlpLogLevel) -> LevelFilter {
    match level {
        OtlpLogLevel::Error => LevelFilter::ERROR,
        OtlpLogLevel::Warn => LevelFilter::WARN,
        OtlpLogLevel::Info => LevelFilter::INFO,
        OtlpLogLevel::Debug => LevelFilter::DEBUG,
        OtlpLogLevel::Trace => LevelFilter::TRACE,
    }
}

fn otlp_filter(level: OtlpLogLevel) -> Targets {
    // Transport and SDK diagnostics remain visible in the local sinks but must
    // never feed back into the exporter that produced them.
    Targets::new()
        .with_default(level_filter(level))
        .with_target("opentelemetry", LevelFilter::OFF)
        .with_target("reqwest", LevelFilter::OFF)
        .with_target("hyper", LevelFilter::OFF)
        .with_target("h2", LevelFilter::OFF)
        .with_target("rustls", LevelFilter::OFF)
        .with_target("tower", LevelFilter::OFF)
}

struct ValidatedConfig {
    endpoint: String,
    level: OtlpLogLevel,
    headers: HeaderMap,
    resource_attributes: FxHashMap<String, String>,
}

impl TryFrom<OtlpLogExporterConfig> for ValidatedConfig {
    type Error = OtlpLogConfigError;

    fn try_from(config: OtlpLogExporterConfig) -> Result<Self, Self::Error> {
        validate_endpoint(&config.endpoint)?;

        if config.headers.len() > MAX_HEADERS {
            return Err(OtlpLogConfigError::TooManyHeaders);
        }
        let mut headers = HeaderMap::with_capacity(config.headers.len());
        for header in config.headers {
            if header.key.is_empty()
                || header.key.len() > MAX_HEADER_KEY_LEN
                || header.value.len() > MAX_HEADER_VALUE_LEN
            {
                return Err(OtlpLogConfigError::InvalidHeader);
            }
            let name = HeaderName::from_bytes(header.key.as_bytes())
                .map_err(|_| OtlpLogConfigError::InvalidHeader)?;
            let mut value = HeaderValue::from_str(&header.value)
                .map_err(|_| OtlpLogConfigError::InvalidHeader)?;
            if reserved_header(&name) || headers.contains_key(&name) {
                return Err(OtlpLogConfigError::InvalidHeader);
            }
            value.set_sensitive(true);
            headers.insert(name, value);
        }

        if config.resource_attributes.len() > MAX_RESOURCE_ATTRIBUTES {
            return Err(OtlpLogConfigError::TooManyResourceAttributes);
        }
        let mut resource_attributes = FxHashMap::default();
        resource_attributes.reserve(config.resource_attributes.len());
        for attribute in config.resource_attributes {
            if !valid_resource_key(&attribute.key)
                || attribute.value.len() > MAX_RESOURCE_VALUE_LEN
                || resource_attributes.contains_key(&attribute.key)
            {
                return Err(OtlpLogConfigError::InvalidResourceAttribute);
            }
            resource_attributes.insert(attribute.key, attribute.value);
        }

        Ok(Self { endpoint: config.endpoint, level: config.level, headers, resource_attributes })
    }
}

fn validate_endpoint(endpoint: &str) -> Result<(), OtlpLogConfigError> {
    if endpoint.len() > MAX_ENDPOINT_LEN {
        return Err(OtlpLogConfigError::InvalidEndpoint);
    }
    let url = Url::parse(endpoint).map_err(|_| OtlpLogConfigError::InvalidEndpoint)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(OtlpLogConfigError::InvalidEndpoint);
    }
    Ok(())
}

fn valid_resource_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_RESOURCE_KEY_LEN
        && key != "service.name"
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
}

fn reserved_header(name: &HeaderName) -> bool {
    name == CONTENT_TYPE
        || name == CONTENT_ENCODING
        || name == CONTENT_LENGTH
        || name == HOST
        || name == CONNECTION
        || name == TRANSFER_ENCODING
        || name == TE
        || name == TRAILER
        || name == UPGRADE
}

#[cfg(test)]
mod tests {
    use std::{
        hint::black_box,
        io::{Read, Write},
        net::TcpListener,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Instant,
    };

    use opentelemetry_sdk::{
        error::OTelSdkResult,
        logs::{LogBatch, LogExporter},
    };
    use tracing::{Dispatch, Subscriber};
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::wire::{OtlpHeader, OtlpResourceAttribute};

    fn config() -> OtlpLogExporterConfig {
        OtlpLogExporterConfig {
            endpoint: "https://collector.example.com/v1/logs".to_owned(),
            level: OtlpLogLevel::Info,
            headers: vec![OtlpHeader {
                key: "authorization".to_owned(),
                value: "Bearer secret".to_owned(),
            }],
            resource_attributes: vec![OtlpResourceAttribute {
                key: "deployment.region".to_owned(),
                value: "eu-west".to_owned(),
            }],
        }
    }

    #[test]
    fn validates_endpoint_headers_and_resource_attributes() {
        ValidatedConfig::try_from(config()).unwrap();

        let mut invalid = config();
        invalid.endpoint = "ftp://collector.example.com/v1/logs".to_owned();
        assert!(matches!(
            ValidatedConfig::try_from(invalid),
            Err(OtlpLogConfigError::InvalidEndpoint)
        ));

        let mut reserved_header = config();
        reserved_header.headers[0].key = "content-type".to_owned();
        assert!(matches!(
            ValidatedConfig::try_from(reserved_header),
            Err(OtlpLogConfigError::InvalidHeader)
        ));

        let mut reserved_attribute = config();
        reserved_attribute.resource_attributes[0].key = "service.name".to_owned();
        assert!(matches!(
            ValidatedConfig::try_from(reserved_attribute),
            Err(OtlpLogConfigError::InvalidResourceAttribute)
        ));
    }

    #[test]
    fn local_kill_switch_rejects_enable() {
        let (_, mut handle) = new_otlp_log_layer(true);
        assert!(matches!(
            handle.configure(Some(config())),
            Err(OtlpLogConfigError::LocallyDisabled)
        ));
    }

    struct CountingLayer(Arc<AtomicUsize>);

    impl<S: Subscriber> Layer<S> for CountingLayer {
        fn on_event(
            &self,
            _event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn disabled_reload_layer_is_neutral_to_other_sinks() {
        let (reload_layer, _handle) = new_otlp_log_layer(false);
        let events = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry()
            .with(reload_layer)
            .with(CountingLayer(Arc::clone(&events)));

        tracing::subscriber::with_default(subscriber, || tracing::info!("still locally visible"));
        assert_eq!(events.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn relay_can_install_and_remove_exporter() {
        crate::runtime::init_background_runtime();
        let (reload_layer, mut handle) = new_otlp_log_layer(false);
        let subscriber = tracing_subscriber::registry().with(reload_layer);
        let mut config = config();
        config.endpoint = "http://127.0.0.1:1/v1/logs".to_owned();

        tracing::subscriber::with_default(subscriber, || {
            handle.configure(Some(config)).unwrap();
            assert!(handle.reload.with_current(Option::is_some).unwrap());

            handle.configure(None).unwrap();
            assert!(handle.reload.with_current(Option::is_none).unwrap());
        });
    }

    #[test]
    fn exports_otlp_protobuf_to_the_exact_relay_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || receive_http_request(&listener));

        let mut config = config();
        config.endpoint = format!("http://{address}/custom/v1/logs");
        let (layer, provider) =
            build_otlp_layer(ValidatedConfig::try_from(config).unwrap()).unwrap();
        let dispatch = Dispatch::new(tracing_subscriber::registry().with(layer));
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(sequence = 42, source = "relay", "export integration test");
        });
        provider.shutdown().unwrap();

        let request = server.join().unwrap();
        let header_end = find_bytes(&request, b"\r\n\r\n").unwrap() + 4;
        let headers = std::str::from_utf8(&request[..header_end]).unwrap().to_ascii_lowercase();
        assert!(headers.starts_with("post /custom/v1/logs http/1.1\r\n"));
        assert!(headers.contains("content-type: application/x-protobuf\r\n"));
        assert!(headers.contains("authorization: bearer secret\r\n"));
        assert!(request.len() > header_end, "OTLP request body is empty");
    }

    fn receive_http_request(listener: &TcpListener) -> Vec<u8> {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

        let mut request = Vec::new();
        let mut content_length = None;
        let mut buffer = [0; 8 * 1_024];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0, "OTLP client closed before completing its request");
            request.extend_from_slice(&buffer[..read]);
            assert!(request.len() <= 1024 * 1024, "unexpectedly large OTLP test request");

            if content_length.is_none()
                && let Some(header_end) = find_bytes(&request, b"\r\n\r\n")
            {
                let headers = std::str::from_utf8(&request[..header_end]).unwrap();
                content_length = headers.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                });
            }
            if let (Some(header_end), Some(content_length)) =
                (find_bytes(&request, b"\r\n\r\n"), content_length)
                && request.len() >= header_end + 4 + content_length
            {
                break;
            }
        }

        stream
            .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
            .unwrap();
        request
    }

    fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack.windows(needle.len()).position(|window| window == needle)
    }

    #[derive(Debug, Default)]
    struct BlockingExporterState {
        released: Mutex<bool>,
        started: Condvar,
        release: Condvar,
        exported: AtomicUsize,
        targets: Mutex<Vec<String>>,
    }

    impl BlockingExporterState {
        fn wait_until_started(&self) {
            let (released, timeout) = self
                .started
                .wait_timeout_while(self.released.lock().unwrap(), Duration::from_secs(2), |_| {
                    self.exported.load(Ordering::Acquire) == 0
                })
                .unwrap();
            drop(released);
            assert!(!timeout.timed_out(), "OTLP export worker did not start");
        }

        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.release.notify_all();
        }
    }

    #[derive(Clone, Debug)]
    struct BlockingExporter(Arc<BlockingExporterState>);

    impl LogExporter for BlockingExporter {
        async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
            let count = {
                let mut targets = self.0.targets.lock().unwrap();
                let initial_len = targets.len();
                targets.extend(batch.iter().map(|(record, _)| {
                    record.target().map(ToString::to_string).unwrap_or_default()
                }));
                targets.len() - initial_len
            };
            self.0.exported.fetch_add(count, Ordering::Release);
            self.0.started.notify_all();
            let released = self.0.released.lock().unwrap();
            drop(self.0.release.wait_while(released, |released| !*released).unwrap());
            Ok(())
        }
    }

    fn blocking_provider(queue_capacity: usize) -> (SdkLoggerProvider, Arc<BlockingExporterState>) {
        let state = Arc::new(BlockingExporterState::default());
        let batch_config = BatchConfigBuilder::default()
            .with_max_queue_size(queue_capacity)
            .with_max_export_batch_size(1)
            .with_scheduled_delay(Duration::from_mins(1))
            .build();
        let processor = BatchLogProcessor::builder(BlockingExporter(Arc::clone(&state)))
            .with_batch_config(batch_config)
            .build();
        let provider = SdkLoggerProvider::builder().with_log_processor(processor).build();
        (provider, state)
    }

    #[test]
    fn saturated_queue_stays_bounded_and_does_not_block_producer() {
        const QUEUE_CAPACITY: usize = 8;
        const FLOOD_EVENTS: usize = 100_000;

        let (provider, state) = blocking_provider(QUEUE_CAPACITY);
        let subscriber =
            tracing_subscriber::registry().with(otlp_layer(&provider, OtlpLogLevel::Info));
        let dispatch = Dispatch::new(subscriber);

        tracing::dispatcher::with_default(&dispatch, || tracing::info!("start export"));
        state.wait_until_started();

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                for sequence in 0..FLOOD_EVENTS {
                    tracing::info!(sequence, "saturate OTLP queue");
                }
            });
            done_tx.send(()).unwrap();
        });
        done_rx.recv_timeout(Duration::from_secs(2)).expect("full OTLP queue blocked the producer");

        state.release();
        provider.shutdown().unwrap();
        assert!(state.exported.load(Ordering::Acquire) <= QUEUE_CAPACITY + 1);
    }

    #[test]
    fn exporter_diagnostics_do_not_feed_back_into_otlp() {
        let (provider, state) = blocking_provider(8);
        let dispatch = Dispatch::new(
            tracing_subscriber::registry().with(otlp_layer(&provider, OtlpLogLevel::Info)),
        );

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::warn!(target: "opentelemetry_sdk", "internal exporter diagnostic");
            tracing::info!("application event");
        });
        state.wait_until_started();

        let targets = state.targets.lock().unwrap().clone();
        assert_eq!(targets.len(), 1);
        assert_ne!(targets[0], "opentelemetry_sdk");
        state.release();
        provider.shutdown().unwrap();
    }

    #[derive(Clone)]
    struct EventSink;

    impl<S: Subscriber> Layer<S> for EventSink {}

    fn probe(name: &str, dispatch: &Dispatch) {
        const EVENTS: u64 = 1_000_000;
        tracing::dispatcher::with_default(dispatch, || {
            for sequence in 0..1_024 {
                tracing::info!(sequence, transactions = 64, source = "relay", "processed batch");
            }
            let started = Instant::now();
            for sequence in 0..EVENTS {
                tracing::info!(
                    sequence = black_box(sequence),
                    transactions = 64,
                    source = "relay",
                    "processed batch"
                );
            }
            let ns_per_event = started.elapsed().as_nanos() as f64 / EVENTS as f64;
            eprintln!("{name}: {ns_per_event:.1} ns/event");
        });
    }

    /// A local diagnostic, not a performance assertion. Run with:
    /// `cargo test -p gravity-types otlp_hot_path_probe --release -- --ignored --nocapture`
    #[test]
    #[ignore = "manual release-mode timing diagnostic"]
    fn otlp_hot_path_probe() {
        probe(
            "plain tracing layer",
            &Dispatch::new(tracing_subscriber::registry().with(EventSink)),
        );

        let (reload_layer, _handle) = new_otlp_log_layer(false);
        probe(
            "reload layer disabled",
            &Dispatch::new(tracing_subscriber::registry().with(reload_layer).with(EventSink)),
        );

        let (provider, state) = blocking_provider(512);
        let dispatch = Dispatch::new(
            tracing_subscriber::registry()
                .with(otlp_layer(&provider, OtlpLogLevel::Info))
                .with(EventSink),
        );
        tracing::dispatcher::with_default(&dispatch, || tracing::info!("start export"));
        state.wait_until_started();
        for sequence in 0..512 {
            tracing::dispatcher::with_default(&dispatch, || {
                tracing::info!(sequence, "fill OTLP queue");
            });
        }
        probe("OTLP queue saturated", &dispatch);
        state.release();
        provider.shutdown().unwrap();
    }
}
