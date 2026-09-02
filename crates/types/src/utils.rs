use std::{fmt, path::PathBuf};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::error;
use tracing_appender::{non_blocking::WorkerGuard, rolling::Rotation};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;
use wincode_derive::{SchemaRead, SchemaWrite};

pub mod env_string {

    use std::env;

    use dotenvy::dotenv;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Field types that can be populated from an `env:KEY` reference or an
    /// inline string. Implemented for `String` (required) and `Option<String>`
    /// (optional — missing env var resolves to `None`).
    pub trait FromEnvString: Sized {
        fn from_resolved<E: Error>(raw: &str, resolved: Option<String>) -> Result<Self, E>;
    }

    impl FromEnvString for String {
        fn from_resolved<E: Error>(raw: &str, resolved: Option<String>) -> Result<Self, E> {
            resolved.ok_or_else(|| E::custom(format!("env var not set for `{raw}`")))
        }
    }

    impl FromEnvString for Option<String> {
        fn from_resolved<E: Error>(_raw: &str, resolved: Option<String>) -> Result<Self, E> {
            Ok(resolved)
        }
    }

    impl FromEnvString for super::WebhookUrl {
        fn from_resolved<E: Error>(raw: &str, resolved: Option<String>) -> Result<Self, E> {
            resolved
                .ok_or_else(|| E::custom(format!("env var not set for `{raw}`")))
                .and_then(|value| Self::new(&value).map_err(E::custom))
        }
    }

    impl FromEnvString for Option<super::WebhookUrl> {
        fn from_resolved<E: Error>(_raw: &str, resolved: Option<String>) -> Result<Self, E> {
            resolved.map(|value| super::WebhookUrl::new(&value).map_err(E::custom)).transpose()
        }
    }

    pub fn deserialize<'de, T: FromEnvString, D: Deserializer<'de>>(d: D) -> Result<T, D::Error> {
        let raw = String::deserialize(d)?;
        let resolved = raw.strip_prefix("env:").map_or_else(
            || Some(raw.clone()),
            |key| {
                _ = dotenv();
                env::var(key).ok()
            },
        );
        T::from_resolved(&raw, resolved)
    }

    pub fn serialize<S: Serializer, T>(_v: &T, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("REDACTED_SECRET")
    }
}

pub fn load_config<T: DeserializeOwned>(path: &str) -> T {
    let config_file = std::fs::read_to_string(path)
        .unwrap_or_else(|_| panic!("unable to find config file: '{path}'"));
    parse_strict(&config_file).unwrap_or_else(|e| panic!("failed to parse config at {path}: {e}"))
}

/// Parses TOML, rejecting fields that are not part of the config.
fn parse_strict<T: DeserializeOwned>(raw: &str) -> anyhow::Result<T> {
    let deserializer = toml::Deserializer::parse(raw)?;
    let mut unknown = Vec::new();
    let mut track = |path: serde_ignored::Path| unknown.push(path.to_string());
    let deserializer = serde_ignored::Deserializer::new(deserializer, &mut track);
    let value = serde_path_to_error::deserialize(deserializer)?;
    anyhow::ensure!(unknown.is_empty(), "unknown config fields: {}", unknown.join(", "));
    Ok(value)
}

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub stdout: StdoutLogConfig,
    #[serde(default)]
    pub file: FileLogSettings,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StdoutLogConfig {
    #[serde(default = "default_level")]
    pub level: String,
    #[serde(default = "default_bool::<true>")]
    pub enabled: bool,
    #[serde(default = "default_bool::<true>")]
    pub enable_ansi: bool,
}

impl Default for StdoutLogConfig {
    fn default() -> Self {
        Self { level: "info".into(), enabled: true, enable_ansi: true }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FileLogSettings {
    #[serde(default = "default_bool::<false>")]
    pub enabled: bool,
    #[serde(default = "default_level")]
    pub level: String,
    #[serde(default = "default_usize::<30>")]
    pub max_days: usize,
    #[serde(default)]
    pub dir: Option<PathBuf>,
}

fn default_level() -> String {
    "info".into()
}

impl Default for FileLogSettings {
    fn default() -> Self {
        Self { enabled: false, level: "info".into(), max_days: default_usize::<30>(), dir: None }
    }
}

#[must_use]
pub fn init_tracing_log(
    prefix: &str,
    settings: &LoggingConfig,
    app_name: &str,
    overrides: &[(&str, &str)],
) -> (Option<WorkerGuard>, Option<WorkerGuard>) {
    if !settings.stdout.enabled && !settings.file.enabled {
        eprintln!("No logging is enabled!");
        return (None, None);
    }

    let registry = tracing_subscriber::registry();

    let (stdout_layer, stdout_guard) = if settings.stdout.enabled {
        let cfg = &settings.stdout;

        let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());

        let level = cfg.level.parse().expect("invalid stdout log level, change to eg 'info'");

        let layer = tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_level(true)
            .with_target(true)
            .with_thread_ids(false)
            .with_ansi(cfg.enable_ansi)
            .compact()
            .with_filter(get_crate_filter(level, overrides));

        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    let (file_layer, file_guard) = if settings.file.enabled {
        let cfg = &settings.file;

        let default_log_dir = flux::utils::directories::logs_dir(app_name);
        let log_dir = cfg.dir.as_ref().unwrap_or(&default_log_dir);

        let file_appender = tracing_appender::rolling::Builder::new()
            .filename_prefix(prefix.to_lowercase())
            .max_log_files(cfg.max_days)
            .rotation(Rotation::DAILY)
            .build(log_dir)
            .unwrap_or_else(|e| {
                panic!("failed to create log appender: dir={} err={e}", log_dir.display())
            });

        let (writer, guard) = tracing_appender::non_blocking(file_appender);

        let level = cfg.level.parse().expect("invalid file log level, change to eg 'info'");

        let layer = tracing_subscriber::fmt::layer()
            .with_writer(writer)
            .with_level(true)
            .with_target(true)
            .with_thread_ids(false)
            .with_ansi(false)
            .json()
            .with_filter(get_crate_filter(level, overrides));

        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    registry.with(stdout_layer).with(file_layer).init();

    (stdout_guard, file_guard)
}

const DISABLE_CRATES: &[&str] =
    &["hyper_util", "reqwest", "rustls", "h2", "tower", "tonic", "solana_rpc_client"];

fn get_crate_filter(crates_level: tracing::Level, overrides: &[(&str, &str)]) -> EnvFilter {
    let mut env_filter = EnvFilter::new(format!("{crates_level}"));

    for crate_name in DISABLE_CRATES {
        env_filter = env_filter.add_directive(format!("{crate_name}=info").parse().unwrap());
    }

    for (crate_name, level) in overrides {
        env_filter = env_filter.add_directive(format!("{crate_name}={level}").parse().unwrap());
    }

    env_filter
}

// in several places we want to treat txs and bundles uniformly by returning
// an iterator, but each branch produces a different concrete iterator type
pub enum EitherIter2<I1, I2> {
    V1(I1),
    V2(I2),
}

impl<I1, I2> Iterator for EitherIter2<I1, I2>
where
    I1: Iterator,
    I2: Iterator<Item = I1::Item>,
{
    type Item = I1::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::V1(it) => it.next(),
            Self::V2(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::V1(it) => it.size_hint(),
            Self::V2(it) => it.size_hint(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AlertWebhookProvider {
    Discord,
    Slack,
}

impl AlertWebhookProvider {
    fn payload(self, message: String) -> AlertWebhookPayload {
        match self {
            Self::Discord => AlertWebhookPayload::Discord { content: message },
            Self::Slack => AlertWebhookPayload::Slack { text: message },
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum AlertWebhookPayload {
    Discord { content: String },
    Slack { text: String },
}

#[derive(Clone, Debug, Deserialize)]
pub struct AlertWebhook {
    provider: AlertWebhookProvider,
    #[serde(with = "env_string")]
    url: WebhookUrl,
}

impl AlertWebhook {
    pub const fn discord(url: WebhookUrl) -> Self {
        Self { provider: AlertWebhookProvider::Discord, url }
    }

    pub const fn provider(&self) -> AlertWebhookProvider {
        self.provider
    }

    fn send(&self, message: String) -> Result<(), reqwest::Error> {
        self.url.send(&self.provider.payload(message))
    }
}

#[derive(Clone)]
pub struct WebhookUrl(Url);

impl WebhookUrl {
    pub fn new(raw: &str) -> anyhow::Result<Self> {
        let url = Url::parse(raw)?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https") && url.host().is_some(),
            "invalid alert webhook URL"
        );
        Ok(Self(url))
    }

    fn send(&self, content: &impl Serialize) -> Result<(), reqwest::Error> {
        reqwest::blocking::Client::new()
            .post(self.0.clone())
            .json(content)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
            .map(|_| ())
            .map_err(reqwest::Error::without_url)
    }
}

impl fmt::Debug for WebhookUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for WebhookUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/[REDACTED]", self.0.origin().ascii_serialization())
    }
}

/// Backwards-compatible name for users of the Rust API. New code should use
/// [`WebhookUrl`].
pub type DiscordWebhookUrl = WebhookUrl;

pub fn send_alert(webhook: Option<&AlertWebhook>, instance_id: &str, message: &str) {
    if is_test_env() {
        return;
    }

    let Some(webhook) = webhook else { return };

    let mut end = 1850.min(message.len());
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let msg = format!("APP_ID: `{instance_id}`\n{}", &message[..end]);

    error!("{msg}");
    eprintln!("{msg}");

    if let Err(err) = webhook.send(msg) {
        error!("failed to send webhook alert: {err}");
        eprintln!("failed to send webhook alert: {err}");
    }
}

const fn is_test_env() -> bool {
    cfg!(test) || cfg!(debug_assertions)
}

pub fn panic_hook(
    instance_id: &str,
    webhook: Option<AlertWebhook>,
) -> Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send> {
    let instance_id = instance_id.to_string();
    Box::new(move |info| {
        let backtrace = backtrace::Backtrace::new();
        let crash_log = format!("panic: {info}\nfull backtrace:\n{backtrace:?}\n");
        error!("{crash_log}");
        eprintln!("{crash_log}");
        send_alert(webhook.as_ref(), &instance_id, &crash_log);
    })
}

#[derive(Debug)]
pub struct Metadata {
    pub version: &'static str,
    pub commit: &'static str,
    pub branch: &'static str,
    pub built_at: &'static str,
}

impl Metadata {
    pub const fn get() -> Self {
        Self {
            version: env!("GIT_VERSION"),
            commit: env!("GIT_HASH"),
            branch: env!("GIT_BRANCH"),
            built_at: env!("BUILT_AT"),
        }
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{} ({}/{} {})", self.version, self.branch, self.commit, self.built_at)
    }
}

pub const fn default_bool<const B: bool>() -> bool {
    B
}

pub const fn default_usize<const U: usize>() -> usize {
    U
}

#[derive(
    serde_repr::Serialize_repr,
    serde_repr::Deserialize_repr,
    SchemaRead,
    SchemaWrite,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Copy,
    Default,
)]
#[repr(i8)]
pub enum OrderType {
    #[default]
    Tx = 1,
    Bundle = 2,
}

#[macro_export]
macro_rules! measure_ns {
    ($expr:expr) => {{
        let (result, duration) = $crate::meas_dur!($expr);
        (result, duration.as_nanos() as u64)
    }};
}

#[macro_export]
macro_rules! measure_us {
    ($expr:expr) => {{
        let (result, duration) = $crate::meas_dur!($expr);
        (result, duration.as_micros() as u64)
    }};
}

#[macro_export]
macro_rules! meas_dur {
    ($expr:expr) => {{
        let start = flux::timing::Instant::now();
        let result = $expr;
        (result, start.elapsed())
    }};
}
