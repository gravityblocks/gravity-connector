use std::{fmt, iter::once, path::PathBuf, sync::OnceLock};

use rustc_hash::FxHashMap;
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

static DISCORD_WEBHOOK_URL: OnceLock<Url> = OnceLock::new();

pub fn alert_discord(instance_id: &str, message: &str) {
    if is_test_env() {
        return;
    }

    let Some(webhook_url) = DISCORD_WEBHOOK_URL.get() else { return };

    let max_len = 1850.min(message.len());
    let msg = format!("APP_ID: `{instance_id}`\n{}", &message[..max_len]);

    error!("{msg}");
    eprintln!("{msg}");

    let content: FxHashMap<&str, String> = once(("content", msg)).collect();

    if let Err(err) =
        reqwest::blocking::Client::new().post(webhook_url.clone()).json(&content).send()
    {
        error!("failed to send discord alert: {err}");
        eprintln!("failed to send discord alert: {err}");
    }
}

const fn is_test_env() -> bool {
    cfg!(test) || cfg!(debug_assertions)
}

pub fn panic_hook(instance_id: &str) -> Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send> {
    let instance_id = instance_id.to_string();
    Box::new(move |info| {
        let backtrace = backtrace::Backtrace::new();
        let crash_log = format!("panic: {info}\nfull backtrace:\n{backtrace:?}\n");
        error!("{crash_log}");
        eprintln!("{crash_log}");
        alert_discord(&instance_id, &crash_log);
    })
}

pub fn set_discord_webhook(webhook_url: Url) {
    DISCORD_WEBHOOK_URL.set(webhook_url).unwrap();
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
