use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::sleep,
    time::Duration,
};

use agave_scheduling_utils::handshake::{ClientLogon, ClientSession, client};
use flux::utils::{ThreadNiceness, thread_boot};
use gravity_connector::{
    APP_NAME, BlockEngineProxyHandle, ClientConfig, Config, ConnectorTile, Failsafe,
    IdentityRpcServer, MAX_SHRED_RECEIVER_ADDRESSES, Network, RESERVED_RELAY_SHRED_RECEIVERS,
    StopCodes, TipDistributionAccountConfig, TipManager, TipManagerConfig, bundle_receiver_loop,
    dedup_shred_receivers, default_block_engine_urls, metrics, monitor_identity,
    set_shred_receiver_addresses, set_shred_retransmit_receiver_addresses,
    spawn_block_builder_info_loop, spawn_block_engine_proxy, spawn_bundle_loop,
    wait_for_expected_identity, wait_for_expected_keypair,
};
use gravity_types::{
    Metadata,
    consts::MAX_ALLOCATOR_FILE_SIZE,
    init_tracing_log, load_config, panic_hook,
    runtime::{background_runtime, init_background_runtime},
    wire::Handshake,
};
use signal_hook::{
    consts::{SIGINT, SIGQUIT, SIGTERM},
    flag::register_usize,
};
use solana_keypair::{Keypair, read_keypair_file};
use solana_signer::Signer;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

enum IdentitySource {
    File { path: PathBuf, keypair: Option<Keypair> },
    Rpc(IdentityRpcServer),
}

#[allow(clippy::too_many_lines)]
fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let path =
        std::env::args().nth(1).expect("missing config path. Run with 'path_to_config.toml'");
    let mut config: Config = load_config(&path);
    config.validate().unwrap_or_else(|err| panic!("invalid config: {err}"));
    let client_variant = config.client.variant();

    let alert_webhook = config.take_alert_webhook();
    std::panic::set_hook(panic_hook(&config.instance_id, alert_webhook));

    let _guard = init_tracing_log("connector", &config.logging, APP_NAME, &[
        ("jsonrpc_client_transports", "info"),
        ("ipc", "off"),
        ("rpc", "off"),
    ]);
    init_background_runtime();

    // Before the identity wait, so startup is scrapeable.
    metrics::spawn_metrics(config.metrics_addr);
    metrics::RELAYS_CONFIGURED.set(config.relay_addrs.len() as i64);

    info!(instance = %config.instance_id, workers =? config.num_workers, metadata =% Metadata::get(), "starting");
    assert!(config.num_workers > 0, "need at least 1 worker");
    let (shred_receivers, shred_retransmit_receivers) = match &mut config.client {
        ClientConfig::Agave(_) => (Vec::new(), Vec::new()),
        ClientConfig::Jito(jito) => {
            dedup_shred_receivers(&mut jito.shred_receivers);
            assert!(
                jito.shred_receivers.len() <=
                    MAX_SHRED_RECEIVER_ADDRESSES - RESERVED_RELAY_SHRED_RECEIVERS,
                "the sidecar needs at least {RESERVED_RELAY_SHRED_RECEIVERS} free slots for shred receivers (max {MAX_SHRED_RECEIVER_ADDRESSES}, curr: {})",
                jito.shred_receivers.len()
            );
            dedup_shred_receivers(&mut jito.shred_retransmit_receivers);
            assert!(
                jito.shred_retransmit_receivers.len() <=
                    MAX_SHRED_RECEIVER_ADDRESSES - RESERVED_RELAY_SHRED_RECEIVERS,
                "the sidecar needs at least {RESERVED_RELAY_SHRED_RECEIVERS} free slots for shred retransmit receivers (max {MAX_SHRED_RECEIVER_ADDRESSES}, curr: {})",
                jito.shred_retransmit_receivers.len()
            );
            (jito.shred_receivers.clone(), jito.shred_retransmit_receivers.clone())
        }
    };

    let stop_flag = Arc::new(AtomicUsize::new(0));
    register_usize(SIGTERM, Arc::clone(&stop_flag), StopCodes::SIGTERM as usize)
        .expect("register SIGTERM");
    register_usize(SIGINT, Arc::clone(&stop_flag), StopCodes::SIGINT as usize)
        .expect("register SIGINT");
    register_usize(SIGQUIT, Arc::clone(&stop_flag), StopCodes::SIGQUIT as usize)
        .expect("register SIGQUIT");

    let (expected_identity, mut identity_source) =
        match (config.identity_path.as_ref(), config.expected_identity) {
            (Some(path), expected_identity) => {
                // Legacy configs need the keypair now to derive the expected identity.
                let (expected_identity, keypair) = expected_identity.map_or_else(
                    || {
                        let keypair = read_keypair_file(path).unwrap_or_else(|err| {
                            panic!(
                                "failed reading identity keypair: {}, err: {err:?}",
                                path.display()
                            )
                        });
                        (keypair.pubkey(), Some(keypair))
                    },
                    |expected_identity| (expected_identity, None),
                );
                info!(path = %path.display(), "using file-backed identity source");
                (expected_identity, IdentitySource::File { path: path.clone(), keypair })
            }
            (None, Some(expected_identity)) => {
                // Start before waiting for Agave so early injections are queued.
                let path = config.identity_rpc_path();
                let server = IdentityRpcServer::start(path.clone(), expected_identity)
                    .unwrap_or_else(|err| {
                        panic!("failed starting identity RPC at {}: {err}", path.display())
                    });
                info!(path = %path.display(), "using in-memory identity source");
                (expected_identity, IdentitySource::Rpc(server))
            }
            (None, None) => unreachable!("config validation requires an identity source"),
        };

    metrics::set_info(&config.instance_id, &expected_identity.to_string(), client_variant.as_str());

    let admin_rpc_path = config.admin_rpc_path();
    let scheduler_bindings_path = config.scheduler_bindings_path();
    if !background_runtime().block_on(wait_for_expected_identity(
        admin_rpc_path.clone(),
        expected_identity,
        stop_flag.clone(),
    )) {
        info!("stopped while waiting for validator identity, exiting");
        return;
    }
    background_runtime().spawn(monitor_identity(
        admin_rpc_path.clone(),
        expected_identity,
        stop_flag.clone(),
    ));
    let identity_kp = match &mut identity_source {
        IdentitySource::Rpc(server) => {
            let Some(identity_kp) = server.wait_for_identity(&stop_flag) else {
                info!("stopped while waiting for in-memory identity, exiting");
                return;
            };
            identity_kp
        }
        IdentitySource::File { path, keypair } => {
            if let Some(identity_kp) = keypair.take() {
                identity_kp
            } else {
                let Some(identity_kp) = background_runtime().block_on(wait_for_expected_keypair(
                    path.clone(),
                    expected_identity,
                    stop_flag.clone(),
                )) else {
                    info!("stopped while waiting for identity keypair, exiting");
                    return;
                };
                identity_kp
            }
        }
    };
    let identity_pubkey = identity_kp.pubkey();
    background_runtime()
        .block_on(set_shred_receiver_addresses(admin_rpc_path.clone(), shred_receivers.clone()));
    background_runtime().block_on(set_shred_retransmit_receiver_addresses(
        admin_rpc_path.clone(),
        shred_retransmit_receivers.clone(),
    ));
    let (crank_bundle_tx, crank_bundle_rx) = mpsc::channel(1000);
    let (crank_trigger_tx, crank_trigger_rx) = mpsc::channel(1000);
    let (bundle_tx, bundle_rx) = mpsc::channel(100_000);
    let connector_crank_enabled = matches!(
        &config.client,
        ClientConfig::Agave(agave) if agave.tip_management.is_some()
    );
    let builder_is_connected = Arc::new(AtomicBool::new(false));
    let block_engine_urls = match &config.client {
        ClientConfig::Agave(agave) => agave
            .tip_management
            .as_ref()
            .map(|_| agave.jito_block_engines.clone().unwrap_or_else(default_block_engine_urls)),
        ClientConfig::Jito(jito) => {
            Some(jito.jito_block_engines.clone().unwrap_or_else(default_block_engine_urls))
        }
    };
    metrics::BLOCK_ENGINES_CONFIGURED.set(block_engine_urls.as_ref().map_or(0, Vec::len) as i64);

    let (bundle_receivers, block_engine_proxy) = if let Some(block_engine_urls) = block_engine_urls
    {
        info!(?block_engine_urls, "using Jito block engines");
        let receiver_kp = identity_kp.insecure_clone();
        let maybe_proxy = match &config.client {
            ClientConfig::Agave(agave) => {
                let tip_config =
                    agave.tip_management.as_ref().expect("validated Agave tip-management config");
                let tip_manager = TipManager::new(TipManagerConfig {
                    tip_payment_program_id: tip_config.tip_payment_program_pubkey,
                    tip_distribution_program_id: tip_config.tip_distribution_program_pubkey,
                    tip_distribution_account_config: TipDistributionAccountConfig {
                        merkle_root_upload_authority: tip_config.merkle_root_upload_authority,
                        vote_account: tip_config.vote_account_pubkey,
                        commission_bps: tip_config.mev_commission_bps,
                    },
                });

                let mut full_rpc_url = tip_config.rpc_url.clone();
                if let Some(api_key) = &tip_config.rpc_api_key {
                    full_rpc_url += "/?api-key=";
                    full_rpc_url += api_key;
                }

                background_runtime().spawn(spawn_bundle_loop(
                    block_engine_urls.clone(),
                    identity_kp.insecure_clone(),
                    tip_manager,
                    full_rpc_url,
                    crank_bundle_tx,
                    crank_trigger_rx,
                ));
                None
            }
            ClientConfig::Jito(jito) => {
                let proxy = BlockEngineProxyHandle::new(jito.block_engine_proxy_addr);
                background_runtime().spawn(spawn_block_engine_proxy(proxy.clone()));
                background_runtime().spawn(spawn_block_builder_info_loop(
                    block_engine_urls.clone(),
                    identity_kp.insecure_clone(),
                    proxy.clone(),
                ));
                info!(
                    url = %proxy.advertised_url(),
                    "local block-engine proxy enabled for Jito client"
                );
                Some(proxy)
            }
        };

        (Some((receiver_kp, block_engine_urls)), maybe_proxy)
    } else {
        warn!("no Jito block engines configured, skipping Jito bundles");
        (None, None)
    };

    let handshake = Handshake {
        identity: identity_pubkey,
        conn_version: Metadata::get().to_string(),
        num_threads: config.num_workers as u8,
        filter_ofac: config.filter_ofac,
    };

    let mut network = Network::new(
        &config.relay_addrs,
        handshake,
        bundle_rx,
        block_engine_proxy.clone(),
        builder_is_connected.clone(),
        admin_rpc_path,
        shred_receivers,
        shred_retransmit_receivers,
        identity_kp,
    );

    if let Some((identity_kp, block_engine_urls)) = bundle_receivers {
        for url in block_engine_urls {
            background_runtime().spawn(bundle_receiver_loop(
                url,
                identity_kp.insecure_clone(),
                bundle_tx.clone(),
                builder_is_connected.clone(),
                block_engine_proxy.clone(),
            ));
        }
    }

    Failsafe::init_path(config.failsafe_path());

    if let Some(failsafe) = Failsafe::read() {
        #[cfg(feature = "test_validator")]
        {
            let _ = &failsafe;
            Failsafe::remove();
        }
        #[cfg(not(feature = "test_validator"))]
        {
            warn!(
                ?failsafe,
                path =? Failsafe::path().display(),
                "found failsafe! Not starting up until it expires or the builder clears it",
            );
            metrics::FAILSAFE_ACTIVE.set(1);
            let mut last_failsafe_log = std::time::Instant::now();
            loop {
                if let Some(code) = StopCodes::poll(&stop_flag) {
                    info!(?code, "received stop code while held in failsafe, exiting");
                    return;
                }
                if failsafe.is_expired() {
                    warn!(?failsafe, "failsafe has expired, removing and starting up");
                    Failsafe::remove();
                    break;
                }
                if network.poll_delete_failsafe() {
                    warn!("builder cleared failsafe, removing and starting up");
                    Failsafe::remove();
                    break;
                }
                if last_failsafe_log.elapsed() >= Duration::from_secs(10) {
                    warn!(
                        ?failsafe,
                        path =? Failsafe::path().display(),
                        "still held in failsafe; not starting until it expires or the builder clears it",
                    );
                    last_failsafe_log = std::time::Instant::now();
                }
                sleep(Duration::from_millis(100));
            }
            metrics::FAILSAFE_ACTIVE.set(0);
        }
    }

    network.wait_for_builder(&stop_flag);
    info!("connecting to agave and starting up");
    let ClientSession { allocators, tpu_to_pack, progress_tracker, workers } = {
        loop {
            if let Some(code) = StopCodes::poll(&stop_flag) {
                info!(?code, "received stop code while connecting to agave, exiting");
                return;
            }

            let session = client::connect(
                &scheduler_bindings_path,
                ClientLogon {
                    worker_count: config.num_workers,
                    allocator_size: MAX_ALLOCATOR_FILE_SIZE,
                    allocator_handles: 1,
                    tpu_to_pack_capacity: 128 * 1024,
                    progress_tracker_capacity: 20 * 64,
                    pack_to_worker_capacity: 64 * 1024,
                    worker_to_pack_capacity: 64 * 1024,
                    flags: 0,
                },
                std::time::Duration::from_secs(2),
            );
            match session {
                Ok(s) => break s,
                Err(err) => warn!(%err, "failed to connect to agave, retrying in 10s"),
            }

            for _ in 0..10 {
                network.poll_startup();
                if !StopCodes::running(&stop_flag) {
                    break;
                }
                sleep(Duration::from_secs(1));
            }
        }
    };
    assert_eq!(
        workers.len(),
        config.num_workers,
        "received wrong number of workers from agave init"
    );
    let allocator = allocators.into_iter().next().unwrap();

    let connector_tile = ConnectorTile::new(
        network,
        tpu_to_pack,
        progress_tracker,
        workers,
        connector_crank_enabled,
        crank_bundle_rx,
        crank_trigger_tx,
        allocator,
        config.slot_duration_override_ms,
        client_variant,
    );

    let flag = stop_flag.clone();
    std::thread::spawn(move || {
        thread_boot(Some(config.connector_core), Some(ThreadNiceness::High));
        connector_tile.run(&flag);
    });

    metrics::READY.set(1);

    while StopCodes::running(&stop_flag) {
        std::thread::sleep(Duration::from_secs(1));
    }

    let code = StopCodes::from(stop_flag.load(Ordering::Relaxed));
    info!(?code, "received stop code, exiting");
}
