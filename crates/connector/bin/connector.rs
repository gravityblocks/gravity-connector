#![feature(likely_unlikely)]

use std::{
    hint::likely,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{sleep, spawn},
    time::Duration,
};

use agave_scheduling_utils::handshake::{ClientLogon, ClientSession, client};
use flux::{
    timing::Nanos,
    utils::{ThreadNiceness, thread_boot},
};
use gravity_connector::{
    APP_NAME, BlockEngineProxyHandle, BridgeTile, ClientVariant, Config, Failsafe,
    MAX_SHRED_RECEIVER_ADDRESSES, NetworkTile, RESERVED_RELAY_SHRED_RECEIVERS, StopCodes,
    TipDistributionAccountConfig, TipManager, TipManagerConfig, bundle_receiver_loop,
    dedup_shred_receivers, default_block_engine_urls, metrics, monitor_identity,
    set_shred_receiver_addresses, spawn_block_engine_proxy, spawn_bundle_loop,
    wait_for_expected_identity,
};
use gravity_types::{
    Metadata,
    consts::MAX_ALLOCATOR_FILE_SIZE,
    init_tracing_log, load_config, panic_hook,
    runtime::{background_runtime, init_background_runtime},
    set_discord_webhook,
    wire::Handshake,
};
use signal_hook::{
    consts::{SIGINT, SIGQUIT, SIGTERM},
    flag::register_usize,
};
use solana_keypair::read_keypair_file;
use solana_signer::Signer;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[allow(clippy::too_many_lines)]
fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let path =
        std::env::args().nth(1).expect("missing config path. Run with 'path_to_config.toml'");
    let mut config: Config = load_config(&path);

    if let Some(url) = &config.discord_webhook {
        set_discord_webhook(url.parse().unwrap());
    }

    std::panic::set_hook(panic_hook(&config.instance_id));

    let _guard = init_tracing_log("connector", &config.logging, APP_NAME, &[(
        "jsonrpc_client_transports",
        "info",
    )]);
    init_background_runtime();

    // Before the identity wait, so startup is scrapeable.
    metrics::spawn_metrics(config.metrics_addr);
    metrics::RELAYS_CONFIGURED.set(config.relay_addrs.len() as i64);

    info!(instance = %config.instance_id, workers =? config.num_workers, metadata =% Metadata::get(), "starting");
    assert!(config.num_workers > 0, "need at least 1 worker");
    dedup_shred_receivers(&mut config.shred_receivers);
    assert!(
        config.shred_receivers.len() <=
            MAX_SHRED_RECEIVER_ADDRESSES - RESERVED_RELAY_SHRED_RECEIVERS,
        "the sidecar needs at least {RESERVED_RELAY_SHRED_RECEIVERS} free slots for shred receivers (max {MAX_SHRED_RECEIVER_ADDRESSES}, curr: {})",
        config.shred_receivers.len()
    );

    let stop_flag = Arc::new(AtomicUsize::new(0));
    register_usize(SIGTERM, Arc::clone(&stop_flag), StopCodes::SIGTERM as usize)
        .expect("register SIGTERM");
    register_usize(SIGINT, Arc::clone(&stop_flag), StopCodes::SIGINT as usize)
        .expect("register SIGINT");
    register_usize(SIGQUIT, Arc::clone(&stop_flag), StopCodes::SIGQUIT as usize)
        .expect("register SIGQUIT");

    let identity_kp = match read_keypair_file(&config.identity_path) {
        Ok(kp) => kp,
        Err(err) => panic!(
            "failed reading identity keypair: {}, err: {:?}",
            config.identity_path.display(),
            err,
        ),
    };

    let identity_pubkey = identity_kp.pubkey();
    metrics::set_info(
        &config.instance_id,
        &identity_pubkey.to_string(),
        config.client_variant.as_str(),
    );

    let admin_rpc_path = config.admin_rpc_path();
    let scheduler_bindings_path = config.scheduler_bindings_path();
    if !background_runtime().block_on(wait_for_expected_identity(
        admin_rpc_path.clone(),
        identity_pubkey,
        stop_flag.clone(),
    )) {
        info!("stopped while waiting for validator identity, exiting");
        return;
    }
    background_runtime().block_on(set_shred_receiver_addresses(
        admin_rpc_path.clone(),
        config.shred_receivers.clone(),
    ));
    background_runtime().spawn(monitor_identity(
        admin_rpc_path.clone(),
        identity_pubkey,
        stop_flag.clone(),
    ));

    let (crank_bundle_tx, crank_bundle_rx) = mpsc::channel(1000);
    let (crank_trigger_tx, crank_trigger_rx) = mpsc::channel(1000);
    let (bundle_tx, bundle_rx) = mpsc::channel(100_000);
    let connector_crank_enabled =
        config.jito.is_some() && config.client_variant == ClientVariant::Agave;
    let builder_is_connected = Arc::new(AtomicBool::new(false));

    let (bundle_receivers, block_engine_proxy) = if let Some(jito_config) = config.jito {
        let tip_manager_config = TipManagerConfig {
            tip_payment_program_id: jito_config.tip_payment_program_pubkey,
            tip_distribution_program_id: jito_config.tip_distribution_program_pubkey,
            tip_distribution_account_config: TipDistributionAccountConfig {
                merkle_root_upload_authority: jito_config.merkle_root_upload_authority,
                vote_account: jito_config.vote_account_pubkey,
                commission_bps: jito_config.mev_commission_bps,
            },
        };

        let tip_manager = TipManager::new(tip_manager_config);

        let mut full_rpc_url = jito_config.rpc_url;
        if let Some(api_key) = jito_config.rpc_api_key {
            full_rpc_url += "/?api-key=";
            full_rpc_url += &api_key;
        }

        let block_engine_urls =
            jito_config.block_engine_urls.map_or_else(default_block_engine_urls, |urls| {
                assert!(!urls.is_empty(), "jito.block_engine_urls must not be empty when set");
                info!(?urls, "using block engine urls from config");
                urls
            });
        metrics::BLOCK_ENGINES_CONFIGURED.set(block_engine_urls.len() as i64);

        let receiver_kp = identity_kp.insecure_clone();
        let maybe_proxy = if config.client_variant == ClientVariant::Jito {
            let block_engine_proxy_addr = jito_config
                .block_engine_proxy_addr
                .expect("jito.block_engine_proxy_addr is required when client_variant = \"jito\"");
            let proxy = BlockEngineProxyHandle::new(block_engine_proxy_addr);
            background_runtime().spawn(spawn_block_engine_proxy(proxy.clone()));
            info!(
                url = %proxy.advertised_url(),
                "local block-engine proxy enabled for Jito client"
            );
            Some(proxy)
        } else {
            None
        };

        background_runtime().spawn(spawn_bundle_loop(
            block_engine_urls.clone(),
            identity_kp.insecure_clone(),
            tip_manager,
            full_rpc_url,
            crank_bundle_tx,
            crank_trigger_rx,
            maybe_proxy.clone(),
        ));

        (Some((receiver_kp, block_engine_urls)), maybe_proxy)
    } else {
        warn!("no `jito` config set, skipping bundles");
        (None, None)
    };

    let handshake = Handshake {
        identity: identity_pubkey,
        conn_version: Metadata::get().to_string(),
        num_threads: config.num_workers as u8,
        filter_ofac: config.filter_ofac,
    };

    let (net_tx, bridge_rx) = rtrb::RingBuffer::new(1_000_000);
    let (bridge_tx, net_rx) = rtrb::RingBuffer::new(1_000_000);
    let (exec_tx, exec_rx) = rtrb::RingBuffer::new(1_000_000);

    let mut network_tile = NetworkTile::new(
        &config.relay_addrs,
        handshake,
        bridge_tx,
        bridge_rx,
        exec_rx,
        bundle_rx,
        block_engine_proxy.clone(),
        builder_is_connected.clone(),
        admin_rpc_path,
        config.shred_receivers,
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
                if network_tile.poll_delete_failsafe() {
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

    network_tile.wait_for_builder(&stop_flag);
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
                    allocator_handles: 2,
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
                network_tile.poll_startup();
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
    let mut allocs = allocators.into_iter();
    let network_allocator = allocs.next().unwrap();
    let connector_allocator = allocs.next().unwrap();

    let flag = stop_flag.clone();
    spawn(move || {
        thread_boot(Some(config.connector_network_core), Some(ThreadNiceness::High));
        while likely(StopCodes::running(&flag)) {
            network_tile.loop_body(&network_allocator);
        }
    });

    let bridge_tile = BridgeTile::new(
        tpu_to_pack,
        progress_tracker,
        workers,
        connector_crank_enabled,
        crank_bundle_rx,
        crank_trigger_tx,
        connector_allocator,
        Nanos::from_millis(config.slot_length_ms),
        config.client_variant,
        net_rx,
        net_tx,
        exec_tx,
    );

    let flag = stop_flag.clone();
    std::thread::spawn(move || {
        thread_boot(Some(config.connector_agave_core), Some(ThreadNiceness::High));
        bridge_tile.run(&flag);
    });

    metrics::READY.set(1);

    while StopCodes::running(&stop_flag) {
        std::thread::sleep(Duration::from_secs(1));
    }

    let code = StopCodes::from(stop_flag.load(Ordering::Relaxed));
    info!(?code, "received stop code, exiting");
}
