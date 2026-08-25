# Gravity Connector

Validator sidecar accepting connections from an external relay.

### Requirements

- Rust toolchain is pinned via [`rust-toolchain.toml`](rust-toolchain.toml) (`nightly-2026-03-10`); `rustup` will pick it up automatically.
- Agave 4.2, or Jito-Solana 4.2 or newer, running with `--enable-scheduler-bindings` so the connector can attach to the scheduler bindings IPC socket.

### Build

```sh
cargo build --profile release-prod
```

### Run

```sh
./target/release-prod/connector path/to/config.toml
```

See [`config.example.toml`](config.example.toml) for a documented sample config.

### Operator setup

#### Agave

Run Agave with scheduler bindings enabled so the connector can attach to the
validator's external scheduler IPC socket:

```sh
agave-validator \
  --enable-scheduler-bindings \
  ...
```

In the connector config, select the Agave client and point `ledger_path` at the
validator ledger. The connector derives `admin.rpc` and
`scheduler_bindings.ipc` from this directory. An empty Agave client table
disables Jito integration:

```toml
ledger_path = "/path/to/ledger"

[client.agave]
```

If regular Agave should receive Jito bundles through the external scheduler,
add the tip-management fields directly to `[client.agave]`. In that mode the
connector handles tip-management lifecycle transactions itself. The built-in
block-engine regions are used unless `client.agave.jito_block_engines` provides
a non-empty override. See [`config.example.toml`](config.example.toml) for every
required field.

#### Jito-Solana

Jito-Solana is the recommended validator client. The minimum supported
Jito-Solana version is 4.1.2.

Run Jito-Solana with scheduler bindings enabled and configure its block-engine
URL to the connector's local proxy. See the
[Jito-Solana command-line reference](https://jito-foundation.gitbook.io/mev/jito-solana/command-line-arguments)
when configuring the validator manually:

```sh
jito-solana-validator \
  --enable-scheduler-bindings \
  --block-engine-url http://127.0.0.1:11226 \
  --disable-block-engine-autoconfig \
  ...
```

Do not set `--bam-url` or any other BAM-specific parameters on Jito-Solana.
Gravity Connector does not integrate with BAM.

The connector config must select the Jito client and expose the local proxy on
the same host and port:

```toml
[client.jito]
block_engine_proxy_addr = "127.0.0.1:11226"
```

The connector field `client.jito.shred_receivers` corresponds to Jito-Solana's
`shred_receiver_addresses` (`--shred-receiver-address`), and
`client.jito.shred_retransmit_receivers` corresponds to
`shred_retransmit_receiver_addresses`
(`--shred-retransmit-receiver-address`). The connector overrides the validator's
values through its admin RPC, so repeat in the connector config any addresses
set directly on the validator. The connector keeps those configured addresses
and appends addresses received from the active relay. See
[`config.example.toml`](config.example.toml) for examples.

Do not configure Jito-Solana with a real public Jito block-engine URL in this
mode. Jito-Solana still needs a block-engine connection to receive block-builder
fee and tip-management data, but direct connections to public block engines can
also feed bundles and packets into Jito-Solana's own pipeline. That competes
with the external scheduler, which should own sequencing while the relay is
connected.

The connector runs a local block-engine proxy that solves both requirements. The validator
gets the block-builder and tip-management data it expects, while the connector
subscribes to the real Jito block engines internally and routes bundles and
packets through the external scheduler. If the relay is disconnected, the
proxy can forward block-engine traffic to Jito-Solana as a fallback. Use
`--disable-block-engine-autoconfig` so Jito-Solana does not auto-discover and
connect to public Jito endpoints outside the connector.

### Restart policy

Operators should run the connector with an always-restart policy. The connector may occasionally exit or panic on recoverable conditions, such as stale Agave progress or a prolonged relay disconnect, so the operator's restart policy should start a fresh connector process automatically.

For example, add the restart fields to a systemd service:

```ini
[Service]
Restart=always
RestartSec=2
```

Or add the restart policy to a Docker run command:

```sh
docker run --restart always ...
```

### Active/standby identities

Set `expected_identity` to the production validator public key when running the
connector on both active and standby machines:

```toml
expected_identity = "<VALIDATOR_IDENTITY_PUBKEY>"
identity_path = "/path/to/current-identity.json"
```

The connector first waits for Agave's live identity to equal
`expected_identity`. It then waits for `identity_path` to contain the matching
keypair before contacting a relay or connecting to the scheduler. This lets an
inactive machine keep a junk key at `identity_path` without making relay
authentication attempts. Once the production key is copied and Agave switches
to the production identity, the waiting connector starts automatically.

When `expected_identity` is omitted, the connector retains the legacy behavior
and derives the expected public key from `identity_path` at startup.

### In-memory identities

Validators whose managed key service injects the identity into Agave with
`setIdentityFromBytes` can configure the connector without a keypair file.
This targets the three-argument admin RPC used by Agave and Jito-Solana 4.2+.

```toml
expected_identity = "<VALIDATOR_IDENTITY_PUBKEY>"
# identity_path intentionally omitted
```

The connector creates an owner-only IPC socket at
`<ledger_path>/gravity-admin/admin.rpc` and exposes only Agave's compatible
`setIdentityFromBytes(bytes, require_tower, require_vote_history)` method. The
two boolean parameters are accepted for wire compatibility but have no effect
in the connector. The managed key service must send the identity to both
Agave's `admin.rpc` and the connector's socket.

Because the socket follows Agave's `<ledger>/admin.rpc` layout, the standard
Agave CLI can target it by using `<ledger_path>/gravity-admin` as its ledger.
Pass the JSON keypair on stdin; supplying a file path makes the CLI call the
unsupported `setIdentity` method instead:

```sh
agave-validator --ledger <ledger_path>/gravity-admin set-identity
```

The connector rejects malformed keypair bytes or a keypair whose public key is
not `expected_identity`. It accepts one valid identity per process; retries with
the same identity succeed without replacing it. It keeps the accepted keypair
in memory and waits for Agave's live identity to match before connecting to a
relay.

If Agave switches to a fallback identity while the connector is running, the
connector exits with `AGAVE_IDENTITY_MISMATCH`. After the restart policy starts
a new connector process, the managed key service must inject the identity into
that process again because the keypair is not persisted. This injection may
happen before Agave switches back; the connector keeps the keypair in memory
and waits for Agave's live identity to match.

### Runtime notes

- If Agave has not created the scheduler bindings IPC socket yet, the connector logs the error and retries the connection every 10 seconds.
- Before the initial IPC handshake, the connector retries until Agave is ready. After a successful handshake, if Agave stops sending progress updates for more than 2 seconds, the connector exits with stop code `AGAVE_NO_PROGRESS`; the restarted process then performs a fresh IPC handshake.
- The connector dials the relays listed in `relay_addrs` and reconnects automatically while any of them is down. Relay entries should use `tcp://host:port` URLs; legacy IP-and-port entries remain accepted. If multiple endpoints are available for a single region, list all of them for redundancy. Hostnames are resolved off the latency-sensitive connector thread and resolved again after disconnects. If DNS returns multiple addresses, the connector rotates through them after connection failures.
- Relay URLs use the existing plaintext TCP transport; DNS names do not enable TLS or authenticate the relay host. DNS changes do not move a healthy connection and take effect when that connection disconnects.
- The CPU core configured by `connector_core` is dedicated to the connector and is expected to run at or near 100% utilization for optimal performance. Operators should not co-locate other workloads on that core.
- For validators running Jito-Solana, bind `client.jito.block_engine_proxy_addr` to localhost unless the network is otherwise trusted. The local proxy only implements the auth surface needed by Jito-Solana and does not validate bearer tokens on block-engine RPCs.
- If the connector enters a sequencing leader slot and receives no valid schedule, it writes a failsafe file and exits; see shutdown behavior below.

### Monitoring

- The connector serves `/metrics` (Prometheus text, all metrics prefixed `gravity_connector_`) and `/health` (JSON summary, `200` when healthy and `503` otherwise) on `metrics_addr`, default `0.0.0.0:9093`. Set it to a specific interface to limit exposure, or firewall the port to your scrapers. The listener is bound early, before the identity and relay waits, so it is scrapeable during startup. Liveness needs no dedicated endpoint: Prometheus already synthesises `up` per scrape.
- Metrics cover connection status only: the Agave link, the relay connection, and the Jito block engine streams. `gravity_connector_healthy` carries the same verdict as `/health`.
- `/health` is `200` only once the connector is past startup with both Agave and a relay connected, using the same 2 second progress threshold the connector exits on. The JSON body names the failing check. Block engine connectivity is reported but not part of the verdict, since the connector still works for non-Jito flow with every upstream down.

### Shutdown behavior

- On startup, the connector waits for a relay connection before connecting to Agave. If no relay accepts, it keeps retrying and logs that it is still waiting; this is not a stop condition.
- While running, if the active relay disconnects, the connector keeps Agave connected and retries that relay in the background. If another relay connection is already available, the connector switches to it. If no relay is connected for 10 minutes, the connector panics; the operator's restart policy should start it again.
- If Agave stops sending progress updates for more than 2 seconds, the connector sets stop code `AGAVE_NO_PROGRESS` and exits. This usually means the validator restarted or the scheduler bindings connection is stale; the connector does not reconnect in-process after a completed handshake, so operators should rely on the restart policy to start a fresh connector process.
- If Agave's admin RPC reports an identity different from `expected_identity`
  (or from the configured `identity_path` keypair when `expected_identity` is
  omitted), the connector sets stop code
  `AGAVE_IDENTITY_MISMATCH` and exits. On restart it remains in the startup
  identity wait until Agave reports the configured identity again.
- If a sequencing leader slot completes without any valid schedule from the relay, the connector writes the failsafe file and panics. The failsafe file is a local JSON marker at `~/.local/share/gravity-connector/failsafe.json` recording the safety stop reason and timestamp. On restart, the connector stays blocked by that marker and logs it periodically, so Agave continues on its vanilla scheduling path until the failsafe expires or the relay sends a delete-failsafe request.
