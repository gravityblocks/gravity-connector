# Gravity Connector

Validator sidecar connecting to [GBX](https://docs.gravityblocks.xyz), an open
and competitive marketplace for Solana blockspace.

## Compatibility

| Validator client             | Branch                                                                                                |
| ---------------------------- | ----------------------------------------------------------------------------------------------------- |
| Agave / Jito-Solana **v4.3** | `main`                                                                                                |
| Agave / Jito-Solana **v4.4** | [`su/schduler-binding-6-0`](https://github.com/gravityblocks/gravity-connector/tree/su/schduler-binding-6-0) ([PR #58](https://github.com/gravityblocks/gravity-connector/pull/58)) |

Build the connector from the branch that matches your validator version. The
scheduler bindings protocol differs between versions, so a mismatched build
cannot attach to the validator.

## Build and run

The Rust toolchain is pinned in [`rust-toolchain.toml`](rust-toolchain.toml).

```sh
cargo build --profile release-prod
./target/release-prod/connector path/to/config.toml
```

[`config.example.toml`](config.example.toml) documents every config field.

## Validator setup

Always start the validator with `--enable-scheduler-bindings`. Set the
connector's `ledger_path` to the validator ledger. The connector finds
`admin.rpc` and `scheduler_bindings.ipc` there.

### Jito-Solana (recommended)

```sh
jito-solana-validator \
  --enable-scheduler-bindings \
  --block-engine-url http://127.0.0.1:11226 \
  --disable-block-engine-autoconfig \
  ...
```

```toml
[client.jito]
block_engine_proxy_addr = "127.0.0.1:11226"
```

- Point Jito-Solana only at the connector's local block-engine proxy. The proxy
  gives the validator the block-builder and tip data it needs. Meanwhile, the
  connector subscribes to the real Jito block engines and routes their bundles
  through the external scheduler. If the validator also connected to a public
  block engine, it would compete with the scheduler. While the relay is
  disconnected, the proxy forwards block-engine traffic to the validator as a
  fallback.
- `--disable-block-engine-autoconfig` stops Jito-Solana from finding and
  connecting to public endpoints on its own.
- Do not set `--bam-url` or other BAM flags. The connector does not support BAM.
- Bind the proxy to localhost. It does not validate bearer tokens.
- `client.jito.shred_receivers` and `shred_retransmit_receivers` replace the
  validator's `--shred-receiver-address` and
  `--shred-retransmit-receiver-address` values through admin RPC. Copy any
  addresses you set on the validator into these lists. The connector adds the
  relay's addresses after yours.

### Agave

```toml
[client.agave]
```

With an empty `[client.agave]` table, Jito integration is off. To receive Jito
bundles on plain Agave, add the tip-management fields to `[client.agave]`. The
connector then handles tip-management transactions itself. See
[`config.example.toml`](config.example.toml).

## Identity

Choose one of the following modes:

- **Keypair file**: set `identity_path`. If `expected_identity` is not set, the
  connector takes the public key from this file at startup.
- **Active/standby**: set both `expected_identity` and `identity_path`. The
  connector waits until Agave's live identity equals `expected_identity`, and
  then until `identity_path` holds the matching keypair. Only after that does
  it contact a relay. A standby machine can keep a junk key at `identity_path`
  and starts automatically after failover.
- **In-memory** (needs Agave 4.3+ or Jito-Solana 4.2+): set only
  `expected_identity`. The connector opens an owner-only socket at
  `<ledger_path>/gravity-admin/admin.rpc` that accepts
  `setIdentityFromBytes`. Your key service must send the identity to both the
  validator and the connector. You can also use the Agave CLI, with the
  keypair JSON on stdin:

  ```sh
  agave-validator --ledger <ledger_path>/gravity-admin set-identity < identity.json
  ```

  The keypair is never saved to disk. After every connector restart, send the
  identity again.

In every mode, if Agave's identity changes away from the expected one, the
connector exits with `AGAVE_IDENTITY_MISMATCH`. On restart, it waits until the
identity matches again.

## Deployment

The connector exits on purpose when it hits recoverable conditions (see
[Lifecycle](#lifecycle)). Run it under a supervisor that always restarts it.

### What the process needs

| Resource       | Requirement                                                                                                                                                         |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| User           | The validator's user, or another user that can connect to the validator's unix sockets in the ledger directory.                                                     |
| Ledger dir     | Read-write. The connector connects to `admin.rpc` and `scheduler_bindings.ipc`, and creates `gravity-admin/`, which holds `failsafe.json` and the in-memory identity socket. |
| Identity file  | Read-only, if `identity_path` is set.                                                                                                                               |
| Log dir        | Writable, if `[logging.file]` is on. The default is `~/.local/share/gravity-connector/logs`.                                                                        |
| Shmem dir      | Writable: `~/.local/share/gravity-connector/shmem`. Holds the relay UDP flow's send telemetry queues, a few hundred KiB per flow socket, removed with the socket. |
| `CAP_SYS_NICE` | Lets the connector thread run at niceness `-10`. Without it, the connector logs a warning and runs at normal priority.                                              |
| CPU            | `connector_core` runs at about 100%. Don't give that core to anything else, such as PoH or XDP threads. If the process is limited to certain CPUs, include this core. |
| Network        | Outbound TCP and UDP to `relay_addrs`; TCP to the Jito block engines. Inbound on `metrics_addr` (default `0.0.0.0:9093`) and on the block-engine proxy, from the validator.       |
| Socket buffers | Set `net.core.wmem_max` and `net.core.rmem_max` to at least `134217728` on the host. The connector requests 64 MiB buffers on relay sockets and warns if the kernel lowers that. |

No `/dev/shm`, `--ipc=host`, or hugepage setup is needed. The validator sends
its shared memory to the connector as file descriptors over
`scheduler_bindings.ipc`.

### systemd

```ini
[Service]
User=sol
ExecStart=/usr/local/bin/gravity-connector /etc/gravity-connector/config.toml
Restart=always
RestartSec=2
AmbientCapabilities=CAP_SYS_NICE
CapabilityBoundingSet=CAP_SYS_NICE
# If you use ProtectSystem=strict or a similar setting, the ledger must stay writable:
# ReadWritePaths=/path/to/ledger
```

### Docker

Docker is not recommended. If you use it anyway, here is an example:

```yaml
services:
  connector:
    image: gravity-connector
    command: ["/config/config.toml"]
    restart: always
    network_mode: host # the validator reaches the proxy on 127.0.0.1; uses host sysctls
    user: "1000:1000" # validator's uid:gid
    cap_add:
      - SYS_NICE # needed even when the container runs as root
    volumes:
      - /path/to/ledger:/path/to/ledger # same path as ledger_path; read-write
      - /path/to/identity.json:/path/to/identity.json:ro
      - ./config.toml:/config/config.toml:ro
```

- Mount the ledger read-write. `failsafe.json` is stored there, so it survives
  when the container is recreated. Don't move it to container storage that is
  wiped on recreation.
- If you set `cpuset`, include `connector_core`. Core numbers are host core
  numbers.

## Lifecycle

- **Startup**: the connector waits for the expected identity, then for a relay
  control connection and a UDP flow connection, then for the validator's scheduler bindings socket (it retries
  every 10s). None of these waits stops the process.
- **Relay loss**: the connector stays attached to Agave and reconnects in the
  background. If another relay has both control and UDP flow connected, it switches to that one. If no
  relay is connected for 10 minutes, the connector panics.
- **Agave stalls**: if the validator sends no progress for more than 2s after
  the handshake, the connector exits with `AGAVE_NO_PROGRESS`. A new process
  does a fresh handshake.
- **Identity mismatch**: the connector exits with `AGAVE_IDENTITY_MISMATCH`.
- **Failsafe**: if a sequencing leader slot ends without a valid schedule from
  the relay, the connector keeps running until the leadership ends, then writes
  `<ledger_path>/gravity-admin/failsafe.json` and panics. While that file
  exists, a restarted connector does not attach to the validator, so Agave
  keeps using its own scheduler. The failsafe does not expire automatically;
  the connector waits until the relay explicitly clears it. The
  `gravity_connector_failsafe_active` metric is `1` during this time.

Relays: use `tcp://host:port` in `relay_addrs`, and list every endpoint of a
region for redundancy. The connector reaches each relay over both TCP and UDP
on that host and port, so allow outbound TCP and UDP to it through your
firewall. Each connector should have its own public source IP: connectors sharing one
address (behind the same NAT, say) fail to connect.

## Monitoring

`metrics_addr` (default `0.0.0.0:9093`) serves:

- `/metrics`: Prometheus metrics with the `gravity_connector_` prefix. They
  cover the Agave link, the relay connection, and the Jito block-engine streams.
- `/health`: a JSON summary. It returns `200` when the connector is past
  startup and both Agave and a relay are connected, and `503` otherwise. The
  body names the check that failed. Block-engine connectivity is reported but
  does not affect the result.

The listener starts before the identity and relay waits, so you can scrape it
during startup. Bind it to a specific interface, or firewall it so only your
scrapers can reach it.
