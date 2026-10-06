# Xiaozhi Desk MQTT Gateway

The Desk Robot gateway forwards MQTT control messages and encrypted UDP Opus
packets to one Xiaozhi WebSocket backend per device session. It includes its own
lightweight MQTT endpoint: **no external MQTT broker is required**. This endpoint
supports the Xiaozhi device path with MQTT 3.1/3.1.1 and QoS 0; it is not a general
purpose persistent broker.

```text
Desk Robot firmware / xiaozhi-esp32
        MQTT control + AES-128-CTR UDP Opus
                         |
              xiaozhi-desk-mqtt-gateway
                         |
          Xiaozhi protocol v2 WebSocket
                         |
          xiaozhi-server / xiaozhi-esp32-server core
```

The gateway owns MQTT connections, UDP keys/sequences and WebSocket transport
bridging. The core server owns conversations and AI sessions. The gateway does
not perform ASR, LLM, VLM or TTS compute and does not participate in NATS worker
scheduling. Audio is forwarded as existing Opus packets without encoding.

## Run

The package retains Node.js 20 or later as its compatibility baseline. For
production, use a supported LTS release, currently Node.js 22 or 24; Node.js 18
and 20 are EOL ([release status](https://nodejs.org/en/about/previous-releases)).
The newest required built-in API is HTTP
[`closeAllConnections()`](https://nodejs.org/api/http.html#servercloseallconnections),
introduced in Node.js 18.2. There is no Node.js 20-only API requirement, but the
baseline does not extend support to the EOL Node.js 18 line.

From the repository root, install production dependencies from the committed lockfile:

```sh
npm ci --omit=dev
cp config/mqtt.json.example config/mqtt.json
# Set backend URLs in config/mqtt.json and create .env as described below.
npm start
```

`config/mqtt.json` and `.env` are local configuration, excluded from Git. Do not
publish either file with credentials. Commit `package-lock.json` alongside any
dependency changes so each cluster node installs the same resolved versions.

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `MQTT_SIGNATURE_KEY` | Required | Shared HMAC key for device MQTT credentials; match the server that provisions them. |
| `SERVER_SECRET` | Required | Shared HMAC key for backend WebSocket authentication; match the core server. |
| `PUBLIC_IP` | `mqtt.xiaozhi.me` (upstream default) | Address advertised to devices in the UDP hello; set this to your reachable gateway hostname/IP. |
| `GATEWAY_ID` | OS hostname | Instance identity in status and lifecycle logs, for example `deskb1x`. |
| `MQTT_PORT` | `1883` | MQTT TCP listener. |
| `UDP_PORT` | Same as MQTT port | Encrypted audio UDP listener and advertised port. |
| `HTTP_PORT` | `8007` | Health/status HTTP listener; replaces the former admin API port. |
| `MQTT_HOST` | Unspecified | Optional IPv4 bind address for MQTT TCP. |
| `UDP_HOST` | Unspecified | Optional IPv4 bind address for encrypted UDP audio. |
| `HTTP_HOST` | Unspecified | Optional IPv4 bind address for health/status; use `127.0.0.1` for node-local checks. |
| `ALLOW_INSECURE_MQTT` | `false` | Set exactly `true` to allow unsigned MQTT credentials when the MQTT key is absent, only for explicit local development. |

Example `.env` (replace the placeholders):

```dotenv
GATEWAY_ID=deskb1x
PUBLIC_IP=deskb1x.example.net
MQTT_SIGNATURE_KEY=replace-with-device-provisioning-key
SERVER_SECRET=replace-with-backend-authentication-key
MQTT_PORT=1883
UDP_PORT=1883
HTTP_PORT=8007
```

Secure startup is the default, independent of `NODE_ENV`. Missing secrets fail
startup before listeners open. `SERVER_SECRET` remains required even with the
local MQTT escape hatch. The gateway never falls back to a test token, prints
passwords or logs authentication tokens. Modern three-part IDs carry a Base64
JSON username; legacy two-part IDs must also pass the configured signature check.
The MQTT signature remains Base64 HMAC-SHA256 of `clientId|username`.

Unspecified bind addresses retain the existing default-interface behavior.
Explicit `MQTT_HOST`, `UDP_HOST` and `HTTP_HOST` values must be IPv4 addresses;
invalid values fail startup before listeners open. `PUBLIC_IP` is the advertised
UDP destination, not the listener bind address. A single-node LAN deployment can
bind MQTT/UDP to that node's management IP and health to `127.0.0.1`.
For three gateways behind a TCP load balancer, bind MQTT/health to private node
addresses and UDP to each node's reachable management address. Advertise that
selected node's own UDP address, so UDP does not get balanced to another instance.
The built-in MQTT TCP listener
does not terminate TLS; provide TLS termination where required. Restrict the
health/status port at the network boundary. Status contains counts, listener
ports and listener states only, with no device identifiers, backend URLs or secrets. The existing
UDP AES-CTR protocol does not provide cryptographic integrity; the gateway retains
that wire format and allows the UDP return address to follow valid packet sequence
progression, as in upstream.

## Backend configuration

`config/mqtt.json.example` documents the JSON structure:

```json
{
  "production": {
    "chat_servers": [
      "ws://deskb1x:8000/xiaozhi/v1/",
      "ws://deskb2x:8000/xiaozhi/v1/",
      "ws://deskb3x:8000/xiaozhi/v1/"
    ]
  },
  "development": {
    "chat_servers": ["ws://localhost:8180/xiaozhi/v1/"],
    "mac_addresss": []
  },
  "backend_connect_timeout_ms": 2500,
  "debug": false,
  "max_mqtt_payload_size": 8192,
  "mcp_client": {
    "capabilities": {},
    "client_info": {"name": "xiaozhi-mqtt-client", "version": "1.0.0"},
    "max_tools_count": 128
  }
}
```

Set the paths and ports to those exposed by your core servers. `chat_servers`
accepts `ws://` or `wss://` URLs without embedded credentials, fragments or query
parameters. URLs containing `?` are rejected, including an existing
`?from=mqtt_gateway` marker. The gateway preserves the configured path and adds
exactly the sole query marker `?from=mqtt_gateway`, matching the current core
server's exact request-path suffix check.
The production list must be nonempty; the optional development section must also
have a nonempty list if present. The upstream spelling `mac_addresss` is retained
for compatibility. MAC addresses listed there select development backends.

New sessions use deterministic round-robin selection within their production or
development list. If the first backend fails during WebSocket handshake or hello,
the bridge tries each remaining distinct backend once. Each attempt has the
configured timeout, covering both handshake and hello. The total establishment
window is bounded by backend count times timeout. Keep that window below the
firmware hello deadline (currently ten seconds); the default allows three attempts
within 7.5 seconds. If all attempts fail, the device
gets an error and the pending session is cleaned up. Its MQTT connection can send
a new hello to retry.

Once a backend returns a valid hello, the session stays on that backend until it
ends. Losing an established backend ends the session with goodbye; no live
migration or cross-instance MQTT/session replication occurs. Repeated device
hello replaces the previous bridge and creates fresh UDP session keys. Each
gateway instance balances independently; there is no shared round-robin state.

The configuration file is watched. Valid reloads affect new sessions and retain
existing sessions. Invalid reloads keep the last valid configuration. Initial
missing/invalid configuration fails startup.

`max_mqtt_payload_size` limits complete incoming MQTT packets and backend JSON
messages. MCP initialize and paginated tools/list are prefetched and cached per
MQTT connection. Dotted configuration lookup honors `mcp_client.max_tools_count`.
A complete cache answers backend initialize/tools/list and absorbs initialized
notifications; other MCP traffic still reaches the device. If prefetch fails,
MCP forwarding remains available directly. Prefetch requests have ten-second
timeouts and repeated pagination cursors are rejected.

## Health and shutdown

| GET endpoint | Meaning |
| --- | --- |
| `/healthz` | HTTP 200 while the process serves requests. |
| `/readyz` | HTTP 200 when MQTT and UDP are listening and a validated backend configuration is available; HTTP 503 otherwise. |
| `/status` | Compact JSON: `status`, `gateway_id`, `active_mqtt_connections`, `active_websocket_sessions`, `configured_backend_count`, `listener_ports` (MQTT/UDP/HTTP), `mqtt_listening`, `udp_listening`, `http_listening`. |

Readiness checks local listeners/configuration, not live backend availability.
Configured backend count is the number of distinct URLs across both lists.
`active_websocket_sessions` counts established, live backend bridges. It clears
on session closure and does not count an idle persistent MQTT connection. A
local panel can use this count to show which gateway holds a robot transport
session; it does not indicate provider execution or replicate session state.

`SIGTERM` and `SIGINT` initiate the same idempotent shutdown. The gateway stops
accepting sessions, closes configuration watchers and timers, destroys MQTT
sockets, and closes MQTT, UDP and HTTP listeners. Established WebSockets get a
close handshake, followed by termination after 1.5 seconds if necessary. In-flight
establishment is cancelled. The process exits naturally after cleanup, suitable
for a systemd service without a premature `process.exit()`.

## Scope and attribution

This Desk Robot fork intentionally removes upstream device-to-device calling,
remote wakeup for calls, admin command/status/call APIs, daily admin bearer tokens,
Express and Opus encoding/silence synthesis. Normal robot-to-core bridging and
device MCP forwarding/cache remain. No firmware, OTA, NATS, compute distribution,
deployment automation or live-session failover is implemented here.

Derived from [78/xiaozhi-mqtt-gateway](https://github.com/78/xiaozhi-mqtt-gateway)
and the [xinnan-tech server integration](https://github.com/xinnan-tech/xiaozhi-esp32-server).
Original gateway author: terrence@tenclass.com. See [LICENSE](LICENSE) for the
retained upstream license and attribution.

Offline listener/status fixtures use only built-in Node modules and open no sockets:

```sh
node test/listener-bindings.js
```

## Cluster core placement

Optional `core_nodes` declares exactly three distinct `{node_id, ws_url,
status_url}` objects. Each WS endpoint must also appear in the mode's existing
`chat_servers`; the status URL uses HTTP(S) `/status` on the same host, with no
credentials/query/fragment. `GATEWAY_ID` must match a node identity. Omitting
`core_nodes` keeps the original external-backend rotation.

For each new hello, the gateway takes a config snapshot and reads all three
statuses concurrently, with a 500 ms total deadline per node and an 8192-byte
response bound. No redirects/proxies are used. Status protocol must be
`xiaozhi-core-transport-v1`, identity must match, readiness must be `ready`, and
VIP ownership must be boolean. Unknown/unavailable cores are excluded. Multiple
reported VIP owners block selection. Candidate order is: remote non-VIP cores,
remote VIP core, local non-VIP core, local VIP core. Equal candidates rotate from
randomized startup position; handshake failures try the next candidate. A hello
must confirm the selected core identity. Config reload and VIP movement affect
new sessions only. There is no busy threshold or session replication.

Private `/status` includes `active_core_sessions`, a count by validated core ID
for established bridges only. No URLs, device IDs or authentication data appear
in this field. Core transport readiness does not imply working providers: the
initial isolated core reports empty capabilities and no conversation runtime.
It has no bootstrap or Vision HTTP endpoint. This phase supports transport probes;
real ASR/LLM/TTS/MCP/VLM acceptance remains a later step.
