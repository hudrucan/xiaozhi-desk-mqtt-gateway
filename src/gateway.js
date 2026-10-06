const net = require("node:net");
const dgram = require("node:dgram");
const crypto = require("node:crypto");
const os = require("node:os");
const { ConfigManager, integer } = require("./config");
const { MQTTConnection } = require("./mqtt-connection");
const { createHealthServer } = require("./health-server");

function bindHost(value, name) {
  if (value === undefined) return undefined;
  if (net.isIP(value) !== 4) throw new Error(`${name} must be an IPv4 bind address`);
  return value;
}

class Gateway {
  constructor() {
    this.id = process.env.GATEWAY_ID || os.hostname();
    if (!/^[A-Za-z0-9_.-]{1,128}$/.test(this.id)) throw new Error("Invalid GATEWAY_ID");
    this.settings = {
      mqttPort: integer(process.env.MQTT_PORT, "MQTT_PORT", 1883),
      udpPort: integer(process.env.UDP_PORT, "UDP_PORT", Number(process.env.MQTT_PORT ?? 1883)),
      httpPort: integer(process.env.HTTP_PORT, "HTTP_PORT", 8007),
      mqttHost: bindHost(process.env.MQTT_HOST, "MQTT_HOST"),
      udpHost: bindHost(process.env.UDP_HOST, "UDP_HOST"),
      httpHost: bindHost(process.env.HTTP_HOST, "HTTP_HOST"),
      publicIp: process.env.PUBLIC_IP || "mqtt.xiaozhi.me",
      allowInsecureMqtt: process.env.ALLOW_INSECURE_MQTT === "true",
      mqttSignatureKey: process.env.MQTT_SIGNATURE_KEY,
      serverSecret: process.env.SERVER_SECRET,
    };
    if (!this.settings.mqttSignatureKey?.trim() && !this.settings.allowInsecureMqtt) {
      throw new Error("MQTT_SIGNATURE_KEY is required (or explicitly set ALLOW_INSECURE_MQTT=true for local development)");
    }
    if (!this.settings.serverSecret?.trim()) throw new Error("SERVER_SECRET is required for backend authentication");
    this.config = new ConfigManager();
    this.connections = new Map();
    this.clientIdMap = new Map();
    this.bridges = new Set();
    this.roundRobin = { production: 0, development: 0 };
    this.listeners = { mqtt: false, udp: false, http: false };
    this.startOperations = [];
    this.stopping = false;
    this.stopPromise = null;
    this.keepAliveTimer = null;
    this.config.on("configChanged", () => this.log("Configuration reloaded; existing sessions stay on their backends"));
    this.config.on("configError", () => this.log("Configuration reload/watch failed; keeping the last valid configuration"));
  }

  log(message) { console.log(`[gateway_id=${this.id}] ${message}`); }

  debug(message) { if (this.config.get("debug") === true) this.log(message); }

  selectBackends(macAddress) {
    const devMacs = this.config.get("development.mac_addresss") || [];
    const mode = devMacs.some((mac) => mac.toLowerCase() === macAddress) ? "development" : "production";
    // Snapshot the list at session establishment. Reloads affect only new sessions.
    const servers = [...new Set(this.config.get(`${mode}.chat_servers`))];
    const offset = this.roundRobin[mode] % servers.length;
    this.roundRobin[mode] = (offset + 1) % servers.length;
    return servers.slice(offset).concat(servers.slice(0, offset));
  }

  backendCount() {
    return new Set([...(this.config.get("production.chat_servers") || []),
      ...(this.config.get("development.chat_servers") || [])]).size;
  }

  isReady() {
    return !this.stopping && this.listeners.mqtt && this.listeners.udp && this.backendCount() > 0;
  }

  status() {
    return { status: this.stopping ? "stopping" : this.isReady() ? "ready" : "not_ready",
      gateway_id: this.id,
      active_mqtt_connections: [...this.connections.values()].filter((conn) => conn.protocol.isConnected).length,
      active_websocket_sessions: [...this.bridges].filter((bridge) => bridge.isAlive()).length,
      configured_backend_count: this.backendCount(),
      listener_ports: { mqtt: this.settings.mqttPort, udp: this.settings.udpPort,
        http: this.settings.httpPort },
      mqtt_listening: this.listeners.mqtt, udp_listening: this.listeners.udp,
      http_listening: this.listeners.http };
  }

  listen(server, kind, port) {
    const operation = new Promise((resolve, reject) => {
      const failed = () => {
        server.off("listening", listening);
        reject(new Error(`${kind.toUpperCase()} listener could not start on port ${port}`));
      };
      const listening = () => {
        server.off("error", failed);
        this.listeners[kind] = true;
        this.log(`${kind.toUpperCase()} listening port=${port}`);
        resolve();
      };
      server.once("error", failed);
      server.once("listening", listening);
      server.on("close", () => { this.listeners[kind] = false; });
      server.on("error", () => {
        if (this.listeners[kind] && !this.stopping) {
          this.log(`${kind.toUpperCase()} listener failed; shutting down`);
          process.exitCode = 1;
          this.stop().catch(() => { process.exitCode = 1; });
        }
      });
      if (kind === "udp") server.bind(port, this.settings.udpHost);
      else server.listen(port, this.settings[`${kind}Host`]);
    });
    this.startOperations.push(operation);
    return operation;
  }

  async start() {
    this.log(`Starting MQTT/UDP transport configured_backends=${this.backendCount()}`);
    if (this.settings.allowInsecureMqtt && !this.settings.mqttSignatureKey) {
      this.log("WARNING: unsigned MQTT credentials explicitly enabled for local development");
    }
    this.config.watch();
    this.mqttServer = net.createServer((socket) => {
      if (this.stopping) { socket.destroy(); return; }
      socket.setNoDelay(true);
      let id;
      do { id = crypto.randomBytes(4).readUInt32BE(); } while (this.connections.has(id));
      const connection = new MQTTConnection(socket, id, this);
      this.connections.set(id, connection);
    });
    await this.listen(this.mqttServer, "mqtt", this.settings.mqttPort);
    if (this.stopping) return;
    this.udpServer = dgram.createSocket("udp4");
    this.udpServer.on("message", (message, rinfo) => this.onUdpMessage(message, rinfo));
    await this.listen(this.udpServer, "udp", this.settings.udpPort);
    if (this.stopping) return;
    this.httpServer = createHealthServer(this);
    await this.listen(this.httpServer, "http", this.settings.httpPort);
    if (this.stopping) return;
    this.keepAliveTimer = setInterval(() => {
      for (const connection of this.connections.values()) connection.checkKeepAlive();
    }, 1000);
    this.log("Gateway ready");
  }

  authenticateConnection(connection) {
    const previous = this.clientIdMap.get(connection.clientId);
    if (previous && previous !== connection) previous.close();
    this.clientIdMap.set(connection.clientId, connection);
  }

  removeConnection(connection) {
    this.connections.delete(connection.connectionId);
    if (this.clientIdMap.get(connection.clientId) === connection) this.clientIdMap.delete(connection.clientId);
    this.log(`MQTT closed connection=${connection.connectionId}`);
  }

  onUdpMessage(message, rinfo) {
    // [type:u8, flags:u8, length:u16, cookie:u32, timestamp:u32, sequence:u32, encrypted Opus]
    if (this.stopping || message.length < 17 || message.length > 65507 ||
        message[0] !== 1 || message[1] !== 0) return;
    const length = message.readUInt16BE(2);
    if (!length || message.length !== 16 + length) return;
    const connection = this.connections.get(message.readUInt32BE(4));
    if (!connection?.protocol.isConnected) return;
    this.debug(`UDP packet connection=${connection.connectionId} bytes=${length}`);
    connection.onUdpMessage(rinfo, message, length, message.readUInt32BE(8), message.readUInt32BE(12));
  }

  sendUdpMessage(message, remoteAddress) {
    if (this.stopping || !this.listeners.udp) return;
    this.udpServer.send(message, remoteAddress.port, remoteAddress.address, (error) => {
      if (error && !this.stopping) this.log("UDP send failed");
    });
  }

  stop() {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    this.stopPromise = this.shutdown();
    return this.stopPromise;
  }

  async shutdown() {
    clearInterval(this.keepAliveTimer);
    this.config.close();
    // Let any in-flight bind finish before closing it. start() stops at the next await.
    await Promise.allSettled(this.startOperations);
    const bridgeClosures = [...this.bridges].map((bridge) => bridge.close());
    for (const connection of this.connections.values()) connection.close();
    const closeListener = (server, kind) => new Promise((resolve) => {
      if (!server || !this.listeners[kind]) { resolve(); return; }
      server.close(() => resolve());
      if (kind === "http") server.closeAllConnections();
    });
    await Promise.all([...bridgeClosures,
      closeListener(this.mqttServer, "mqtt"), closeListener(this.udpServer, "udp"),
      closeListener(this.httpServer, "http")]);
    this.connections.clear();
    this.clientIdMap.clear();
    this.bridges.clear();
    this.log("Gateway stopped");
  }
}

module.exports = { Gateway };
