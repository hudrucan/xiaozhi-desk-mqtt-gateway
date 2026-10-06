const crypto = require("node:crypto");
const { MQTTProtocol } = require("../mqtt-protocol");
const { validateMqttCredentials } = require("./mqtt-auth");
const { WebSocketBridge, validateMessage } = require("./websocket-bridge");
const { isObject } = require("./config");

class MQTTConnection {
  constructor(socket, connectionId, server) {
    this.server = server;
    this.connectionId = connectionId;
    this.createdAt = Date.now();
    this.realClientIp = socket.remoteAddress?.replace(/^::ffff:/, "");
    this.closing = false;
    this.bridge = null;
    this.udp = null;
    this.mcpPendingRequests = new Map();
    this.mcpRequestId = 10000;
    this.mcpCachedTools = [];
    this.mcpCacheReady = false;
    this.protocol = new MQTTProtocol(socket, server.config);
    this.protocol.on("connect", (data) => this.handleConnect(data));
    this.protocol.on("publish", (data) => this.handlePublish(data));
    this.protocol.on("disconnect", () => this.close());
    this.protocol.on("close", () => this.close());
    this.protocol.on("error", () => this.close());
    this.protocol.on("protocolError", () => {
      server.log(`Invalid MQTT packet connection=${connectionId}; closing connection`);
      this.close();
    });
  }

  handleConnect(data) {
    try {
      Object.assign(this, validateMqttCredentials(data.clientId, data.username,
        data.password, this.realClientIp, this.server.settings));
      this.clientId = data.clientId;
      this.server.authenticateConnection(this);
      this.protocol.acceptConnect();
      this.server.log(`MQTT connected connection=${this.connectionId}`);
      this.initializeDeviceTools();
    } catch {
      this.server.log(`MQTT authentication rejected connection=${this.connectionId}`);
      this.protocol.rejectConnect();
      // No MCP requests or bridge exist yet. Socket close completes cleanup.
    }
  }

  handlePublish(data) {
    try {
      const message = validateMessage(JSON.parse(data.payload));
      if (message.type === "hello") {
        this.parseHelloMessage(message).catch(() => this.close());
      } else {
        this.parseOtherMessage(message);
      }
    } catch {
      this.server.log(`Invalid MQTT message connection=${this.connectionId}; closing connection`);
      this.close();
    }
  }

  sendMqttMessage(payload) {
    if (this.closing) return;
    try { this.protocol.sendPublish(this.replyTo, payload); }
    catch {
      this.server.log(`MQTT send failed connection=${this.connectionId}; closing connection`);
      this.close();
    }
  }

  generateUdpHeader(length, timestamp, sequence) {
    const header = Buffer.alloc(16);
    header[0] = 1;
    header.writeUInt16BE(length, 2);
    header.writeUInt32BE(this.connectionId, 4);
    header.writeUInt32BE(timestamp, 8);
    header.writeUInt32BE(sequence, 12);
    return header;
  }

  sendUdpMessage(payload, timestamp) {
    if (!this.udp?.remoteAddress || !this.bridge?.isAlive()) return;
    if (this.udp.localSequence === 0xffffffff) {
      this.endSession();
      return;
    }
    const header = this.generateUdpHeader(payload.length, timestamp, ++this.udp.localSequence);
    const cipher = crypto.createCipheriv(this.udp.encryption, this.udp.key, header);
    this.server.sendUdpMessage(Buffer.concat([header, cipher.update(payload), cipher.final()]),
      this.udp.remoteAddress);
  }

  async parseHelloMessage(message) {
    const audio = message.audio_params;
    if (message.version !== 3 || !isObject(audio) || audio.format !== "opus" ||
        !Number.isSafeInteger(audio.sample_rate) || audio.sample_rate <= 0 ||
        ![1, 2].includes(audio.channels) || !Number.isFinite(audio.frame_duration) ||
        audio.frame_duration <= 0 || audio.frame_duration > 120 ||
        (message.features !== undefined && !isObject(message.features))) {
      throw new Error("Invalid device hello");
    }
    // Detach the old bridge synchronously; late close/hello events cannot mutate the new session.
    this.endSession(false);
    const udp = { key: crypto.randomBytes(16), nonce: this.generateUdpHeader(0, 0, 0),
      encryption: "aes-128-ctr", remoteSequence: -1, localSequence: 0,
      remoteAddress: null, startTime: Date.now() };
    const bridge = new WebSocketBridge(this, () => this.server.selectBackends(this.macAddress));
    this.udp = udp;
    this.bridge = bridge;
    this.server.bridges.add(bridge);
    bridge.on("close", () => {
      if (this.bridge === bridge) this.endSession();
    });
    try {
      const hello = await bridge.connect(audio, message.features);
      if (this.closing || this.bridge !== bridge || !bridge.isAlive()) return;
      udp.session_id = hello.session_id;
      this.sendMqttMessage(JSON.stringify({ type: "hello", version: message.version,
        session_id: udp.session_id, transport: "udp",
        udp: { server: this.server.settings.publicIp, port: this.server.settings.udpPort,
          encryption: udp.encryption, key: udp.key.toString("hex"), nonce: udp.nonce.toString("hex") },
        audio_params: hello.audio_params }));
    } catch {
      if (this.bridge !== bridge || this.closing) return;
      this.server.log(`Session establishment failed connection=${this.connectionId}`);
      this.sendMqttMessage(JSON.stringify({ type: "error", message: "Unable to establish backend session" }));
      this.endSession(false);
    }
  }

  parseOtherMessage(message) {
    if (message.type === "mcp" && message.payload.method === undefined) {
      const { id, error, result } = message.payload;
      const request = this.mcpPendingRequests.get(id);
      if (request) {
        this.mcpPendingRequests.delete(id);
        if (error) request.reject(new Error("Device MCP request failed"));
        else request.resolve(result);
        return;
      }
    }
    if (!this.bridge) {
      if (message.type !== "goodbye") {
        this.sendMqttMessage(JSON.stringify({ type: "goodbye", session_id: message.session_id }));
      }
      return;
    }
    if (message.type === "goodbye" || message.type === "udp_timeout") {
      this.endSession();
      return;
    }
    this.bridge.sendJson(message);
  }

  onUdpMessage(rinfo, message, payloadLength, timestamp, sequence) {
    if (!this.bridge?.isAlive() || !this.udp?.session_id ||
        sequence <= this.udp.remoteSequence) return;
    try {
      const decipher = crypto.createDecipheriv(this.udp.encryption, this.udp.key, message.subarray(0, 16));
      const opus = Buffer.concat([decipher.update(message.subarray(16, 16 + payloadLength)), decipher.final()]);
      this.udp.remoteAddress = { address: rinfo.address, port: rinfo.port };
      this.udp.remoteSequence = sequence;
      this.bridge.sendAudio(opus, timestamp);
    } catch {
      this.server.log(`UDP processing failed connection=${this.connectionId}; ending session`);
      this.endSession();
    }
  }

  endSession(notify = true) {
    const bridge = this.bridge;
    const udp = this.udp;
    this.bridge = null;
    this.udp = null;
    if (!bridge) return;
    if (notify && udp?.session_id) {
      this.sendMqttMessage(JSON.stringify({ type: "goodbye", session_id: udp.session_id }));
    }
    this.server.log(`Session ended connection=${this.connectionId} duration_ms=${Date.now() - udp.startTime}`);
    bridge.close().finally(() => this.server.bridges.delete(bridge));
  }

  checkKeepAlive() {
    if (!this.protocol.isConnected) {
      if (Date.now() - this.createdAt > 10000) this.close();
      return;
    }
    const interval = this.protocol.getKeepAliveInterval();
    if (interval && Date.now() - this.protocol.getLastActivity() > interval) this.close();
  }

  close() {
    if (this.closing) return;
    this.closing = true;
    for (const request of this.mcpPendingRequests.values()) request.reject(new Error("Connection closed"));
    this.mcpPendingRequests.clear();
    this.endSession(false);
    this.server.removeConnection(this);
    this.protocol.close();
  }

  async initializeDeviceTools() {
    try {
      const client = this.server.config.get("mcp_client") || {};
      const initialized = await this.sendMcpRequest("initialize", {
        protocolVersion: "2024-11-05", capabilities: client.capabilities || {},
        clientInfo: client.client_info || { name: "xiaozhi-mqtt-client", version: "1.0.0" },
      });
      if (!isObject(initialized)) throw new Error("Invalid MCP initialize response");
      this.mcpCachedInitialize = initialized;
      this.sendMqttMessage(JSON.stringify({ type: "mcp",
        payload: { jsonrpc: "2.0", method: "notifications/initialized" } }));
      const max = Number(this.server.config.get("mcp_client.max_tools_count") ?? 128);
      const tools = [];
      const cursors = new Set();
      let cursor;
      do {
        const result = await this.sendMcpRequest("tools/list", { cursor });
        if (!isObject(result) || !Array.isArray(result.tools) || result.tools.some((tool) => !isObject(tool))) {
          throw new Error("Invalid MCP tools/list response");
        }
        tools.push(...result.tools.slice(0, max - tools.length));
        if (!result.tools.length || tools.length >= max || result.nextCursor === undefined) break;
        if (typeof result.nextCursor !== "string" || cursors.has(result.nextCursor)) {
          throw new Error("Invalid MCP pagination cursor");
        }
        cursor = result.nextCursor;
        cursors.add(cursor);
      } while (!this.closing);
      if (this.closing) return;
      this.mcpCachedTools = tools;
      this.mcpCacheReady = true;
      this.server.log(`MCP cache ready connection=${this.connectionId} tools=${tools.length}`);
    } catch {
      // Without a complete cache, backend MCP messages continue through to the device.
      if (!this.closing) this.server.log(`MCP prefetch unavailable connection=${this.connectionId}; using direct forwarding`);
    }
  }

  sendMcpRequest(method, params, timeout = 10000) {
    if (this.closing || !this.protocol.isConnected) return Promise.reject(new Error("Connection unavailable"));
    const id = this.mcpRequestId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.mcpPendingRequests.delete(id);
        reject(new Error("MCP request timed out"));
      }, timeout);
      this.mcpPendingRequests.set(id, {
        resolve: (result) => { clearTimeout(timer); resolve(result); },
        reject: (error) => { clearTimeout(timer); reject(error); },
      });
      this.sendMqttMessage(JSON.stringify({ type: "mcp",
        payload: { jsonrpc: "2.0", method, id, params } }));
    });
  }

  onMcpMessageFromBridge(message) {
    const { method, id } = message.payload;
    if (method === "notifications/initialized") return;
    const result = method === "initialize" ? this.mcpCachedInitialize : { tools: this.mcpCachedTools };
    this.bridge?.sendJson({ type: "mcp", payload: { jsonrpc: "2.0", id, result } });
  }
}

module.exports = { MQTTConnection };
