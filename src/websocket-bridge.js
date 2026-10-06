const { EventEmitter } = require("node:events");
const WebSocket = require("ws");
const { isObject } = require("./config");
const { backendAuthorization } = require("./mqtt-auth");

function validateMessage(message) {
  if (!isObject(message) || typeof message.type !== "string" ||
      !message.type || message.type.length > 64) throw new Error("Invalid transport message");
  if (message.type === "mcp") {
    const payload = message.payload;
    if (!isObject(payload) || payload.jsonrpc !== "2.0" ||
        (payload.method !== undefined && typeof payload.method !== "string") ||
        (payload.id !== undefined && typeof payload.id !== "string" &&
          !Number.isSafeInteger(payload.id)) ||
        (payload.error !== undefined && !isObject(payload.error))) {
      throw new Error("Invalid MCP payload");
    }
    if (payload.method === undefined && !Object.hasOwn(payload, "result") &&
        !Object.hasOwn(payload, "error")) throw new Error("Invalid MCP response");
  }
  return message;
}

class WebSocketBridge extends EventEmitter {
  constructor(connection, backends) {
    super();
    this.connection = connection;
    this.backends = backends;
    this.coreNodes = connection.server.config.get("core_nodes");
    this.wsClient = null;
    this.coreId = null;
    this.established = false;
    this.closed = false;
    this.cancelAttempt = null;
    this.closePromise = Promise.resolve();
    this.forceCloseTimer = null;
  }

  async connect(audioParams, features) {
    if (typeof this.backends === "function") this.backends = await this.backends();
    for (let index = 0; index < this.backends.length; index++) {
      if (this.closed) throw new Error("Session cancelled");
      try {
        const hello = await this.attempt(this.backends[index], audioParams, features);
        this.connection.server.log(`Session established connection=${this.connection.connectionId} backend_attempt=${index + 1}`);
        return hello;
      } catch {
        if (this.closed) throw new Error("Session cancelled");
        this.connection.server.log(`Backend establishment failed connection=${this.connection.connectionId} attempt=${index + 1}/${this.backends.length}`);
      }
    }
    throw new Error("All configured backends failed to establish a session");
  }

  attempt(endpoint, audioParams, features) {
    const connection = this.connection;
    const url = new URL(endpoint);
    url.search = "?from=mqtt_gateway";
    const headers = {
      "device-id": connection.macAddress,
      "protocol-version": "2",
      authorization: backendAuthorization(connection.uuid, connection.macAddress,
        connection.server.settings.serverSecret),
    };
    if (connection.uuid) headers["client-id"] = connection.uuid;
    if (connection.userData.ip) headers["x-forwarded-for"] = connection.userData.ip;
    const timeout = Number(connection.server.config.get("backend_connect_timeout_ms") ?? 2500);
    const ws = new WebSocket(url, { headers, handshakeTimeout: timeout,
      maxPayload: Math.max(65507, Number(connection.server.config.get("max_mqtt_payload_size") ?? 8192)),
      perMessageDeflate: false });
    this.wsClient = ws;
    this.closePromise = new Promise((resolve) => ws.once("close", resolve));
    return new Promise((resolve, reject) => {
      let settled = false;
      let established = false;
      let timer;
      const fail = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.cancelAttempt = null;
        ws.terminate();
        reject(new Error("Backend session establishment failed"));
      };
      this.cancelAttempt = fail;
      timer = setTimeout(fail, timeout);
      ws.on("open", () => {
        if (this.closed) return fail();
        ws.send(JSON.stringify({ type: "hello", version: 2, transport: "websocket",
          audio_params: audioParams, features }));
      });
      ws.on("message", (data, isBinary) => {
        if (this.closed || (settled && !established)) return;
        try {
          if (isBinary) {
            if (!established || data.length < 16) throw new Error("Invalid backend audio header");
            const length = data.readUInt32BE(12);
            if (!length || length > 65491 || data.length !== 16 + length) {
              throw new Error("Invalid backend audio length");
            }
            connection.sendUdpMessage(data.subarray(16), data.readUInt32BE(8));
            return;
          }
          if (data.length > Number(connection.server.config.get("max_mqtt_payload_size") ?? 8192)) {
            throw new Error("Backend JSON exceeds configured limit");
          }
          const message = validateMessage(JSON.parse(data.toString("utf8")));
          if (!established) {
            if (message.type !== "hello" || typeof message.session_id !== "string" ||
                !message.session_id || !isObject(message.audio_params)) {
              throw new Error("Backend hello required");
            }
            const node = this.coreNodes?.find((entry) => entry.ws_url === endpoint);
            if (node && message.core_id !== node.node_id) throw new Error("Core hello identity mismatch");
            this.coreId = node?.node_id || null;
            settled = true;
            established = true;
            this.established = true;
            clearTimeout(timer);
            this.cancelAttempt = null;
            resolve(message);
          } else if (message.type === "hello") {
            throw new Error("Unexpected backend hello");
          } else if (message.type === "mcp" && connection.mcpCacheReady &&
              ["initialize", "notifications/initialized", "tools/list"].includes(message.payload.method)) {
            connection.onMcpMessageFromBridge(message);
          } else {
            connection.sendMqttMessage(JSON.stringify(message));
          }
        } catch {
          if (!established) fail();
          else {
            connection.server.log(`Invalid backend message connection=${connection.connectionId}; ending session`);
            ws.terminate();
          }
        }
      });
      ws.on("error", () => {
        // ws may emit error before close; retain this handler after cancellation.
        if (!established) fail();
        else ws.terminate();
      });
      ws.on("close", () => {
        clearTimeout(timer);
        if (this.wsClient === ws) {
          clearTimeout(this.forceCloseTimer);
          this.wsClient = null;
        }
        if (!established) fail();
        else {
          this.established = false;
          this.closed = true;
          this.emit("close");
        }
      });
    });
  }

  sendJson(message) {
    if (this.isAlive()) {
      if (this.wsClient.bufferedAmount > 1048576) throw new Error("Backend output buffer limit exceeded");
      this.wsClient.send(JSON.stringify(message));
    }
  }

  sendAudio(opus, timestamp) {
    if (!this.isAlive()) return;
    if (this.wsClient.bufferedAmount > 1048576) throw new Error("Backend output buffer limit exceeded");
    const buffer = Buffer.alloc(16 + opus.length);
    buffer.writeUInt32BE(timestamp, 8);
    buffer.writeUInt32BE(opus.length, 12);
    opus.copy(buffer, 16);
    this.wsClient.send(buffer, { binary: true });
  }

  isAlive() {
    return !this.closed && this.established && this.wsClient?.readyState === WebSocket.OPEN;
  }

  close() {
    if (this.closed) return this.closePromise;
    this.closed = true;
    if (this.cancelAttempt) this.cancelAttempt();
    else if (this.wsClient && this.wsClient.readyState !== WebSocket.CLOSED) {
      const ws = this.wsClient;
      ws.close();
      this.forceCloseTimer = setTimeout(() => ws.terminate(), 1500);
    }
    return this.closePromise;
  }
}

module.exports = { WebSocketBridge, validateMessage };
