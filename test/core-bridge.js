// Core identity/snapshot and fallback checks; mocked WebSockets only.
const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const endpoint = "ws://192.0.2.3:8000/xiaozhi/v1/";
async function fixture(coreId, changeConfig = false, fallback = false) {
  const attempts = [];
  class FakeWebSocket extends EventEmitter {
    static OPEN = 1; static CLOSED = 3;
    constructor(url, options) {
      super(); this.readyState = 1; this.bufferedAmount = 0;
      attempts.push(url.href);
      assert.equal(url.search, "?from=mqtt_gateway");
      assert.equal(options.perMessageDeflate, false);
      queueMicrotask(() => this.emit("open"));
    }
    send(data) {
      const hello = JSON.parse(data);
      assert.equal(hello.type, "hello");
      const id = fallback ? (attempts.length === 1 ? "wrong" : "node1") : coreId;
      queueMicrotask(() => this.emit("message", Buffer.from(JSON.stringify({ type: "hello",
        session_id: "fixture-session", core_id: id, audio_params: { format: "opus" } })), false));
    }
    terminate() { this.readyState = 3; queueMicrotask(() => this.emit("close")); }
    close() { this.terminate(); }
  }
  const sandbox = { module: { exports: {} }, URL, Buffer, setTimeout, clearTimeout,
    require(name) {
      if (name === "node:events") return { EventEmitter };
      if (name === "ws") return FakeWebSocket;
      if (name === "./config") return { isObject: (v) => v !== null && typeof v === "object" && !Array.isArray(v) };
      if (name === "./mqtt-auth") return { backendAuthorization: () => "Bearer offline-fixture" };
      throw new Error("Unexpected dependency");
    },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../src/websocket-bridge.js"), "utf8"), sandbox);
  let nodes = [{ node_id: "node3", ws_url: endpoint }];
  const second = "ws://192.0.2.1:8000/xiaozhi/v1/";
  if (fallback) nodes.push({ node_id: "node1", ws_url: second });
  const connection = { macAddress: "00:11:22:33:44:55", uuid: "fixture-client", userData: {},
    connectionId: 1, server: { settings: { serverSecret: "fixture-only" }, log() {},
      config: { get: (key) => key === "core_nodes" ? nodes : undefined } } };
  const bridge = new sandbox.module.exports.WebSocketBridge(connection, async () => fallback ? [endpoint, second] : [endpoint]);
  if (changeConfig) nodes = [{ node_id: "changed", ws_url: endpoint }];
  try {
    await bridge.connect({ format: "opus", sample_rate: 16000, channels: 1, frame_duration: 60 }, {});
    assert.equal(bridge.coreId, fallback ? "node1" : "node3");
    assert.equal(bridge.isAlive(), true);
    await bridge.close();
    assert.equal(bridge.isAlive(), false);
    return attempts.length;
  } catch (error) {
    assert.equal(bridge.isAlive(), false);
    throw error;
  }
}
async function main() {
  assert.equal(await fixture("node3"), 1);
  assert.equal(await fixture("node3", true), 1);
  assert.equal(await fixture("node3", false, true), 2);
  await assert.rejects(fixture("wrong"), /All configured backends/);
  console.log("PASS: core hello identity, frozen config, handshake fallback and cleanup; no sockets");
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
