// Offline fixtures: no configuration files, credentials, timers or sockets.
const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function load(env) {
  const sandbox = {
    module: { exports: {} }, process: { env }, console,
    require(name) {
      if (name.startsWith("node:")) return require(name);
      if (name === "./config") return {
        ConfigManager: class extends EventEmitter { get() { return undefined; } },
        integer: (value, _name, fallback) => value === undefined ? fallback : Number(value),
      };
      if (name === "./core-selector") return { CoreSelector: class {} };
      if (name === "./mqtt-connection") return { MQTTConnection: class {} };
      if (name === "./health-server") return { createHealthServer() { throw new Error("Unexpected server startup"); } };
      throw new Error("Unexpected dependency");
    },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../src/gateway.js"), "utf8"), sandbox);
  const gateway = new sandbox.module.exports.Gateway();
  gateway.log = () => {};
  return gateway;
}

async function main() {
  const env = { MQTT_SIGNATURE_KEY: "fixture-only", SERVER_SECRET: "fixture-only" };
  for (const hosts of [{}, { MQTT_HOST: "10.10.10.11", UDP_HOST: "192.168.1.101", HTTP_HOST: "10.10.10.11" }]) {
    const gateway = load({ ...env, ...hosts });
    for (const [kind, port] of [["mqtt", 1883], ["udp", 1883], ["http", 8007]]) {
      const listener = new EventEmitter();
      let call;
      const capture = (...args) => { call = args; listener.emit("listening"); };
      listener.bind = capture;
      listener.listen = capture;
      await gateway.listen(listener, kind, port);
      assert.deepEqual(call, [port, hosts[`${kind.toUpperCase()}_HOST`]]);
      assert.equal(gateway.listeners[kind], true);
      listener.emit("close");
      assert.equal(gateway.listeners[kind], false);
    }
  }
  const gateway = load(env);
  gateway.config.get = (key) => key === "production.chat_servers" ? ["ws://fixture.example/x/"] : undefined;
  gateway.connections.set(1, { protocol: { isConnected: true } });
  gateway.bridges.add({ isAlive: () => false });
  assert.equal(gateway.status().active_mqtt_connections, 1);
  assert.equal(gateway.status().active_websocket_sessions, 0);
  const active = { isAlive: () => true, coreId: "node3" };
  gateway.bridges.add(active);
  assert.equal(gateway.status().active_websocket_sessions, 1);
  assert.equal(gateway.status().active_core_sessions.node3, 1);
  gateway.bridges.delete(active);
  assert.equal(gateway.status().active_websocket_sessions, 0);
  assert.equal(Object.keys(gateway.status().active_core_sessions).length, 0);
  assert.equal(JSON.stringify(gateway.status().listener_ports), JSON.stringify({ mqtt: 1883, udp: 1883, http: 8007 }));
  assert.equal(JSON.stringify(gateway.status()).includes("fixture-only"), false);
  for (const key of ["MQTT_HOST", "UDP_HOST", "HTTP_HOST"]) {
    for (const value of ["", "localhost", "::1", "127.0.0.1\n", "not-an-IP"]) {
      assert.throws(() => load({ ...env, [key]: value }), new RegExp(`${key} must be an IPv4 bind address`));
    }
  }
  console.log("PASS: 6 listener/default binding fixtures; 15 invalid-host fixtures; session/port status; no sockets opened");
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
