// Mock HTTP streams exercise bounded core probes without network access.
const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const node = { node_id: "node3", status_url: "http://192.0.2.3:8000/status" };
const valid = { protocol: "xiaozhi-core-transport-v1", core_id: "node3", status: "ready", local_vip_owner: false };
async function fixture(payload, code = 200, timeout = false) {
  let requested = false;
  const request = new EventEmitter();
  request.destroy = () => {};
  const response = new EventEmitter();
  response.statusCode = code;
  response.destroy = () => {};
  const client = { get(url, options, callback) {
    requested = true;
    assert.equal(url.href, node.status_url);
    assert.equal(options.agent, false);
    if (!timeout) queueMicrotask(() => {
      callback(response);
      response.emit("data", Buffer.from(payload));
      response.emit("end");
    });
    return request;
  }};
  const sandbox = { module: { exports: {} }, Buffer, URL, setTimeout, clearTimeout,
    require(name) {
      if (name === "node:http" || name === "node:https") return client;
      if (name === "node:crypto") return require(name);
      if (name === "./config") return { isObject: (x) => x !== null && typeof x === "object" && !Array.isArray(x) };
      throw new Error("Unexpected dependency");
    },
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "../src/core-selector.js"), "utf8"), sandbox);
  const result = await sandbox.module.exports.readStatus(node);
  assert.equal(requested, true);
  return result;
}
async function main() {
  assert.equal((await fixture(JSON.stringify(valid))).vip, false);
  for (const changes of [{ core_id: "node2" }, { protocol: "unknown" },
      { status: "stopping" }, { local_vip_owner: null }, { local_vip_owner: "false" }]) {
    assert.equal(await fixture(JSON.stringify({ ...valid, ...changes })), null);
  }
  for (const data of ["{", "[]", "x".repeat(8193)]) assert.equal(await fixture(data), null);
  assert.equal(await fixture(JSON.stringify(valid), 302), null);
  assert.equal(await fixture("", 200, true), null);
  console.log("PASS: core protocol/identity/readiness/VIP validation, malformed/oversize/redirect/timeout; no sockets");
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
