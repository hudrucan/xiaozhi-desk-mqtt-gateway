// Offline cache pagination matches the firmware name-cursor contract.
const assert = require("node:assert/strict");
const { MQTTConnection } = require("../src/mqtt-connection");
const cached = Array.from({length: 30}, (_, i) => ({name: `self.tool.${i}`, description: "é".repeat(300),
  inputSchema: {type: "object", properties: {}}}));
const sent = [];
const connection = {mcpCachedTools: cached, mcpCachedInitialize: {protocolVersion:"2024-11-05"},
  server: {config:{get: () => 8192}}, bridge:{sendJson: (v) => sent.push(v)}};
const request = (cursor) => MQTTConnection.prototype.onMcpMessageFromBridge.call(connection,
  {payload: {method:"tools/list", id:123, params:cursor === undefined ? {} : {cursor}}});
let cursor, collected = [];
do {
  request(cursor);
  const value = sent.at(-1);
  assert.ok(Buffer.byteLength(JSON.stringify(value)) <= 8192);
  assert.equal(value.payload.id, 123);
  assert.ok(value.payload.result.tools.length);
  collected.push(...value.payload.result.tools);
  cursor = value.payload.result.nextCursor;
} while(cursor);
assert.deepEqual(collected, cached);
assert.ok(sent.length > 1);
request("not-advertised");
assert.equal(sent.at(-1).payload.error.code, -32602);
connection.mcpCachedTools = [{name:"oversize", description:"x".repeat(8192)}];
request();
assert.equal(sent.at(-1).payload.error.code, -32603);
connection.mcpCachedTools = [];
request();
assert.deepEqual(sent.at(-1).payload.result, {tools:[]});
console.log("PASS: bounded Unicode pages, cursor correlation, empty and oversized inventories; no sockets");
