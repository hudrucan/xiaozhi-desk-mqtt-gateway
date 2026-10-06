// Offline placement fixtures; all status I/O is injected, no sockets opened.
const assert = require("node:assert/strict");
const { CoreSelector, validateCoreNodes } = require("../src/core-selector");
const { validateConfig } = require("../src/config");
const nodes = [1, 2, 3].map((i) => ({ node_id: `node${i}`,
  ws_url: `ws://192.0.2.${i}:8000/xiaozhi/v1/`, status_url: `http://192.0.2.${i}:8000/status` }));
const urls = nodes.map((n) => n.ws_url);
async function main() {
  validateCoreNodes(nodes);
  validateConfig({ production: { chat_servers: urls }, core_nodes: nodes });
  for (let vip = 1; vip <= 3; vip++) {
    for (let gateway = 1; gateway <= 3; gateway++) {
      const select = new CoreSelector(`node${gateway}`, async (node) => ({ node, vip: node.node_id === `node${vip}` }));
      const result = await select.select(nodes, urls);
      const chosen = nodes.find((n) => n.ws_url === result[0]);
      assert.notEqual(chosen.node_id, `node${gateway}`);
      assert.notEqual(chosen.node_id, `node${vip}`);
      assert.equal(result.length, 3);
    }
  }
  const equal = new CoreSelector("node1", async (node) => ({ node, vip: node.node_id === "node1" }));
  equal.cursor = 0;
  assert.equal((await equal.select(nodes, urls))[0], urls[1]);
  assert.equal((await equal.select(nodes, urls))[0], urls[2]);
  const oneRemote = new CoreSelector("node2", async (node) => node.node_id === "node3" ? null : { node, vip: node.node_id === "node1" });
  assert.deepEqual(await oneRemote.select(nodes, urls), [urls[0], urls[1]]);
  const alone = new CoreSelector("node2", async (node) => node.node_id === "node2" ? { node, vip: true } : null);
  assert.deepEqual(await alone.select(nodes, urls), [urls[1]]);
  const absent = new CoreSelector("node1", async () => null);
  await assert.rejects(absent.select(nodes, urls), /No validated core/);
  const split = new CoreSelector("node1", async (node) => ({ node, vip: true }));
  await assert.rejects(split.select(nodes, urls), /Conflicting VIP/);
  const missing = new CoreSelector("unknown", async () => null);
  await assert.rejects(missing.select(nodes, urls), /Missing local/);
  for (const invalid of [[], nodes.slice(0, 2), [nodes[0], nodes[0], nodes[2]],
      [nodes[0], nodes[1], { ...nodes[2], status_url: "http://user:password@192.0.2.3/status" }],
      [nodes[0], nodes[1], { ...nodes[2], status_url: "http://192.0.2.3/status?q=1" }]]) {
    assert.throws(() => validateCoreNodes(invalid));
  }
  assert.throws(() => validateConfig({ production: { chat_servers: ["ws://other.example/"] }, core_nodes: nodes }));
  console.log("PASS: 9 gateway/VIP placements; rotation; remote/local fallback; unavailable/split VIP; strict config");
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
