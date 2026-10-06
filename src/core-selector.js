const http = require("node:http");
const https = require("node:https");
const crypto = require("node:crypto");
const { isObject } = require("./config");

const PROTOCOL = "xiaozhi-core-transport-v1";
const MAX_STATUS_BYTES = 8192;

function validateCoreNodes(nodes) {
  if (!Array.isArray(nodes) || nodes.length !== 3) throw new Error("core_nodes requires three nodes");
  const ids = new Set();
  const urls = new Set();
  const statuses = new Set();
  for (const node of nodes) {
    if (!isObject(node) || !/^[A-Za-z0-9_-]{1,128}$/.test(node.node_id || "")) {
      throw new Error("Invalid core identity");
    }
    for (const [key, schemes] of [["ws_url", ["ws:", "wss:"]], ["status_url", ["http:", "https:"]]]) {
      let url;
      try { url = new URL(node[key]); } catch { throw new Error("Invalid core URL"); }
      if (typeof node[key] !== "string" || !schemes.includes(url.protocol) ||
          url.username || url.password || url.hash || node[key].includes("?") ||
          (key === "status_url" && url.pathname !== "/status")) throw new Error("Invalid core URL");
    }
    if (new URL(node.ws_url).hostname !== new URL(node.status_url).hostname ||
        ids.has(node.node_id) || urls.has(node.ws_url) || statuses.has(node.status_url)) {
      throw new Error("Core nodes must have distinct identities and endpoints");
    }
    ids.add(node.node_id); urls.add(node.ws_url); statuses.add(node.status_url);
  }
}

function readStatus(node) {
  // No redirects, proxy environment, credentials or response bodies in diagnostics.
  return new Promise((resolve) => {
    const url = new URL(node.status_url);
    let settled = false;
    let request;
    let response;
    let timer;
    const finish = (value) => {
      if (settled) return;
      settled = true; clearTimeout(timer);
      response?.destroy(); request?.destroy(); resolve(value);
    };
    const client = url.protocol === "https:" ? https : http;
    request = client.get(url, { agent: false }, (incoming) => {
      response = incoming;
      if (incoming.statusCode !== 200) { finish(null); return; }
      const chunks = [];
      let bytes = 0;
      incoming.on("data", (chunk) => {
        bytes += chunk.length;
        if (bytes > MAX_STATUS_BYTES) finish(null);
        else chunks.push(chunk);
      });
      incoming.on("end", () => {
        try {
          const status = JSON.parse(Buffer.concat(chunks).toString("utf8"));
          if (!isObject(status) || status.protocol !== PROTOCOL ||
              status.core_id !== node.node_id || status.status !== "ready" ||
              typeof status.local_vip_owner !== "boolean") { finish(null); return; }
          finish({ node, vip: status.local_vip_owner });
        } catch { finish(null); }
      });
      incoming.on("error", () => finish(null));
      incoming.on("aborted", () => finish(null));
    });
    request.on("error", () => finish(null));
    timer = setTimeout(() => finish(null), 500);
  });
}

class CoreSelector {
  constructor(gatewayId, probe = readStatus) {
    this.gatewayId = gatewayId;
    this.probe = probe;
    // Rotate equal candidates without always picking the lowest node at startup.
    this.cursor = crypto.randomInt(0, 2147483647);
  }

  async select(nodes, allowedUrls) {
    if (!nodes.some((node) => node.node_id === this.gatewayId)) throw new Error("Missing local core identity");
    const statuses = (await Promise.all(nodes.map((node) => this.probe(node)))).filter(Boolean);
    if (statuses.filter((entry) => entry.vip).length > 1) {
      throw new Error("Conflicting VIP ownership; refusing new core selection");
    }
    const candidates = statuses.filter((entry) => allowedUrls.includes(entry.node.ws_url));
    // Different gateway and VIP first, then a remote VIP, then local fallback.
    const rank = ({ node, vip }) => node.node_id === this.gatewayId ? (vip ? 3 : 2) : (vip ? 1 : 0);
    const ordered = [];
    const cursor = this.cursor;
    this.cursor = (cursor + 1) % 2147483647;
    for (let tier = 0; tier < 4; tier++) {
      const peers = candidates.filter((entry) => rank(entry) === tier);
      if (!peers.length) continue;
      const start = cursor % peers.length;
      ordered.push(...peers.slice(start), ...peers.slice(0, start));
    }
    if (!ordered.length) throw new Error("No validated core is available");
    return ordered.map((entry) => entry.node.ws_url);
  }
}

module.exports = { CoreSelector, readStatus, validateCoreNodes };
