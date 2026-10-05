const fs = require("node:fs");
const path = require("node:path");
const { EventEmitter } = require("node:events");

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function integer(value, name, fallback, max = 65535) {
  const number = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(number) || number < 1 || number > max) {
    throw new Error(`${name} must be an integer between 1 and ${max}`);
  }
  return number;
}

function validateConfig(config) {
  if (!isObject(config)) throw new Error("MQTT configuration must be an object");
  for (const mode of ["production", "development"]) {
    const section = config[mode];
    if (mode === "development" && section === undefined) continue;
    if (!isObject(section) || !Array.isArray(section.chat_servers) ||
        section.chat_servers.length === 0) {
      throw new Error(`${mode}.chat_servers must contain at least one backend`);
    }
    for (const endpoint of section.chat_servers) {
      let url;
      try { url = new URL(endpoint); } catch {
        throw new Error(`${mode}.chat_servers contains an invalid URL`);
      }
      // The server detects the exact suffix ?from=mqtt_gateway. The gateway owns it.
      if (typeof endpoint === "string" && endpoint.includes("?")) {
        throw new Error(`${mode}.chat_servers must not contain query parameters`);
      }
      if (typeof endpoint !== "string" || !["ws:", "wss:"].includes(url.protocol) ||
          url.username || url.password || url.hash) {
        throw new Error(`${mode}.chat_servers requires ws/wss URLs without credentials`);
      }
    }
  }
  const macs = config.development?.mac_addresss;
  if (macs !== undefined && (!Array.isArray(macs) ||
      macs.some((mac) => typeof mac !== "string" ||
        !/^[0-9a-f]{2}(:[0-9a-f]{2}){5}$/i.test(mac)))) {
    throw new Error("development.mac_addresss must be an array of MAC addresses");
  }
  if (config.mcp_client !== undefined && !isObject(config.mcp_client)) {
    throw new Error("mcp_client must be an object");
  }
  for (const key of ["capabilities", "client_info"]) {
    const value = config.mcp_client?.[key];
    if (value !== undefined && !isObject(value)) {
      throw new Error(`mcp_client.${key} must be an object`);
    }
  }
  integer(config.max_mqtt_payload_size, "max_mqtt_payload_size", 8192, 1048576);
  integer(config.mcp_client?.max_tools_count, "mcp_client.max_tools_count", 128, 10000);
  integer(config.backend_connect_timeout_ms, "backend_connect_timeout_ms", 2500, 60000);
  return config;
}

class ConfigManager extends EventEmitter {
  constructor(filePath = path.join(__dirname, "..", "config", "mqtt.json")) {
    super();
    this.configPath = filePath;
    try {
      this.config = validateConfig(JSON.parse(fs.readFileSync(filePath, "utf8")));
    } catch (error) {
      if (error.code === "ENOENT") throw new Error("config/mqtt.json is missing; copy config/mqtt.json.example first");
      // A JSON parser error can contain input fragments, so do not echo it.
      throw new Error("config/mqtt.json is invalid; check its JSON and configuration values");
    }
    this.closed = false;
    this.watchDebounceTimer = null;
    this.watcher = null;
  }

  watch() {
    this.watcher = fs.watch(path.dirname(this.configPath), (_, filename) => {
      if (filename && filename.toString() !== path.basename(this.configPath)) return;
      clearTimeout(this.watchDebounceTimer);
      this.watchDebounceTimer = setTimeout(() => this.reload(), 300);
    });
    this.watcher.on("error", () => this.emit("configError"));
  }

  reload() {
    if (this.closed) return;
    try {
      const next = validateConfig(JSON.parse(fs.readFileSync(this.configPath, "utf8")));
      this.config = next;
      this.emit("configChanged");
    } catch {
      // Keep the last valid configuration after a broken or partial write.
      this.emit("configError");
    }
  }

  get(key) {
    return key.split(".").reduce((value, part) =>
      isObject(value) && Object.hasOwn(value, part) ? value[part] : undefined,
    this.config);
  }

  close() {
    this.closed = true;
    clearTimeout(this.watchDebounceTimer);
    this.watcher?.close();
    this.watcher = null;
  }
}

module.exports = { ConfigManager, integer, isObject };
