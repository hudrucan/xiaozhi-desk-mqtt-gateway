const crypto = require("node:crypto");
const net = require("node:net");
const { isObject } = require("./config");

function validateMqttCredentials(clientId, username, password, realClientIp, settings) {
  if (typeof clientId !== "string" || !clientId || clientId.length > 512 ||
      typeof username !== "string" || typeof password !== "string") {
    throw new Error("Invalid MQTT credentials");
  }
  const parts = clientId.split("@@@");
  if (![2, 3].includes(parts.length) || !parts[0] ||
      (parts.length === 3 && !parts[2]) || parts.some((part) => /[\r\n\0]/.test(part))) {
    throw new Error("Invalid MQTT client ID");
  }
  const macAddress = parts[1].replace(/_/g, ":").toLowerCase();
  if (!/^[0-9a-f]{2}(:[0-9a-f]{2}){5}$/.test(macAddress)) {
    throw new Error("Invalid device MAC address");
  }
  // Verify both modern and legacy IDs; the old two-part ID must not bypass auth.
  if (settings.mqttSignatureKey) {
    const expected = crypto.createHmac("sha256", settings.mqttSignatureKey)
      .update(`${clientId}|${username}`).digest("base64");
    const supplied = Buffer.from(password);
    const signature = Buffer.from(expected);
    if (supplied.length !== signature.length || !crypto.timingSafeEqual(supplied, signature)) {
      throw new Error("Invalid MQTT signature");
    }
  } else if (!settings.allowInsecureMqtt) {
    throw new Error("MQTT signature verification is unavailable");
  }
  let userData = {};
  if (parts.length === 3) {
    if (!username || username.length > 8192 ||
        !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(username)) {
      throw new Error("Invalid MQTT username encoding");
    }
    try { userData = JSON.parse(Buffer.from(username, "base64").toString("utf8")); }
    catch { throw new Error("Invalid MQTT username JSON"); }
    if (!isObject(userData)) throw new Error("MQTT username JSON must be an object");
  }
  const ip = realClientIp?.replace(/^::ffff:/, "");
  userData.ip = ip && net.isIP(ip) ? ip : undefined;
  return { groupId: parts[0], macAddress, uuid: parts[2], userData,
    replyTo: `devices/p2p/${parts[1]}` };
}

function backendAuthorization(uuid, macAddress, secret) {
  const timestamp = Math.floor(Date.now() / 1000);
  const signature = crypto.createHmac("sha256", secret)
    .update(`${uuid || "default-client-id"}|${macAddress}|${timestamp}`)
    .digest("base64url");
  return `Bearer ${signature}.${timestamp}`;
}

module.exports = { validateMqttCredentials, backendAuthorization };
