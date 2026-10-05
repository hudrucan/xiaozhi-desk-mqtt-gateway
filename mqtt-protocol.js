const { EventEmitter } = require("node:events");
const { TextDecoder } = require("node:util");

const PacketType = { CONNECT: 1, CONNACK: 2, PUBLISH: 3, SUBSCRIBE: 8,
  SUBACK: 9, PINGREQ: 12, PINGRESP: 13, DISCONNECT: 14 };
const decoder = new TextDecoder("utf-8", { fatal: true });

// Distinguish an incomplete length field from an invalid four-byte encoding.
function remainingLength(buffer) {
  let value = 0;
  for (let index = 0; index < 4; index++) {
    if (index + 1 >= buffer.length) return null;
    const digit = buffer[index + 1];
    value += (digit & 127) * (128 ** index);
    if (!(digit & 128)) return { value, bytesRead: index + 1 };
  }
  throw new Error("Malformed MQTT remaining length");
}

function encodeLength(value) {
  const bytes = [];
  do {
    const digit = value % 128;
    value = Math.floor(value / 128);
    bytes.push(digit | (value ? 128 : 0));
  } while (value);
  return Buffer.from(bytes);
}

class Reader {
  constructor(buffer) { this.buffer = buffer; this.position = 0; }
  take(length) {
    if (this.position + length > this.buffer.length) throw new Error("Truncated MQTT packet");
    const value = this.buffer.subarray(this.position, this.position + length);
    this.position += length;
    return value;
  }
  byte() { return this.take(1)[0]; }
  uint16() { return this.take(2).readUInt16BE(0); }
  field() { return this.take(this.uint16()); }
  string() {
    const value = decoder.decode(this.field());
    if (value.includes("\0")) throw new Error("Invalid MQTT string");
    return value;
  }
  end() {
    if (this.position !== this.buffer.length) throw new Error("Unexpected MQTT packet data");
  }
}

class MQTTProtocol extends EventEmitter {
  constructor(socket, config) {
    super();
    this.socket = socket;
    this.config = config;
    this.buffer = Buffer.alloc(0);
    this.isConnected = false;
    this.closed = false;
    this.keepAliveInterval = 0;
    this.lastActivity = Date.now();
    socket.on("data", (data) => {
      if (this.closed) return;
      this.lastActivity = Date.now();
      try {
        // Bound partial-packet buffering while accepting coalesced TCP packets.
        const limit = Number(this.config.get("max_mqtt_payload_size") ?? 8192);
        for (let offset = 0; offset < data.length && !this.closed;) {
          const space = limit - this.buffer.length;
          if (space <= 0) throw new Error("MQTT packet exceeds configured limit");
          const size = Math.min(space, data.length - offset);
          this.buffer = Buffer.concat([this.buffer, data.subarray(offset, offset + size)]);
          offset += size;
          this.processBuffer(limit);
        }
      } catch (error) {
        this.emit("protocolError", error);
        this.close();
      }
    });
    socket.on("error", (error) => this.emit("error", error));
    socket.on("close", () => {
      this.closed = true;
      this.isConnected = false;
      this.emit("close");
    });
  }

  processBuffer(limit) {
    while (this.buffer.length >= 2 && !this.closed) {
      const length = remainingLength(this.buffer);
      if (!length) return;
      const size = 1 + length.bytesRead + length.value;
      if (size > limit) throw new Error("MQTT packet exceeds configured limit");
      if (this.buffer.length < size) return;
      const first = this.buffer[0];
      const body = this.buffer.subarray(1 + length.bytesRead, size);
      this.buffer = this.buffer.subarray(size);
      this.parsePacket(first, body);
    }
  }

  parsePacket(first, body) {
    const type = first >> 4;
    const flags = first & 15;
    if (!this.isConnected && type !== PacketType.CONNECT) {
      throw new Error("MQTT CONNECT required");
    }
    const reader = new Reader(body);
    switch (type) {
      case PacketType.CONNECT: {
        if (flags !== 0 || this.isConnected) throw new Error("Invalid MQTT CONNECT");
        const protocol = reader.string();
        const protocolLevel = reader.byte();
        if (!((protocolLevel === 4 && protocol === "MQTT") ||
            (protocolLevel === 3 && protocol === "MQIsdp"))) {
          this.rejectConnect(1);
          return;
        }
        const connectFlags = reader.byte();
        const will = !!(connectFlags & 4);
        const willQos = (connectFlags >> 3) & 3;
        if ((connectFlags & 1) || willQos === 3 ||
            (!will && (connectFlags & 56)) ||
            ((connectFlags & 64) && !(connectFlags & 128))) {
          throw new Error("Invalid MQTT CONNECT flags");
        }
        const keepAlive = reader.uint16();
        const clientId = reader.string();
        if (will) { reader.string(); reader.field(); }
        const username = connectFlags & 128 ? reader.string() : "";
        const password = connectFlags & 64 ? decoder.decode(reader.field()) : "";
        reader.end();
        this.keepAliveInterval = keepAlive * 1500;
        // The connection authenticates synchronously before sending CONNACK.
        this.emit("connect", { clientId, username, password, protocol, protocolLevel, keepAlive });
        break;
      }
      case PacketType.PUBLISH: {
        const qos = (flags >> 1) & 3;
        if (qos !== 0) throw new Error("Only MQTT QoS 0 is supported");
        const topic = reader.string();
        if (!topic || /[+#]/.test(topic)) throw new Error("Invalid MQTT publish topic");
        const payload = decoder.decode(reader.take(body.length - reader.position));
        this.emit("publish", { topic, payload, qos });
        break;
      }
      case PacketType.SUBSCRIBE: {
        if (flags !== 2) throw new Error("Invalid MQTT SUBSCRIBE flags");
        const packetId = reader.uint16();
        if (!packetId) throw new Error("Invalid MQTT packet ID");
        const codes = [];
        while (reader.position < body.length) {
          if (!reader.string()) throw new Error("Empty MQTT subscription");
          const qos = reader.byte();
          if (qos > 2) throw new Error("Invalid MQTT subscription QoS");
          codes.push(0);
        }
        if (!codes.length) throw new Error("Empty MQTT SUBSCRIBE packet");
        this.sendPacket(PacketType.SUBACK, Buffer.from([packetId >> 8, packetId & 255, ...codes]));
        break;
      }
      case PacketType.PINGREQ:
        if (flags || body.length) throw new Error("Invalid MQTT PINGREQ");
        this.sendPacket(PacketType.PINGRESP, Buffer.alloc(0));
        break;
      case PacketType.DISCONNECT:
        if (flags || body.length) throw new Error("Invalid MQTT DISCONNECT");
        this.emit("disconnect");
        this.close();
        break;
      default:
        throw new Error("Unsupported MQTT packet type");
    }
  }

  acceptConnect() {
    this.isConnected = true;
    this.sendPacket(PacketType.CONNACK, Buffer.from([0, 0]));
  }

  rejectConnect(code = 5) {
    this.closed = true;
    const packet = Buffer.from([PacketType.CONNACK << 4, 2, 0, code]);
    this.socket.end(packet);
  }

  sendPacket(type, body) {
    if (this.closed || !this.socket.writable) return;
    if (this.socket.writableLength > 1048576) throw new Error("MQTT output buffer limit exceeded");
    this.socket.write(Buffer.concat([Buffer.from([type << 4]), encodeLength(body.length), body]));
  }

  sendPublish(topic, payload) {
    if (!this.isConnected || this.closed) return;
    const topicBytes = Buffer.from(topic);
    const length = Buffer.alloc(2);
    length.writeUInt16BE(topicBytes.length);
    this.sendPacket(PacketType.PUBLISH, Buffer.concat([length, topicBytes, Buffer.from(payload)]));
  }

  getLastActivity() { return this.lastActivity; }
  getKeepAliveInterval() { return this.keepAliveInterval; }
  close() {
    this.closed = true;
    this.isConnected = false;
    this.buffer = Buffer.alloc(0);
    this.socket.destroy();
  }
}

module.exports = { PacketType, MQTTProtocol };
