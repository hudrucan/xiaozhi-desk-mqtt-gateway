require("dotenv").config();
const { Gateway } = require("./src/gateway");

async function main() {
  let gateway;
  const shutdown = (signal) => {
    if (!gateway) return;
    gateway.log(`Received ${signal}; shutting down`);
    gateway.stop().catch(() => {
      console.error("Gateway shutdown failed");
      process.exitCode = 1;
    });
  };
  try {
    gateway = new Gateway();
    process.on("SIGTERM", () => shutdown("SIGTERM"));
    process.on("SIGINT", () => shutdown("SIGINT"));
    await gateway.start();
  } catch (error) {
    // Error messages describe configuration/listeners, never credentials.
    console.error(`Gateway startup failed: ${error.message}`);
    process.exitCode = 1;
    if (gateway) await gateway.stop();
  }
}

if (require.main === module) main();
