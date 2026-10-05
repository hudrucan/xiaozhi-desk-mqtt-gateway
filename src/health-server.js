const http = require("node:http");

function createHealthServer(gateway) {
  const server = http.createServer((request, response) => {
    const route = request.url?.split("?")[0];
    let code = 200;
    let result;
    if (request.method !== "GET") {
      code = 405;
      response.setHeader("Allow", "GET");
      result = { error: "Method not allowed" };
    } else if (route === "/healthz") {
      result = { status: "alive", gateway_id: gateway.id };
    } else if (route === "/readyz") {
      code = gateway.isReady() ? 200 : 503;
      result = { status: code === 200 ? "ready" : "not_ready", gateway_id: gateway.id };
    } else if (route === "/status") {
      result = gateway.status();
    } else {
      code = 404;
      result = { error: "Not found" };
    }
    request.resume();
    response.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    response.end(JSON.stringify(result));
  });
  server.requestTimeout = 5000;
  server.headersTimeout = 5000;
  return server;
}

module.exports = { createHealthServer };
