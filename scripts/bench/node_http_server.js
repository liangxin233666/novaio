const cluster = require("cluster");
const http = require("http");

const port = Number(process.env.PORT || 8081);
const workers = Number(process.env.WORKERS || 1);
const body = "Hello NovaIO!";

if (cluster.isPrimary && workers > 1) {
  for (let i = 0; i < workers; i += 1) {
    cluster.fork();
  }

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, () => {
      for (const id in cluster.workers) {
        cluster.workers[id].kill(signal);
      }
      process.exit(0);
    });
  }

  console.log(`Node/libuv cluster starting ${workers} workers on ${port}`);
  return;
}

const server = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "text/plain",
    "Content-Length": Buffer.byteLength(body),
    "Connection": "keep-alive",
  });
  res.end(body);
});

server.keepAliveTimeout = 60_000;
server.headersTimeout = 65_000;

server.listen(port, "0.0.0.0", () => {
  console.log(`Node/libuv HTTP server listening on ${port}, pid=${process.pid}`);
});
