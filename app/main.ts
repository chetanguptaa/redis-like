import * as net from "net";

// Uncomment the code below to pass the first stage
const server: net.Server = net.createServer((connection: net.Socket) => {
  // Handle connection
  connection.on("data", (data: Buffer) => {
    connection.write(`+PONG\r\n`);
  });
});

server.listen(6379, "127.0.0.1");
