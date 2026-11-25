import { DurableObject } from "cloudflare:workers";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json({
        ok: true,
        service: "rudihuette-signal-v58"
      });
    }

    if (url.pathname !== "/signal") {
      return new Response("Not found", { status: 404 });
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("WebSocket required", { status: 426 });
    }

    const room = url.searchParams.get("room");

    if (!room || !/^[a-zA-Z0-9_-]{8,80}$/.test(room)) {
      return new Response("Invalid room", { status: 400 });
    }

    const id = env.SIGNAL_ROOMS.idFromName(room);
    const stub = env.SIGNAL_ROOMS.get(id);

    return stub.fetch(request);
  }
};

export class RudiSignalRoom extends DurableObject {
  async fetch(request) {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    const peerId = crypto.randomUUID();

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({
      peerId,
      connectedAt: Date.now()
    });

    const sockets = this.ctx.getWebSockets();

    server.send(JSON.stringify({
      type: "connected",
      peerId
    }));

    for (const socket of sockets) {
      if (socket === server) continue;

      try {
        socket.send(JSON.stringify({
          type: "peer-joined",
          peerId
        }));
      } catch (error) {
        console.error("Peer notification failed", error);
      }
    }

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }

  async webSocketMessage(socket, message) {
    const text = typeof message === "string"
      ? message
      : new TextDecoder().decode(message);

    if (text.length > 65536) {
      socket.close(1009, "Message too large");
      return;
    }

    let data;

    try {
      data = JSON.parse(text);
    } catch {
      socket.send(JSON.stringify({
        type: "error",
        message: "Invalid JSON"
      }));
      return;
    }

    const attachment = socket.deserializeAttachment();
    const outgoing = JSON.stringify({
      type: "signal",
      from: attachment?.peerId || null,
      data
    });

    for (const peer of this.ctx.getWebSockets()) {
      if (peer === socket) continue;

      try {
        peer.send(outgoing);
      } catch (error) {
        console.error("Signal relay failed", error);
      }
    }
  }

  async webSocketClose(socket, code, reason) {
    const attachment = socket.deserializeAttachment();

    for (const peer of this.ctx.getWebSockets()) {
      if (peer === socket) continue;

      try {
        peer.send(JSON.stringify({
          type: "peer-left",
          peerId: attachment?.peerId || null
        }));
      } catch (error) {
        console.error("Peer leave notification failed", error);
      }
    }

    if (
      code >= 1000 &&
      code < 5000 &&
      ![1004, 1005, 1006, 1015].includes(code)
    ) {
      socket.close(code, reason);
    } else {
      socket.close();
    }
  }

  async webSocketError(socket) {
    try {
      socket.close(1011, "WebSocket error");
    } catch {}
  }
}
