// Rudihütte Signal Worker v58 – protocol compatible with the Version 58 app.
// The random 24-character pairing room is the shared secret; only the matching
// host/client pair in that room can exchange signaling messages.
const ALLOWED_ORIGINS = new Set(['https://rudihuette58.pages.dev']);
const ROOM_RE = /^[A-Za-z0-9_-]{12,80}$/;

export class RudiSignalRoom {
  constructor(state, env) { this.state = state; this.env = env; }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname !== '/signal') return new Response('Not found', { status: 404 });
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('WebSocket required', { status: 426 });
    const room = url.searchParams.get('room') || '';
    if (!ROOM_RE.test(room)) return new Response('Invalid room', { status: 400 });

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    const peerId = crypto.randomUUID();
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ room, peerId, role: null });
    server.send(JSON.stringify({ type: 'connected', peerId }));
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, message) {
    const sender = ws.deserializeAttachment();
    if (!sender?.room) { try { ws.close(1008, 'Invalid session'); } catch {} return; }

    let data;
    try { data = JSON.parse(typeof message === 'string' ? message : new TextDecoder().decode(message)); }
    catch { try { ws.close(1003, 'Invalid JSON'); } catch {} return; }
    if (!data || typeof data !== 'object') return;

    if (data.kind === 'hello') {
      if (!['host', 'client'].includes(data.role)) { try { ws.close(1008, 'Invalid role'); } catch {} return; }
      sender.role = data.role;
      ws.serializeAttachment(sender);

      // Replace an older socket from the same device role. This prevents a
      // sleeping Safari tab and its resumed socket from creating duplicate peers.
      for (const other of this.state.getWebSockets()) {
        if (other === ws || other.readyState !== WebSocket.OPEN) continue;
        const info = other.deserializeAttachment();
        if (info?.room === sender.room && info?.role === sender.role) {
          try { other.close(4001, 'Replaced by resumed connection'); } catch {}
        }
      }

      const opposite = sender.role === 'host' ? 'client' : 'host';
      let found = false;
      for (const other of this.state.getWebSockets()) {
        if (other === ws || other.readyState !== WebSocket.OPEN) continue;
        const info = other.deserializeAttachment();
        if (info?.room === sender.room && info?.role === opposite) {
          found = true;
          try { other.send(JSON.stringify({ type: 'peer-joined', peerId: sender.peerId })); } catch {}
        }
      }
      if (found) {
        try { ws.send(JSON.stringify({ type: 'peer-joined', peerId: 'existing-peer' })); } catch {}
      }
      return;
    }

    if (!sender.role) { try { ws.close(1008, 'Send hello first'); } catch {} return; }
    const allowed = ['request-offer', 'offer', 'answer', 'ice'];
    if (!allowed.includes(data.kind)) return;
    if (data.kind === 'offer' && (typeof data.sdp !== 'string' || data.sdp.length > 200000)) return;
    if (data.kind === 'answer' && (!data.description || typeof data.description.sdp !== 'string' || data.description.sdp.length > 200000)) return;
    if (data.kind === 'ice' && (!data.candidate || typeof data.candidate !== 'object')) return;

    const opposite = sender.role === 'host' ? 'client' : 'host';
    for (const other of this.state.getWebSockets()) {
      if (other === ws || other.readyState !== WebSocket.OPEN) continue;
      const info = other.deserializeAttachment();
      if (info?.room === sender.room && info?.role === opposite) {
        try { other.send(JSON.stringify({ type: 'signal', from: sender.peerId, data })); } catch {}
      }
    }
  }

  webSocketClose(ws) {
    const sender = ws.deserializeAttachment();
    if (sender?.room && sender?.role) {
      const opposite = sender.role === 'host' ? 'client' : 'host';
      for (const other of this.state.getWebSockets()) {
        if (other === ws || other.readyState !== WebSocket.OPEN) continue;
        const info = other.deserializeAttachment();
        if (info?.room === sender.room && info?.role === opposite) {
          try { other.send(JSON.stringify({ type: 'peer-left', peerId: sender.peerId })); } catch {}
        }
      }
    }
    try { ws.close(); } catch {}
  }

  webSocketError(ws) { try { ws.close(1011, 'Socket error'); } catch {} }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return Response.json({ ok: true, service: 'rudihuette-signal-v58', protocol: 'app-v58' });
    if (url.pathname !== '/signal') return new Response('Not found', { status: 404 });
    const origin = request.headers.get('Origin');
    if (!ALLOWED_ORIGINS.has(origin)) return new Response('Forbidden: origin not allowed', { status: 403 });
    if (request.headers.get('Upgrade') !== 'websocket') return new Response('WebSocket required', { status: 426 });
    const room = url.searchParams.get('room') || '';
    if (!ROOM_RE.test(room)) return new Response('Invalid room', { status: 400 });
    const id = env.SIGNAL_ROOMS.idFromName(room);
    return env.SIGNAL_ROOMS.get(id).fetch(request);
  }
};
