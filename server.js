const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const url = require('url');

const rooms = new Map();
const pins = new Map();

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);
  if (parsed.pathname.startsWith('/pin/')) {
    const pin = parsed.pathname.split('/')[2];
    const sessionId = pins.get(pin);
    if (!sessionId || !rooms.has(sessionId)) {
      res.writeHead(404, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
      return res.end(JSON.stringify({ error: 'Session not found' }));
    }
    const room = rooms.get(sessionId);
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({
      pin,
      sessionId,
      sessionKey: room.sessionKey,
      hostName: room.hostName,
      serverUrl: `https://${req.headers.host}`
    }));
  }
  res.writeHead(200);
  res.end('AvidTune Relay Online');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const parsed = url.parse(req.url, true);
  if (parsed.pathname === '/together/relay') {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', (ws, req) => {
  const parsed = url.parse(req.url, true);
  const { sessionId, role, pin, sessionKey, name } = parsed.query;

  if (role === 'host') {
    const room = { hostSocket: ws, guests: new Map(), pin, hostName: name, sessionKey };
    rooms.set(sessionId, room);
    if (pin) pins.set(pin, sessionId);

    ws.on('message', (msg) => {
      try {
        const data = JSON.parse(msg.toString());
        if (data.type === 'broadcast') {
          room.guests.forEach((g) => g.readyState === WebSocket.OPEN && g.send(data.message));
        } else if (data.type === 'to_guest' && data.participantId) {
          const g = room.guests.get(data.participantId);
          if (g && g.readyState === WebSocket.OPEN) g.send(data.message);
        }
      } catch (e) {
        room.guests.forEach((g) => g.readyState === WebSocket.OPEN && g.send(msg.toString()));
      }
    });

    ws.on('close', () => {
      if (pin) pins.delete(pin);
      room.guests.forEach((g) => {
        if (g.readyState === WebSocket.OPEN) {
          g.send(JSON.stringify({ type: 'host_command', command: 'KICK' }));
          g.close();
        }
      });
      rooms.delete(sessionId);
    });
  } else {
    const room = rooms.get(sessionId);
    if (!room || room.hostSocket.readyState !== WebSocket.OPEN) {
      ws.close(1008, 'Room unavailable');
      return;
    }
    const participantId = Math.random().toString(36).substring(2, 10);
    room.guests.set(participantId, ws);
    room.hostSocket.send(JSON.stringify({ type: 'guest_connected', participantId }));

    ws.on('message', (msg) => {
      if (room.hostSocket.readyState === WebSocket.OPEN) {
        room.hostSocket.send(JSON.stringify({ type: 'from_guest', participantId, message: msg.toString() }));
      }
    });

    ws.on('close', () => {
      room.guests.delete(participantId);
      if (room.hostSocket.readyState === WebSocket.OPEN) {
        room.hostSocket.send(JSON.stringify({ type: 'guest_disconnected', participantId }));
      }
    });
  }
});

const port = process.env.PORT || 8080;
server.listen(port, () => console.log(`Relay running on port ${port}`));
