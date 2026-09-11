const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// Map<roomId, Set<WebSocket>>
const rooms = new Map();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

wss.on('connection', (ws, req) => {
  // URL shape: /broadcast/<roomId>  or  /listen/<roomId>
  const match = req.url.match(/^\/(broadcast|listen)\/([a-zA-Z0-9-]+)/);
  if (!match) {
    ws.close(1008, 'invalid path');
    return;
  }

  const [, role, roomId] = match;
  log(`${role} joined room ${roomId}`);

  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  const room = rooms.get(roomId);

  ws.role = role;
  ws.roomId = roomId;
  room.add(ws);

  ws.on('message', (data, isBinary) => {
    if (ws.role !== 'broadcast') return;   // only broadcasters send audio
    if (!isBinary) return;                 // ignore any text frames
    for (const peer of room) {
      if (peer !== ws && peer.readyState === WebSocket.OPEN) {
        peer.send(data, { binary: true });
      }
    }
  });

  ws.on('close', () => {
    log(`${role} left room ${roomId}`);
    room.delete(ws);
    if (room.size === 0) rooms.delete(roomId);
  });

  ws.on('error', (err) => {
    log(`error in room ${roomId}:`, err.message);
  });
});

log(`LiTo relay server listening on port ${PORT}`);
