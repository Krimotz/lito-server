const express = require('express');
const expressWs = require('express-ws');

const app = express();
expressWs(app);

const PORT = process.env.PORT || 8080;

// Map<roomId, Set<WebSocket>>
const rooms = new Map();

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

// Health check endpoint for Render
app.get('/health', (req, res) => {
  res.status(200).send('LiTo relay is running');
});

// Root endpoint
app.get('/', (req, res) => {
  res.send('LiTo relay is running');
});

// WebSocket endpoint - accepts ANY path and parses it manually
app.ws('/*', (ws, req) => {
  const match = req.url.match(/^\/(broadcast|listen)\/([a-zA-Z0-9-]+)/);
  if (!match) {
    log('Invalid WebSocket path:', req.url);
    ws.close(1008, 'invalid path');
    return;
  }

  const [, role, roomId] = match;
  log(`${role} attempting to join room ${roomId}`);

  const room = rooms.get(roomId);

  // Broadcaster: always allowed, creates the room
  if (role === 'broadcast') {
    if (!rooms.has(roomId)) rooms.set(roomId, new Set());
    const r = rooms.get(roomId);
    r.add(ws);
    ws.role = role;
    ws.roomId = roomId;
    ws.send(JSON.stringify({ type: 'joined', role, roomId }));
    log(`broadcast joined room ${roomId}`);
  }

  // Listener: only allowed if a broadcaster is already present
  else if (role === 'listen') {
    if (!room || room.size === 0) {
      log(`listen rejected: no broadcaster in room ${roomId}`);
      ws.send(JSON.stringify({ type: 'error', message: 'No broadcaster in that room' }));
      ws.close(1008, 'no broadcaster');
      return;
    }
    room.add(ws);
    ws.role = role;
    ws.roomId = roomId;
    ws.send(JSON.stringify({ type: 'joined', role, roomId }));
    log(`listen joined room ${roomId}`);
  }

  ws.on('message', (data, isBinary) => {
    if (ws.role !== 'broadcast') return;
    if (!isBinary) return;
    const r = rooms.get(ws.roomId);
    if (!r) return;
    for (const peer of r) {
      if (peer !== ws && peer.readyState === 1) {
        peer.send(data, { binary: true });
      }
    }
  });

  ws.on('close', () => {
    log(`${ws.role} left room ${ws.roomId}`);
    const r = rooms.get(ws.roomId);
    if (!r) return;
    r.delete(ws);
    if (r.size === 0) rooms.delete(ws.roomId);
  });

  ws.on('error', (err) => {
    log(`error in room ${ws.roomId}:`, err.message);
  });
});

app.listen(PORT, '0.0.0.0', () => {
  log(`LiTo relay server listening on port ${PORT}`);
});
