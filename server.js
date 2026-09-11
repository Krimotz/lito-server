const express = require('express');
const expressWs = require('express-ws');

const app = express();
expressWs(app); // Initialize express-ws

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

// Root endpoint (optional, for browser visits)
app.get('/', (req, res) => {
  res.send('LiTo relay is running');
});

// WebSocket endpoint
app.ws('/(:role)/(:roomId)', (ws, req) => {
  const { role, roomId } = req.params;

  if (role !== 'broadcast' && role !== 'listen') {
    ws.close(1008, 'invalid role');
    return;
  }

  log(`${role} joined room ${roomId}`);

  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  const room = rooms.get(roomId);

  ws.role = role;
  ws.roomId = roomId;
  room.add(ws);

  ws.on('message', (data, isBinary) => {
    if (ws.role !== 'broadcast') return;
    if (!isBinary) return;
    for (const peer of room) {
      if (peer !== ws && peer.readyState === 1) { // 1 = OPEN
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

app.listen(PORT, '0.0.0.0', () => {
  log(`LiTo relay server listening on port ${PORT}`);
});
