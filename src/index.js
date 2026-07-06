const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { Registry } = require('./registry');
const { TcpRelay } = require('./relay');

const WS_PORT = Number(process.env.PORT || 3000);
const RELAY_PORT = 9001;

const registry = new Registry();
const relay = new TcpRelay(RELAY_PORT);

/** @type {Map<string, { sessionId: string, hostId: string, viewerWs: import('ws').WebSocket, relayToken: string }>} */
const pendingSessions = new Map();

/** @type {Map<string, string>} ws -> hostId */
const socketToHostId = new Map();

function getPublicHost(ws) {
  return ws.publicHost || '127.0.0.1';
}

function send(ws, payload) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(payload));
  }
}

function makeSessionId() {
  return crypto.randomBytes(8).toString('hex');
}

function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'register': {
      const id = String(msg.id || '').trim();
      const deviceName = String(msg.deviceName || 'Remote Desk Host').trim();
      if (!/^\d{9}$/.test(id)) {
        send(ws, { type: 'error', code: 'invalid_id', message: 'ID must be 9 digits' });
        return;
      }

      const result = registry.register(id, ws, deviceName);
      if (!result.ok) {
        send(ws, { type: 'error', code: result.error, message: 'Remote ID already in use' });
        return;
      }

      socketToHostId.set(ws, id);
      send(ws, { type: 'register_ok', id, relayPort: RELAY_PORT, publicHost: getPublicHost(ws) });
      break;
    }

    case 'heartbeat': {
      const hostId = socketToHostId.get(ws);
      if (hostId) {
        registry.touch(hostId);
        send(ws, { type: 'heartbeat_ack' });
      }
      break;
    }

    case 'connect_request': {
      const targetId = String(msg.targetId || '').trim();
      const viewerName = String(msg.viewerName || 'Viewer').trim();
      if (!/^\d{9}$/.test(targetId)) {
        send(ws, { type: 'error', code: 'invalid_id', message: 'Target ID must be 9 digits' });
        return;
      }

      const host = registry.get(targetId);
      if (!host || host.ws.readyState !== 1) {
        send(ws, { type: 'error', code: 'host_offline', message: 'Host is offline' });
        return;
      }

      const sessionId = makeSessionId();
      const relayToken = relay.createToken();
      pendingSessions.set(sessionId, {
        sessionId,
        hostId: targetId,
        viewerWs: ws,
        relayToken,
      });

      send(ws, { type: 'connect_pending', sessionId, targetId });

      send(host.ws, {
        type: 'incoming_request',
        sessionId,
        viewerName,
        targetId,
      });
      break;
    }

    case 'connect_accept': {
      const sessionId = String(msg.sessionId || '');
      const pending = pendingSessions.get(sessionId);
      if (!pending) {
        send(ws, { type: 'error', code: 'session_not_found', message: 'Session expired' });
        return;
      }

      if (registry.get(pending.hostId)?.ws !== ws) {
        send(ws, { type: 'error', code: 'unauthorized', message: 'Not the host for this session' });
        return;
      }

      const relayInfo = {
        publicHost: getPublicHost(pending.viewerWs),
        relayPort: RELAY_PORT,
        relayToken: pending.relayToken,
      };

      send(pending.viewerWs, {
        type: 'connect_accept',
        sessionId,
        role: 'viewer',
        ...relayInfo,
      });

      send(ws, {
        type: 'connect_accept',
        sessionId,
        role: 'host',
        ...relayInfo,
      });

      pendingSessions.delete(sessionId);
      break;
    }

    case 'connect_reject': {
      const sessionId = String(msg.sessionId || '');
      const pending = pendingSessions.get(sessionId);
      if (!pending) {
        return;
      }

      send(pending.viewerWs, {
        type: 'connect_rejected',
        sessionId,
        reason: msg.reason || 'declined',
      });
      pendingSessions.delete(sessionId);
      relay.removeSession(pending.relayToken);
      break;
    }

    default:
      send(ws, { type: 'error', code: 'unknown_type', message: `Unknown message type: ${msg.type}` });
  }
}

async function main() {
  await relay.start();

  const server = http.createServer((_req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Remote Desk signaling server\n');
  });

  const wss = new WebSocketServer({ server });

  wss.on('connection', (ws, req) => {
    const hostHeader = req.headers.host || '';
    ws.publicHost = hostHeader.split(':')[0] || '127.0.0.1';

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        send(ws, { type: 'error', code: 'invalid_json', message: 'Invalid JSON' });
        return;
      }
      handleMessage(ws, msg);
    });

    ws.on('close', () => {
      registry.unregisterBySocket(ws);
      socketToHostId.delete(ws);

      for (const [sessionId, pending] of pendingSessions.entries()) {
        if (pending.viewerWs === ws) {
          pendingSessions.delete(sessionId);
          relay.removeSession(pending.relayToken);
        }
      }
    });
  });

  server.listen(WS_PORT, '0.0.0.0', () => {
    console.log(`Signaling WS:  port ${WS_PORT}`);
    console.log(`TCP relay:     port ${RELAY_PORT}`);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
