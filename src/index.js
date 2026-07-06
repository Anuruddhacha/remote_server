const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { Registry } = require('./registry');
const { TcpRelay } = require('./relay');
const { log, warn, error } = require('./logger');

const WS_PORT = Number(process.env.PORT || 3000);
const RELAY_PORT = 9001;

const registry = new Registry();
const relay = new TcpRelay(RELAY_PORT);

/** @type {Map<string, { sessionId: string, hostId: string, viewerWs: import('ws').WebSocket, relayToken: string }>} */
const pendingSessions = new Map();

/** @type {Map<import('ws').WebSocket, { id: string, role: string }>} */
const socketMeta = new Map();

/** @type {Map<string, string>} ws -> hostId */
const socketToHostId = new Map();

let connectionCounter = 0;

function getPublicHost(ws) {
  return ws.publicHost || '127.0.0.1';
}

function describeSocket(ws) {
  const meta = socketMeta.get(ws);
  if (meta) {
    return `${meta.role}${meta.id ? `:${meta.id}` : ''}`;
  }
  return 'unknown';
}

function send(ws, payload) {
  if (ws.readyState !== 1) {
    warn('SEND', `Cannot send to closed socket (${describeSocket(ws)})`, payload.type);
    return;
  }
  log('SEND', `→ ${describeSocket(ws)}`, { type: payload.type, ...payload });
  ws.send(JSON.stringify(payload));
}

function makeSessionId() {
  return crypto.randomBytes(8).toString('hex');
}

function handleMessage(ws, msg) {
  log('MSG', `← ${describeSocket(ws)}`, { type: msg.type, ...msg });

  switch (msg.type) {
    case 'register': {
      const id = String(msg.id || '').trim();
      const deviceName = String(msg.deviceName || 'Remote Desk Host').trim();
      if (!/^\d{9}$/.test(id)) {
        warn('REGISTER', 'Invalid ID format', { id });
        send(ws, { type: 'error', code: 'invalid_id', message: 'ID must be 9 digits' });
        return;
      }

      const result = registry.register(id, ws, deviceName);
      if (!result.ok) {
        warn('REGISTER', 'ID already in use', { id });
        send(ws, { type: 'error', code: result.error, message: 'Remote ID already in use' });
        return;
      }

      socketToHostId.set(ws, id);
      socketMeta.set(ws, { id, role: 'host' });
      const publicHost = getPublicHost(ws);

      log('REGISTER', 'Host online', {
        id,
        deviceName,
        publicHost,
        relayPort: RELAY_PORT,
        onlineHosts: registry.count(),
      });

      send(ws, { type: 'register_ok', id, relayPort: RELAY_PORT, publicHost });
      break;
    }

    case 'heartbeat': {
      const hostId = socketToHostId.get(ws);
      if (hostId) {
        registry.touch(hostId);
        send(ws, { type: 'heartbeat_ack' });
      } else {
        warn('HEARTBEAT', 'Received from non-host socket');
      }
      break;
    }

    case 'connect_request': {
      const targetId = String(msg.targetId || '').trim();
      const viewerName = String(msg.viewerName || 'Viewer').trim();
      socketMeta.set(ws, { id: targetId, role: 'viewer' });

      if (!/^\d{9}$/.test(targetId)) {
        warn('CONNECT', 'Invalid target ID', { targetId });
        send(ws, { type: 'error', code: 'invalid_id', message: 'Target ID must be 9 digits' });
        return;
      }

      log('CONNECT', 'Viewer requesting connection', { targetId, viewerName, onlineHosts: registry.count() });

      const host = registry.get(targetId);
      if (!host || host.ws.readyState !== 1) {
        warn('CONNECT', 'Host offline or not found', { targetId, found: !!host });
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

      log('CONNECT', 'Session created, notifying host', {
        sessionId,
        targetId,
        relayToken: relayToken.slice(0, 8) + '...',
        pendingSessions: pendingSessions.size,
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
        warn('ACCEPT', 'Session not found or expired', { sessionId });
        send(ws, { type: 'error', code: 'session_not_found', message: 'Session expired' });
        return;
      }

      if (registry.get(pending.hostId)?.ws !== ws) {
        warn('ACCEPT', 'Unauthorized accept attempt', { sessionId, hostId: pending.hostId });
        send(ws, { type: 'error', code: 'unauthorized', message: 'Not the host for this session' });
        return;
      }

      const relayInfo = {
        publicHost: getPublicHost(pending.viewerWs),
        relayPort: RELAY_PORT,
        relayToken: pending.relayToken,
      };

      log('ACCEPT', 'Host accepted — sending relay info to both peers', {
        sessionId,
        hostId: pending.hostId,
        ...relayInfo,
        relayToken: pending.relayToken.slice(0, 8) + '...',
      });

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
        warn('REJECT', 'Session not found', { sessionId });
        return;
      }

      log('REJECT', 'Host rejected connection', { sessionId, reason: msg.reason || 'declined' });

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
      warn('MSG', 'Unknown message type', { type: msg.type });
      send(ws, { type: 'error', code: 'unknown_type', message: `Unknown message type: ${msg.type}` });
  }
}

async function main() {
  log('STARTUP', 'Starting Remote Desk signaling server...');

  await relay.start();
  log('STARTUP', `TCP relay listening on port ${RELAY_PORT}`);
  warn('STARTUP', `Relay port ${RELAY_PORT} may not be reachable on Back4App (only port ${WS_PORT} is public)`);

  const server = http.createServer((req, res) => {
    log('HTTP', `${req.method} ${req.url}`, { host: req.headers.host, ip: req.socket.remoteAddress });
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Remote Desk signaling server\n');
  });

  const wss = new WebSocketServer({ server });

  wss.on('connection', (ws, req) => {
    const connId = ++connectionCounter;
    const hostHeader = req.headers.host || '';
    ws.publicHost = hostHeader.split(':')[0] || '127.0.0.1';
    ws.connId = connId;

    log('WS', 'New WebSocket connection', {
      connId,
      publicHost: ws.publicHost,
      ip: req.socket.remoteAddress,
      origin: req.headers.origin || 'none',
    });

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch (err) {
        error('MSG', 'Invalid JSON received', { connId, raw: raw.toString().slice(0, 100) });
        send(ws, { type: 'error', code: 'invalid_json', message: 'Invalid JSON' });
        return;
      }
      handleMessage(ws, msg);
    });

    ws.on('close', (code, reason) => {
      const hostId = socketToHostId.get(ws);
      const meta = socketMeta.get(ws);

      log('WS', 'WebSocket disconnected', {
        connId,
        code,
        reason: reason?.toString() || '',
        role: meta?.role || 'unknown',
        hostId: hostId || 'n/a',
      });

      const removedId = registry.unregisterBySocket(ws);
      if (removedId) {
        log('REGISTER', 'Host went offline', { id: removedId, onlineHosts: registry.count() });
      }

      socketToHostId.delete(ws);
      socketMeta.delete(ws);

      for (const [sessionId, pending] of pendingSessions.entries()) {
        if (pending.viewerWs === ws) {
          log('CONNECT', 'Viewer disconnected — cleaning session', { sessionId });
          pendingSessions.delete(sessionId);
          relay.removeSession(pending.relayToken);
        }
      }
    });

    ws.on('error', (err) => {
      error('WS', 'WebSocket error', { connId, message: err.message });
    });
  });

  server.listen(WS_PORT, '0.0.0.0', () => {
    log('STARTUP', 'Server ready', {
      signalingPort: WS_PORT,
      relayPort: RELAY_PORT,
      pid: process.pid,
    });
  });
}

main().catch((err) => {
  error('STARTUP', 'Fatal error', { message: err.message, stack: err.stack });
  process.exit(1);
});
