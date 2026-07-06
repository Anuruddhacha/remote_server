const net = require('net');
const crypto = require('crypto');
const { log, warn, error } = require('./logger');

class TcpRelay {
  constructor(port) {
    this.port = port;
    /** @type {Map<string, { host?: net.Socket, viewer?: net.Socket, hostBuffer: Buffer, viewerBuffer: Buffer, bytesHost: number, bytesViewer: number }>} */
    this.sessions = new Map();
    this.server = net.createServer((socket) => this.handleConnection(socket));
  }

  start(host = '0.0.0.0') {
    return new Promise((resolve, reject) => {
      this.server.listen(this.port, host, () => resolve());
      this.server.on('error', (err) => {
        error('RELAY', 'Relay server error', { message: err.message });
        reject(err);
      });
    });
  }

  createToken() {
    return crypto.randomBytes(16).toString('hex');
  }

  handleConnection(socket) {
    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    log('RELAY', 'New TCP connection (awaiting handshake)', { remote });

    let buffer = Buffer.alloc(0);

    const onData = (chunk) => {
      buffer = Buffer.concat([buffer, toBuffer(chunk)]);
      const newline = buffer.indexOf(0x0a);
      if (newline === -1) {
        return;
      }

      socket.removeListener('data', onData);

      let msg;
      try {
        msg = JSON.parse(buffer.slice(0, newline).toString('utf8'));
      } catch {
        warn('RELAY', 'Invalid handshake JSON — closing', { remote });
        socket.destroy();
        return;
      }

      const token = msg.token;
      const role = msg.role;
      if (!token || (role !== 'host' && role !== 'viewer')) {
        warn('RELAY', 'Invalid handshake — closing', { remote, token: !!token, role });
        socket.destroy();
        return;
      }

      log('RELAY', 'Handshake OK', { remote, role, token: token.slice(0, 8) + '...' });

      const remaining = buffer.slice(newline + 1);
      this.attachSocket(token, role, socket, remaining);
    };

    socket.on('data', onData);
    socket.on('error', (err) => {
      warn('RELAY', 'Socket error before handshake', { remote, message: err.message });
      socket.destroy();
    });
  }

  attachSocket(token, role, socket, initialBuffer) {
    if (!this.sessions.has(token)) {
      this.sessions.set(token, {
        hostBuffer: Buffer.alloc(0),
        viewerBuffer: Buffer.alloc(0),
        bytesHost: 0,
        bytesViewer: 0,
      });
      log('RELAY', 'New relay session created', { token: token.slice(0, 8) + '...' });
    }

    const session = this.sessions.get(token);
    session[role] = socket;

    const peerRole = role === 'host' ? 'viewer' : 'host';
    const bufferKey = peerRole === 'host' ? 'hostBuffer' : 'viewerBuffer';
    const peer = session[peerRole];

    log('RELAY', `${role} attached to session`, {
      token: token.slice(0, 8) + '...',
      peerConnected: !!(peer && !peer.destroyed),
      bufferedForPeer: session[bufferKey].length,
    });

    if (initialBuffer.length > 0) {
      this.forward(session, token, peerRole, initialBuffer);
    }

    if (session[bufferKey].length > 0) {
      log('RELAY', `Flushing ${session[bufferKey].length} buffered bytes to ${role}`, {
        token: token.slice(0, 8) + '...',
      });
      socket.write(session[bufferKey]);
      session[bufferKey] = Buffer.alloc(0);
    }

    socket.on('data', (chunk) => {
      this.forward(session, token, peerRole, chunk);
    });

    const cleanup = () => {
      log('RELAY', `${role} disconnected from session`, { token: token.slice(0, 8) + '...' });
      session[role] = undefined;
      if (!session.host && !session.viewer) {
        log('RELAY', 'Session closed (both peers gone)', {
          token: token.slice(0, 8) + '...',
          bytesHost: session.bytesHost,
          bytesViewer: session.bytesViewer,
        });
        this.sessions.delete(token);
      }
    };

    socket.on('close', cleanup);
    socket.on('error', cleanup);
  }

  forward(session, token, peerRole, chunk) {
    const data = toBuffer(chunk);
    const fromRole = peerRole === 'host' ? 'viewer' : 'host';
    const bytesKey = fromRole === 'host' ? 'bytesHost' : 'bytesViewer';
    session[bytesKey] += data.length;

    const peer = session[peerRole];
    if (peer && !peer.destroyed) {
      peer.write(data);
      if (session[bytesKey] % 500000 < data.length) {
        log('RELAY', `Forwarding data ${fromRole} → ${peerRole}`, {
          token: token.slice(0, 8) + '...',
          chunkBytes: data.length,
          totalBytes: session[bytesKey],
        });
      }
      return;
    }

    const bufferKey = peerRole === 'host' ? 'hostBuffer' : 'viewerBuffer';
    session[bufferKey] = Buffer.concat([session[bufferKey], data]);
    if (session[bufferKey].length > 4 * 1024 * 1024) {
      warn('RELAY', 'Buffer overflow — dropping data', { token: token.slice(0, 8) + '...' });
      session[bufferKey] = Buffer.alloc(0);
    } else {
      log('RELAY', `Buffering data for absent ${peerRole}`, {
        token: token.slice(0, 8) + '...',
        chunkBytes: data.length,
        bufferTotal: session[bufferKey].length,
      });
    }
  }

  sessionCount() {
    return this.sessions.size;
  }

  removeSession(token) {
    const session = this.sessions.get(token);
    if (!session) {
      return;
    }
    log('RELAY', 'Session removed by signaling', { token: token.slice(0, 8) + '...' });
    if (session.host) {
      session.host.destroy();
    }
    if (session.viewer) {
      session.viewer.destroy();
    }
    this.sessions.delete(token);
  }
}

function toBuffer(chunk) {
  return Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
}

module.exports = { TcpRelay };
