const net = require('net');
const crypto = require('crypto');

class TcpRelay {
  constructor(port) {
    this.port = port;
    /** @type {Map<string, { host?: net.Socket, viewer?: net.Socket, hostBuffer: Buffer, viewerBuffer: Buffer }>} */
    this.sessions = new Map();
    this.server = net.createServer((socket) => this.handleConnection(socket));
  }

  start(host = '0.0.0.0') {
    return new Promise((resolve, reject) => {
      this.server.listen(this.port, host, () => resolve());
      this.server.on('error', reject);
    });
  }

  createToken() {
    return crypto.randomBytes(16).toString('hex');
  }

  handleConnection(socket) {
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
        socket.destroy();
        return;
      }

      const token = msg.token;
      const role = msg.role;
      if (!token || (role !== 'host' && role !== 'viewer')) {
        socket.destroy();
        return;
      }

      const remaining = buffer.slice(newline + 1);
      this.attachSocket(token, role, socket, remaining);
    };

    socket.on('data', onData);
    socket.on('error', () => socket.destroy());
  }

  attachSocket(token, role, socket, initialBuffer) {
    if (!this.sessions.has(token)) {
      this.sessions.set(token, { hostBuffer: Buffer.alloc(0), viewerBuffer: Buffer.alloc(0) });
    }

    const session = this.sessions.get(token);
    session[role] = socket;

    const peerRole = role === 'host' ? 'viewer' : 'host';
    const bufferKey = peerRole === 'host' ? 'hostBuffer' : 'viewerBuffer';

    if (initialBuffer.length > 0) {
      this.forward(session, peerRole, initialBuffer);
    }

    if (session[bufferKey].length > 0) {
      socket.write(session[bufferKey]);
      session[bufferKey] = Buffer.alloc(0);
    }

    socket.on('data', (chunk) => {
      this.forward(session, peerRole, chunk);
    });

    const cleanup = () => {
      session[role] = undefined;
      if (!session.host && !session.viewer) {
        this.sessions.delete(token);
      }
    };

    socket.on('close', cleanup);
    socket.on('error', cleanup);
  }

  forward(session, peerRole, chunk) {
    const data = toBuffer(chunk);
    const peer = session[peerRole];
    if (peer && !peer.destroyed) {
      peer.write(data);
      return;
    }

    const bufferKey = peerRole === 'host' ? 'hostBuffer' : 'viewerBuffer';
    session[bufferKey] = Buffer.concat([session[bufferKey], data]);
    if (session[bufferKey].length > 4 * 1024 * 1024) {
      session[bufferKey] = Buffer.alloc(0);
    }
  }

  removeSession(token) {
    const session = this.sessions.get(token);
    if (!session) {
      return;
    }
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
