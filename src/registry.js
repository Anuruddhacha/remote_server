class Registry {
  constructor() {
    /** @type {Map<string, { ws: import('ws').WebSocket, deviceName: string, registeredAt: number }>} */
    this.hosts = new Map();
  }

  register(id, ws, deviceName) {
    const existing = this.hosts.get(id);
    if (existing && existing.ws !== ws && existing.ws.readyState === 1) {
      return { ok: false, error: 'id_in_use' };
    }

    this.set(id, ws, deviceName);
    return { ok: true };
  }

  /** @param {string} id @param {import('ws').WebSocket} ws @param {string} deviceName */
  set(id, ws, deviceName) {
    this.unregisterBySocket(ws);
    this.hosts.set(id, { ws, deviceName, registeredAt: Date.now() });
  }

  get(id) {
    return this.hosts.get(id) ?? null;
  }

  unregisterBySocket(ws) {
    for (const [id, entry] of this.hosts.entries()) {
      if (entry.ws === ws) {
        this.hosts.delete(id);
        return id;
      }
    }
    return null;
  }

  touch(id) {
    const entry = this.hosts.get(id);
    if (entry) {
      entry.registeredAt = Date.now();
    }
  }
}

module.exports = { Registry };
