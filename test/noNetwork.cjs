'use strict';
// Preload for offline regression runs. Never use when starting the application.
const deny = () => { throw new Error('Network access disabled during offline tests'); };
for (const name of ['node:http', 'node:https']) {
  const transport = require(name);
  transport.request = deny;
  transport.get = deny;
}
const net = require('node:net');
net.connect = deny;
net.createConnection = deny;
net.Socket.prototype.connect = deny;
require('node:tls').connect = deny;
globalThis.fetch = deny;
