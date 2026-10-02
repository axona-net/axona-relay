// Load test run by relayctl.ps1 before any relay is started: node-datachannel's
// native binary must load on this host. A file, not `node -e`, because Windows
// PowerShell 5.1 strips the double quotes from a native command's arguments.
const n = require('node-datachannel');
const p = new n.PeerConnection('t', { iceServers: [] });
p.createDataChannel('x');
p.close();
console.log('  load-test: node-datachannel OK on', process.platform, process.arch);
setTimeout(() => process.exit(0), 300);
