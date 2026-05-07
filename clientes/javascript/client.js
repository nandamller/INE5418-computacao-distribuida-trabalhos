const net = require('net');
const fs = require('fs');

function loadConfig(path = 'config.txt') {
  const cfg = {};
  if (!fs.existsSync(path)) return cfg;
  for (const line of fs.readFileSync(path, 'utf-8').split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#') || !trimmed.includes('=')) continue;
    const [k, ...rest] = trimmed.split('=');
    cfg[k.trim()] = rest.join('=').trim();
  }
  return cfg;
}

class EncurtadorClient {
  constructor() {
    const cfg = loadConfig();
    this.host = process.env.PROXY_HOST || cfg.client_proxy_host || 'proxy';
    this.port = parseInt(process.env.PROXY_PORT || cfg.client_proxy_port || '8080', 10);
  }

  _enviarComando(payload) {
    return new Promise((resolve) => {
      const socket = net.createConnection({ host: this.host, port: this.port });
      let buffer = Buffer.alloc(0);

      socket.on('connect', () => {
        socket.write(JSON.stringify(payload));
      });

      socket.on('data', (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);
      });

      socket.on('end', () => {
        try {
          resolve(JSON.parse(buffer.toString('utf-8')));
        } catch (e) {
          resolve({ erro: `Falha ao decodificar resposta: ${e.message}` });
        }
      });

      socket.on('error', (err) => {
        resolve({ erro: `Falha na comunicação: ${err.message}` });
      });
    });
  }

  encurta(urlOriginal) {
    return this._enviarComando({ acao: 'encurta', url: urlOriginal });
  }

  resolve(codigoCurto) {
    return this._enviarComando({ acao: 'resolve', codigo: codigoCurto });
  }

  removeUrl(codigoCurto) {
    return this._enviarComando({ acao: 'remove', codigo: codigoCurto });
  }
}

module.exports = { EncurtadorClient };
