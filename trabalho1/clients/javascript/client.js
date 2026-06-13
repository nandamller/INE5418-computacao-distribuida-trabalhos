const net = require('net');
const fs = require('fs');


class EncurtadorClient {
  constructor() {
    this.host = process.env.PROXY_HOST || 'proxy';
    this.port = parseInt(process.env.PROXY_PORT || '8080', 10);
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
