const fs = require('fs');
const http = require('http');
const WebSocket = require('ws');

class WsTransport {
    constructor(options) {
        this.port = options.port || 3000;
        const server = new http.createServer({});
        this.server = server;
        const wss = new WebSocket.Server({ server });

        wss.on('connection', function connection(ws) {
            ws.on('message', function incoming(message) {
                console.log('received: %s', message, 'on worker: ' + process.pid);
                ws.send(message + ' from worker: ' + process.pid);
            });

        });
    }

    async start() {
        this.server.listen(this.port);
    }

    get isPermanentConnection() {
        return false;
    }
}

module.exports = WsTransport;