const http = require('http');

class HttpTransport {
    constructor(options) {
        this.port = options.port || 3000;
        this.server = http.createServer((req, res) => {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('hello from worker: ' + process.pid + '\n');
        });
    }

    async start() {
        this.server.listen(this.port);
    }


    get isPermanentConnection() {
        return false;
    }
}

module.exports = HttpTransport;