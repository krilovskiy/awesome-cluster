let AwesomeCluster = require('./awesome-cluster');

let HttpTransport = require('./httpTransport');
let WsTransport = require('./wsTransport');

let myTransport = new HttpTransport({port: 8080});

let clusterOptions = {
    transport: myTransport,
    cluster: {
        workers: 2,
        respawn: false
    }
};


let myCluser = new AwesomeCluster(clusterOptions);

myCluser.start();