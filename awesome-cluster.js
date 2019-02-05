/**
 *  @fileOverview Модуль кластеризации сетевого nodejs приложения
 *
 *  @author telegram: @little0big
 *
 *  @version 1.0.0
 */

const os = require('os');
const net = require('net');
const ch = require('child_process');
const util = require('util');

/**
 * Подключает дебаг и выставляет его значения
 * в зависимости от того, какой процесс запущен (мастер или воркер)
 * @type {Object}
 */
let debug;

/**
 * Оффсет номера порта.
 * Используется при создании воркера и его дебаге
 * @type {Number}
 */
let debugPortOffset = 1;

/**
 * Уникальный id воркера. Выставляется в его env при создании
 * @type {Number}
 */
let processUniqueId = 0;

/**
 * Максимальный сетевой порт.
 * Используется при старте воркера в дебаг-режиме
 * @type {Number}
 */
const maxPort = 65535;

/**
 * Класс кластеризует сетевое приложение nodejs
 * и распределяет входящие соединения по воркерам.
 * Логика работы воркера вынесена в подключаемые файлы транспортов.
 * Экземпляр транспорта необходимо передать в конструктор.
 *
 * @see this.transport.isPermanentConnection
 * Реализовано два типа балансировки нагрузки
 * в зависимости от параметров транспорта:
 * 1 - Round Robin
 * 2 - Липучка
 *
 * Round Robin - Каждое новое входящее соединение попаадет
 * на следующего в списке воркера
 *
 * Липучка - в зависимости от socket.remoteAddress, новое соединение
 * будет прикреплено к определенному воркеру.
 * @class
 * @see httpTransport.js
 * @see wsTransport.js
 */
class AwesomeCluster extends net.Server {
    /**
     * Конструктор кластера. Считывает параметры кластеризации.
     * Инициализирует состояние кластера.
     * @constructor
     * @param {Object} options Опции кластеризации приложения
     * @param {Object} options.transport Объект транспорта
     * @param {Object} options.cluster объект с параметрами кластеризации
     * @param {Number} options.cluster.workers количество воркеров
     * @param {Boolean} options.cluster.respawn нужно ли пересоздавать воркеров
     *                  при их падении.
     */
    constructor(options) {
        super();
        this.transport = options.transport;
        this.isClusterMode = !!options.cluster;
        if (this.isClusterMode) {
            this._isMaster = true;
            this._isWorker = false;
            this.clusterOptions = options.cluster;
            this.workerCount = options.cluster.workers || os.cpus().length;
            this.port = this.transport.port;
            const respawn = options.cluster.respawn;
            this.needRespawn = (
                typeof respawn !== 'undefined' ? respawn : true
            );
        }
        this.env = options.env || {};
    }

    /**
     * Запускает кластер и воркеров.
     * В зависимости от режима кластеризации и опций,
     */
    async start() {
        if (this.isClusterMode) {
            if (this.isMaster) {
                await this.startCluster();
            } else {
                await this.startWorker();
                await this.startTransport();
            }
        } else {
            await this.startWorker();
            await this.startTransport();
        }
    }

    /**
     * Запускает транспорт, который был передан в параметрах
     * Используется только при запуске воркеров
     *
     * В транспорте должен быть установлен transport.server,
     *
     * Перехватывает метод server.listen транспорта в режиме кластера
     * для корректно балансировки
     */
    async startTransport() {
        if (this.isClusterMode) {
            debug('we should catch listen function');
            const server = this.transport.server;
            if (server.listen && typeof server.listen === 'function') {
                this.transport.server.listen = function(port) {
                    debug('fake listening on port: ', port);
                };
            }
        }
        try {
            await this.transport.start();
        } catch (e) {
            this.logError(`Failed to start transport: ${e.message}`);
            process.exit(1);
        }
    }

    /**
     * Запускает и настраивает воркер процессы в режиме кластера
     */
    async startWorker() {
        // Выставляем корректный дебаг
        debug = require('debug')('AwesomeCluster:Worker');
        if (this.isClusterMode) {
            debug(`starting worker: ${process.pid}`);
        } else {
            debug('starting single worker');
            return;
        }


        // Перезаписывает оригинальный close для информирвоания о выходе
        const server = this.transport.server;
        const oldClose = server.close;
        server.close = function close() {
            debug('graceful close');
            process.send({type: 'close'});
            return oldClose.apply(this, arguments);
        };

        process.on('message', (msg, socket) => {
            if (msg !== 'balancing' || !socket) return;

            // У нас новый сокет. Передадим его в соответствующий сервер
            debug('incoming socket');
            server._connections++;
            socket.server = server;
            server.emit('connection', socket);
        });
    }

    /**
     * Запускаем кластер и форкаем его
     */
    async startCluster() {
        // Выставляем корректный дебаг
        debug = require('debug')('AwesomeCluster:Master');
        debug('starting master process');

        const self = this;

        // стартуем net.Server и устанавливаем обработчик
        // входящих соединений
        net.Server.call(this, {
            pauseOnConnect: true,
        }, this.balance);

        this.workers = [];

        // Сид для хэширования socket.remoteAddress
        this.seed = (Math.random() * 0xffffffff) | 0;
        debug('master seed=%d', this.seed);

        this.listen(this.port);

        // Когда сервер начал слушать, запускаем воркеров
        this.once('listening', async function() {
            debug('master listening on %j', self.address());

            for (let i = 0; i < self.workerCount; i++) {
                await self.spawnWorker();
            }
        });
    }

    /**
     * Проверка является ли процесс мастер-процессом
     * @return {boolean}
     */
    get isMaster() {
        // process is a worker
        this._isWorker = !!process.env.CLUSTER_MASTER_PID;

        // process is master
        this._isMaster = !this._isWorker;

        // process id
        this.pid = process.pid;
        if (this._isMaster) process.env.CLUSTER_MASTER_PID = this.pid;

        return this._isMaster;
    }

    /**
     * Логирование ошибок
     * @param {String} msg сообщение для лога
     */
    logError(msg) {
        console.error(msg);
    }

    /**
     * Стартуем воркера и навешиваем обработчики событий
     */
    async spawnWorker() {
        const worker = await this.fork(this.env);
        const self = this;

        worker.on('exit', async (code) => {
            debug('worker=%d died with code=%d', worker.pid, code);
            // Если нужно, форкнемся еще раз
            if (self.needRespawn) {
                await self.respawn(worker);
            }
        });

        worker.on('message', async (msg) => {
            if (msg.type === 'close') {
                // Если нужно, форкнемся еще раз
                if (self.needRespawn) {
                    await self.respawn(worker);
                }
            }
        });

        worker.on('error', (err) => {
            this.logError('Error on worker: ', err);
        });

        debug('worker=%d spawn', worker.pid);
        this.workers.push(worker);
    }

    /**
     * Пересоздание воркера
     * @param {ChildProcess} worker - процесс воркера
     */
    async respawn(worker) {
        const index = this.workers.indexOf(worker);
        if (index !== -1) this.workers.splice(index, 1);
        await this.spawnWorker();
    }

    /**
     * Реализация форка
     * @param {Object} env - окружение для воркера
     */
    async fork(env) {
        // Выставляем аргументы форка
        const workerEnv = util._extend({}, process.env);
        const execArgv = process.execArgv.slice();
        const inspectRegex = /--inspect(?:-brk|-port)?|--debug-port/;

        util._extend(workerEnv, env);
        workerEnv.NODE_UNIQUE_ID = `${++processUniqueId}`;
        if (execArgv.some((arg) => arg.match(inspectRegex))) {
            // выставляем debugPort для корректного дебага
            let inspectPort = process.debugPort + debugPortOffset;
            if (inspectPort > maxPort) inspectPort -= 1;
            debugPortOffset++;
            for (let i = 0; i < execArgv.length; i++) {
                if (execArgv[i].includes('--inspect-brk')) {
                    execArgv.splice(i, 1);
                }
            }
            execArgv.push(`--inspect-brk=${inspectPort}`);
        }

        return ch.fork(process.argv[1], [], {
            execArgv,
            env: workerEnv,
        });
    }

    /**
     * Функция хэширования адреса текущего соединения
     * @param {Buffer} ip - адрес текущего соединения
     * @return {number|*} - хэш текущего соединения
     */
    hash(ip) {
        let hash = this.seed;
        for (let i = 0; i < ip.length; i++) {
            const num = ip[i];

            hash += num;
            hash %= 2147483648;
            hash += (hash << 10);
            hash %= 2147483648;
            hash ^= hash >> 6;
        }

        hash += hash << 3;
        hash %= 2147483648;
        hash ^= hash >> 11;
        hash += hash << 15;
        hash %= 2147483648;

        return hash >>> 0;
    }

    /**
     * Балансировка входящего соединения.
     * В зависимости от транспорта
     * @see this.transport.isPermanentConnection
     * реализует один из видов балансировки.
     *
     * @param {Socket} socket - сокет входяшего соединения
     */
    balance(socket) {
        const addr = Buffer.from(socket.remoteAddress || '127.0.0.1');
        let worker;

        // находим нужного воркера
        if (this.transport.isPermanentConnection) {
            // sticky algo
            debug('sticky balancing connection %j', addr);
            const hash = this.hash(addr);
            worker = this.workers[hash % this.workers.length];
        } else {
            // round robin algo
            debug('rr balancing connection %j', addr);
            worker = this.workers.shift();
            this.workers.push(worker);
        }

        try {
            // Пытаемся отправить сокет
            if (worker.connected) {
                worker.send('balancing', socket);
            } else {
                throw new Error('Can\'t send message to worker');
            }
        } catch (e) {
            this.logError(e);
            socket.emit('close');
            socket.end();
        }
    }
}

module.exports = AwesomeCluster;
