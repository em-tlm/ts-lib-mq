const Promise = require('bluebird');
const amqp = require('amqplib');
const _ = require('lodash');
const assert = require('assert');
const config = require('./config').rabbitmqConfig;
const debug = require('debug')('ts-lib-mq:rabbitmq');
const EventEmitter = require('events');

// global settings and variables
const deadLetterName = 'deadletter';
const retryInterval = 2000;
let retry = 0;
// use this eventEmitter to communicate between the shared connection and rabbitmq instance
const eventEmitter = new EventEmitter();
// we do NOT expect any process to communicate with more than 20 queues
eventEmitter.setMaxListeners(20);

// Create the connection to RabbitMQ
let connection; // global, shared by all queues/channels

/**
 * Class representing a message queue
 */
class RabbitMQ extends EventEmitter {

    /**
     * Create a RabbitMQ message queue.
     * All message queue shares the same underlining connection to one single RabbitMQ.
     * @param {String} queueName - the name of the queue
     * @param {Object} options - options object to specify durable, persistent and prefetch
     */
    constructor(queueName, opts) {
        super();

        assert(_.isString(queueName), 'must pass a string as queueName name');
        this.queueName = queueName;

        const options = opts || {};

        assert(_.isUndefined(options.prefetch) ||
            (_.isNumber(options.prefetch) && options.prefetch > 0),
            'prefetch, if passed in, must be a positive number');

        assert(_.isUndefined(options.durable) || _.isBoolean(options.durable),
            'durable, if passed in, must be a boolean');

        assert(_.isUndefined(options.persistent) || _.isBoolean(options.persistent),
            'persistent, if passed in, must be a boolean');

        this.prefetch = options.prefetch || 20;

        if (_.isUndefined(options.durable)) {
            options.durable = true;
        }
        this.durable = options.durable;

        if (_.isUndefined(options.persistent)) {
            options.persistent = true;
        }
        this.persistent = options.persistent;

        // save all the message callbacks in case:
        // we need to reestablish the connection and reapply all the message callbacks
        this.callbacks = [];

        // FIXME: maintain backward compatibility, connect in constructor
        this.connect();

        // attach an function to the reconnection event for every instance of message queue.
        // by default, a maximum of 10 listeners can be registered due to NodeJs internal
        // implementation. this limit is increased to 20 (hardcoded in the beginning of this file)
        // there shouldn't be more than 20 instances of RabbitMQ in one process
        eventEmitter.on('reconnection', () => this.connect());

        // listen on the events and then emit them out for applications to deal with the events
        eventEmitter.on('error', err => this.emit('error', err));

        eventEmitter.on('close', () => this.emit('close'));

        eventEmitter.on('blocked', reason => this.emit('blocked', reason));

        eventEmitter.on('unblocked', () => this.emit('unblocked'));
    }

    _declareQueue() {
        return this.channel.then(channel => channel.assertQueue(this.queueName, {
            durable: this.durable,
            deadLetterExchange: deadLetterName,
        }));
    }

    _declareDeadQueue() {
        const deadQueue = `${this.queueName}.dead`;

        return this.channel.then(channel => channel.assertExchange(deadLetterName, 'direct', { durable: true })
            .then(() => channel.assertQueue(deadQueue, { durable: this.durable }))
            .then(() => channel.bindQueue(deadQueue, deadLetterName, this.queueName))
        );
    }

    sendToQueue(data) {
        return this.channel.then((channel) => {
            channel.sendToQueue(this.queueName, new Buffer(data), { persistent: this.persistent });
            return channel.waitForConfirms();
        });
    }

    batchSendToQueue(batchData) {
        return this.channel.then((channel) => {
            batchData.forEach(data => channel
                .sendToQueue(this.queueName, new Buffer(data), { persistent: this.persistent }));
            return channel.waitForConfirms();
        });
    }

    // if you call this method multiple times, then all the callbacks will be called
    setMessageCallback(callback) {
        this.callbacks.push(callback);

        return this.channel
            .then((channel) => {
                channel.prefetch(this.prefetch);
                return channel.consume(this.queueName, callback);
            })
            .then(response => response.consumerTag)
            .catch(err => debug(`Failed to set message callback on queueName ${this.queueName}. Error: ${err.message}`));
    }

    ack(message) {
        this.channel.then(channel => channel.ack(message));
    }

    reject(message, requeue) {
        this.channel.then(channel => channel.reject(message, requeue || false));
    }

    info() {
        return this._declareQueue();
    }

    purge() {
        return this.channel.then(channel => channel.purgeQueue(this.queueName));
    }

    // initialize channel and queues, add callbacks
    connect() {
        this.channel = connection.then(conn => conn.createConfirmChannel());

        this._declareQueue()
            .catch(e => debug(`Failed to declare queue. Error: ${e.message}`));
        this._declareDeadQueue()
            .catch(e => debug(`Failed to declare dead queue. Error: ${e.message}`));

        // attach all the msg callbacks
        this.callbacks.forEach(cb => this.setMessageCallback(cb));
    }

    // disconnect from channel, use to pause consumption
    disconnect() {
        return this.channel.then(channel => channel.close());
    }

    // reconnect to channel, will reset connection
    reconnect() {
        return this.disconnect().then(() => this.connect());
    }
}


function createConnection() {
    // over write the connection object everytime we want to establish the connection
    // this will happen during reconnection
    connection = amqp.connect(config.amqpEndpoint);

    connection
        .then((conn) => {
            // this is a reconnection
            if (retry > 0) {
                eventEmitter.emit('reconnection');
            }

            debug(retry
                ? `Connected to Rabbitmq successfully after ${retry} retry`
                : `Connected to Rabbitmq successfully the first time!`);

            // reset the retry counter
            retry = 0;

            // attach proper event listeners for error handling
            conn.on('error', (err) => {
                debug(`RabbitMQ connection has an error: ${err.message}`);
                eventEmitter.emit('error', err);
            });

            conn.on('close', () => {
                debug('RabbitMQ connection closed and reconnecting now');
                eventEmitter.emit('close');
                createConnection();
            });

            conn.on('blocked', (reason) => {
                debug(`RabbitMQ connection is blocked due to ${reason}`);
                eventEmitter.emit('blocked', reason);
            });

            conn.on('unblocked', () => {
                debug(`RabbitMQ connection is unblocked`);
                eventEmitter.emit('unblocked');
            });
        })
        .catch((err) => {
            debug(`Retry RabbitMQ connection: ${++retry} attempts. Error: ${err.message}`);
            // try to reconnect again if it fails
            return Promise.delay(retryInterval).then(createConnection);
        });
}

// create the connection to rabbitMQ
createConnection();

module.exports = RabbitMQ;
