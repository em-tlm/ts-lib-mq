"use strict";

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
const eventEmitter = new EventEmitter();
eventEmitter.setMaxListeners(20); // do NOT expect any process to connect to more than 20 queues


// Create the connection to RabbitMQ
let connection;
createConnection();

/**
 * Class representing a message queue
 */
class RabbitMQ {

    /**
     * Create a RabbitMQ message queue.
     * All message queue shares the same underlining connection to one single RabbitMQ.
     * @param {String} queueName - the name of the queue
     * @param {Object} options - options object to specify durable, persistent and prefetch
     */
    constructor(queueName, options) {

        assert(_.isString(queueName), `must pass a string as queueName name`);
        this.queueName = queueName;

        options = options || {};

        assert(_.isUndefined(options.prefetch) ||
            _.isNumber(options.prefetch) && options.prefetch > 0,
            `prefetch, if passed in, must be a positive number `);

        assert(_.isUndefined(options.durable) || _.isBoolean(options.durable),
            `durable, if passed in, must be a boolean`);

        assert(_.isUndefined(options.persistent) || _.isBoolean(options.persistent),
            `persistent, if passed in, must be a boolean`);

        this.prefetch = options.prefetch || 20;
        this.durable = options.durable || true;
        this.persistent = options.persistent || true;

        this.connection = connection;
        this.channel = this._createChannel();

        // save all the message callbacks in case:
        // we need to reestablish the connection and reapply all the message callbacks
        this.callbacks = [];
        this._declareQueue()
            .catch((e) => {
                debug(`Failed to declare queue. Error: ${e.message}`);
            });
        this._declareDeadQueue()
            .catch(function (e) {
                debug(`Failed to declare dead queue. Error: ${e.message}`);
            });


        // attach an function to the reconnection event for every instance of message queue.
        // by default, a maximum of 10 listeners can be registered due to NodeJs internal implementation
        // this limit is increased to 20 (hardcoded in the beginning of this file)
        // there shouldn't be more than 20 instances of RabbitMQ in one process
        eventEmitter.on('reconnection', () => {
            console.log(this);
            // now that the connection is back, let's reinitialize the channel and queue
            this.channel = this._createChannel();
            this._declareQueue();
            this._declareDeadQueue();

            // reattach all the msg callbacks
            this.callbacks.forEach((cb) => {
                this.setMessageCallback(cb);
            })
        });
    };

    _createChannel() {
        return connection.then(function (connection) {
            return connection.createConfirmChannel();
        });
    };

    _declareQueue() {
        const queueName = this.queueName;
        const channel = this.channel;
        return channel.then((channel) => {
            return channel.assertQueue(queueName, {
                durable: this.durable,
                deadLetterExchange: deadLetterName
            });
        });
    };


    _declareDeadQueue() {
        const channel = this.channel;
        const routingKey = this.queueName;
        const deadQueue = this.queueName + '.dead';
        const durable = this.durable;
        return channel.then(function (channel) {
            return channel.assertExchange(deadLetterName, 'direct', {
                durable: true
            }).then(function () {
                return channel.assertQueue(deadQueue, {
                    durable: durable
                });
            }).then(function () {
                return channel.bindQueue(deadQueue, deadLetterName, routingKey);
            });
        });
    };

    sendToQueue(data) {
        const queueName = this.queueName;
        const channel = this.channel;
        const persistent = this.persistent;
        return channel.then(function (channel) {
            channel.sendToQueue(queueName, new Buffer(data), {persistent: persistent});
            return channel.waitForConfirms();
        })
    };

    batchSendToQueue(batchData) {
        const queueName = this.queueName;
        const channel = this.channel;
        const persistent = this.persistent;
        return channel.then(function (channel) {
            batchData.forEach(function (data) {
                channel.sendToQueue(queueName, new Buffer(data), {persistent: persistent});
            });
            return channel.waitForConfirms();
        });
    };


    //if you call this method multiple times, then all the callbacks will be called
    setMessageCallback(callback) {
        const self = this;
        const queueName = this.queueName;
        const channel = this.channel;

        // save the callback first
        self.callbacks.push(callback);

        channel
            .then((channel) => {
                channel.prefetch(this.prefetch);
                channel.consume(queueName, callback);
            })
            .catch((err) => {
                debug(`Failed to set message callback on queueName ${this.queueName}. Error: ${err.message}`);
            });

    };

    ack(message) {
        const channel = this.channel;
        channel.then(function (channel) {
            channel.ack(message);
        });
    };

    reject(message) {
        const channel = this.channel;
        channel.then(function (channel) {
            channel.reject(message, false);
        });
    };


    info() {
        return this._declareQueue();
    };

    getLastMessage() {
        const queueName = this.queueName;
        const channel = this.channel;
        const msg = channel.then(function (channel) {
            return channel.get(queueName);
        });
        return Promise.all([channel, msg]).spread(function (channel, msg) {
            channel.nackAll();
            return msg;
        });
    };

    purge() {
        const queueName = this.queueName;
        const channel = this.channel;
        return channel.then(function (channel) {
            return channel.purgeQueue(queueName);
        });
    };


}


function createConnection() {

    // over write the connection object everytime we want to reestablish the connection
    connection = amqp.connect(config.amqpEndpoint);

    connection
        .then(function (conn) {
            // this is a reconnection
            if (retry > 0) {
                eventEmitter.emit('reconnection');
            }

            debug(retry ? `Connected to Rabbitmq successful after ${retry} retry` : `connected to Rabbitmq successful the first time!`);

            // reset the retry counter
            retry = 0;

            // attach proper event listeners for error handling
            conn.on('error', function (err) {
                debug(`RabbitMQ connection has an error: ${err.message}`);
            });

            conn.on('close', function () {
                debug('RabbitMQ connection closed and reconnecting now');
                createConnection();
            });

        })
        .catch(function (err) {
            Promise
                .delay(retryInterval)
                .then(function () {
                    createConnection(); // try to reconnect again if it fails
                    retry++;
                    debug(`Retry RabbitMQ connection: ${retry} attempts. Error: ${err.message}`);
                })
        });
}

module.exports = RabbitMQ;