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
const eventEmitter = new EventEmitter(); // use this eventEmitter to communicate between the shared connection and rabbitmq instance
eventEmitter.setMaxListeners(20); // we do NOT expect any process to communicate with more than 20 queues


// Create the connection to RabbitMQ
let connection; // global, shared by all queues/channels
createConnection();

/**
 * Class representing a message queue
 */
class RabbitMQ extends EventEmitter{

    /**
     * Create a RabbitMQ message queue.
     * All message queue shares the same underlining connection to one single RabbitMQ.
     * @param {String} queueName - the name of the queue
     * @param {Object} options - options object to specify durable, persistent and prefetch
     */
    constructor(queueName, options) {
        super();

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

        if (_.isUndefined(options.durable)){
            options.durable = true;
        }
        this.durable = options.durable;

        if (_.isUndefined(options.persistent)){
            options.persistent = true;
        }
        this.persistent = options.persistent;


        this.channel = this._createChannel();

        // save all the message callbacks in case:
        // we need to reestablish the connection and reapply all the message callbacks
        this.callbacks = [];
        this._declareQueue()
            .catch((e) => {
                debug(`Failed to declare queue. Error: ${e.message}`);
            });
        this._declareDeadQueue()
            .catch((e) => {
                debug(`Failed to declare dead queue. Error: ${e.message}`);
            });


        // attach an function to the reconnection event for every instance of message queue.
        // by default, a maximum of 10 listeners can be registered due to NodeJs internal implementation
        // this limit is increased to 20 (hardcoded in the beginning of this file)
        // there shouldn't be more than 20 instances of RabbitMQ in one process
        eventEmitter.on('reconnection', () => {
            // now that the connection is back, let's reinitialize the channel and queue
            this.channel = this._createChannel();
            this._declareQueue();
            this._declareDeadQueue();

            // reattach all the msg callbacks
            this.callbacks.forEach((cb) => {
                this.setMessageCallback(cb);
            })
        });

        // listen on the events and then emit them out for applications to deal with the events
        eventEmitter.on('error', (err) => {
            this.emit('error', err);
        });

        eventEmitter.on('close', () => {
            this.emit('close');
        });

        eventEmitter.on('blocked', (reason) => {
            this.emit('blocked', reason);
        });

        eventEmitter.on('unblocked', () => {
            this.emit('unblocked');
        });
    };

    _createChannel() {
        return connection.then((connection) => {
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
        return channel.then( (channel) => {
            return channel.assertExchange(deadLetterName, 'direct', {
                durable: true
            }).then( () => {
                return channel.assertQueue(deadQueue, {
                    durable: durable
                });
            }).then( () => {
                return channel.bindQueue(deadQueue, deadLetterName, routingKey);
            });
        });
    };

    sendToQueue(data) {
        const queueName = this.queueName;
        const channel = this.channel;
        const persistent = this.persistent;
        return channel.then( (channel) =>{
            channel.sendToQueue(queueName, new Buffer(data), {persistent: persistent});
            return channel.waitForConfirms();
        })
    };

    batchSendToQueue(batchData) {
        const queueName = this.queueName;
        const channel = this.channel;
        const persistent = this.persistent;
        return channel.then( (channel) => {
            batchData.forEach( (data) => {
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
        channel.then( (channel) => {
            channel.ack(message);
        });
    };

    reject(message, requeue) {
        const channel = this.channel;
        channel.then( (channel) => {
            channel.reject(message, requeue || false);
        });
    };


    info() {
        return this._declareQueue();
    };


    purge() {
        const queueName = this.queueName;
        const channel = this.channel;
        return channel.then( (channel) => {
            return channel.purgeQueue(queueName);
        });
    };


}


function createConnection() {

    // over write the connection object everytime we want to establish the connection
    // this will happen during reconnection
    connection = amqp.connect(config.amqpEndpoint);

    connection
        .then( (conn) => {
            // this is a reconnection
            if (retry > 0) {
                eventEmitter.emit('reconnection');
            }

            debug(retry ? `Connected to Rabbitmq successful after ${retry} retry` : `connected to Rabbitmq successful the first time!`);

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
        .catch( (err) => {
            Promise
                .delay(retryInterval)
                .then(() => {
                    createConnection(); // try to reconnect again if it fails
                    retry++;
                    debug(`Retry RabbitMQ connection: ${retry} attempts. Error: ${err.message}`);
                })
        });
}

module.exports = RabbitMQ;