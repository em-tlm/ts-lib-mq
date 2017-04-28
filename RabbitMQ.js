"use strict";

const Promise = require('bluebird');
const amqp = require('amqplib');
//const config = require('./../config').config;
const config = {};
const debug = require('debug')('ts-lib-mq:rabbitmq');
const EventEmitter = require('events');

/**
 * Rabbitmq settings
 */
const defaultAMQPEndpoint = 'amqp://localhost';
const deadLetterName = 'deadletter';
const defaultOptions = {
    durable: false,    // if a queue is durable
    persistent: false, // if a message is persistent
    prefetch: 100     // maximum numbers of message this consumer can get from the queue
};

/**
 * The global connection variable and event emitter.
 * The event emitter will be shared among all the instances of QueueAdapter
 */
// todo: test what if it is const/let
let connection;
let eventEmitter = new EventEmitter();
eventEmitter.setMaxListeners(20);

/**
 * Rabbitmq connection retry configuration
 */
const retryInterval = 5000;
let retry = 0;


/**
 * Create connection to Rabbitmq
 */
function createConnection() {

    // over write the connection object everytime we want to reestablis the connecction
    connection = amqp.connect(config.getAMQPEndpoint || defaultAMQPEndpoint);
    connection
        .then(function (conn) {
            // this is a reconnection
            if (retry > 0) {
                eventEmitter.emit('reconnection');
            }

            debug(retry ? `Connected to Rabbitmq successful after ${retry} retry` : `connected to Rabbitmq successful the first time!`);
            // attache event listener to deal with unexpected behavior
            conn.on('error', function (err) {
                debug(`Connection has an error: ${err.message}`);
            });

            conn.on('close', function () {
                debug('Connection closed and reconnecting now');
                // create the connection again
                createConnection();
            });

            // reset the retry counter
            retry = 0;

        })
        .catch(function (err) {
            Promise
                .delay(retryInterval)
                .then(function () {
                    // try to reconnect again if it fails
                    createConnection();
                    retry++;
                    debug(`Retry rabbitmq connection: ${retry} attempts. Error: ${err.message}`);
                })
        });
}

/**
 * Crate the first connection to Rabbitmq !!!
 */
createConnection();


// todo: rewrite this in ES6 class
var QueueAdapter = function (queue, options) {
    let self = this;

    this.queue = queue;
    this.options = options || defaultOptions;
    this.channel = this._createChannel();
    this.callbacks = [];
    this._declareQueue();
    this._declareDeadQueue();

    // attach an event listener to every time there is a new queue initialized
    // by default, a maximum of 10 listeners can be registered due to nodejs internal implementation
    // there shouldn't be more than 10 instances of QueueAdatper in one program
    eventEmitter.on('reconnection', function () {
        // now that the connection is back, let's reinitialize the queue/channel
        self.channel = self._createChannel();
        self._declareQueue();
        self._declareDeadQueue();

        // reattach all the callbacks
        self.callbacks.forEach(function(cb){
            self.setMessageCallback(cb);
        })
    });
};

QueueAdapter.prototype._createChannel = function () {
    return connection.then(function (connection) {
        return connection.createConfirmChannel();
    });
};

QueueAdapter.prototype._declareQueue = function () {
    var queue = this.queue;
    var channel = this.channel;
    var options = this.options;
    return channel.then(function (channel) {
        return channel.assertQueue(queue, {
            durable: options.durable,
            deadLetterExchange: deadLetterName
        });
    }).catch(function(e){
        debug(e.message);
    });
};

QueueAdapter.prototype._declareDeadQueue = function () {
    var channel = this.channel;
    var routingKey = this.queue;
    var deadQueue = this.queue + '.dead';
    var options = this.options;
    return channel.then(function (channel) {
        return channel.assertExchange(deadLetterName, 'direct', {
            durable: true
        }).then(function () {
            return channel.assertQueue(deadQueue, {
                durable: options.durable
            });
        }).then(function () {
            return channel.bindQueue(deadQueue, deadLetterName, routingKey);
        });
    }).catch(function(e){
        debug(e.message);
    });
};


QueueAdapter.prototype.sendToQueue = function (data) {
    var queue = this.queue;
    var channel = this.channel;
    var options = this.options;
    return channel.then(function (channel) {
        channel.sendToQueue(queue, new Buffer(data), {persistent: options.persistent});
        return channel.waitForConfirms();
    })
};

//batchData should be an array of "data"
QueueAdapter.prototype.batchSendToQueue = function (batchData) {
    var queue = this.queue;
    var channel = this.channel;
    var options = this.options;
    return channel.then(function (channel) {
        batchData.forEach(function (data) {
            channel.sendToQueue(queue, new Buffer(data), {persistent: options.persistent});
        });
        return channel.waitForConfirms();
    });
};

//if you call this method multiple times, then all the callbacks will be called
QueueAdapter.prototype.setMessageCallback = function (callback) {
    var self = this;
    var queue = this.queue;
    var channel = this.channel;

    // save the callback first
    self.callbacks.push(callback);

    channel
        .then(function (channel) {
            channel.prefetch(defaultOptions.prefetch);
            channel.consume(queue, callback);
        })
        .catch(function (err) {
            debug(`Failed to set message callback on queue ${self.queue}. Error: ${err.message}`);
        });

};

QueueAdapter.prototype.ack = function (message) {
    var channel = this.channel;
    channel.then(function (channel) {
        channel.ack(message);
    });
};

QueueAdapter.prototype.reject = function (message) {
    var channel = this.channel;
    channel.then(function (channel) {
        channel.reject(message, false);
    });
};

QueueAdapter.prototype.size = function () {
    return this._declareQueue().then(function (queueInfo) {
        return queueInfo.messageCount;
    });
};

QueueAdapter.prototype.getLastMessage = function () {
    var queue = this.queue;
    var channel = this.channel;
    var msg = channel.then(function (channel) {
        return channel.get(queue);
    });
    return Promise.all([channel, msg]).spread(function (channel, msg) {
        channel.nackAll();
        return msg;
    });
};

QueueAdapter.prototype.purge = function () {
    var queue = this.queue;
    var channel = this.channel;
    return channel.then(function (channel) {
        return channel.purgeQueue(queue);
    });
};

module.exports = QueueAdapter;
