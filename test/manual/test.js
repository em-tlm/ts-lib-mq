'use strict';

const RabbitMQ = require('../../').RabbitMQ;
const debug = require('debug')('ts-lib-mq:test');

const queue = new RabbitMQ('testqueue1');
// const queue2 = new RabbitMQ('testqueue2');

queue.setMessageCallback((message) => {
    debug(JSON.parse(message.content.toString()));
    queue.ack(message);
});

queue.on('close', () => {
    debug('connection closed');
});

let counter = 0;

setInterval(() => queue
    .sendToQueue(JSON.stringify({ data: counter++ }))
    .catch(err => debug(`Failed to send to queue. Error: ${err.message}`)), 2500);
