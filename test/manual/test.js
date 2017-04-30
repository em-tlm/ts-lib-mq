"use strict";

const RabbitMQ = require('../../RabbitMQ.js');
const debug = require('debug')('ts-lib-mq:test');

let queue = new RabbitMQ('testqueue1');

queue
    .setMessageCallback(function (message) {
        debug(JSON.parse(message.content.toString()));
        queue.ack(message);
    });

let counter = 0;
setInterval(function(){

    queue.sendToQueue(JSON.stringify({data: counter})).catch(function(err){
        debug(`Failed to send to queue. Error: ${err.message}`);
    });
    counter++;
}, 2500);