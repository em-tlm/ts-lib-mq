"use strict";

let QueueAdapter = require('../../RabbitMQ.js');

let queue = new QueueAdapter('testqueue');

queue
    .setMessageCallback(function (message) {
        console.log(JSON.parse(message.content.toString()));
    })

setInterval(function(){
    queue.sendToQueue(JSON.stringify({data:'test'})).catch(function(err){
        console.error(`Failed to send to queue. Error: ${err.message}`);
    });
}, 5000);