#### Usage 


```javascript
const RabbitMQ = new require('ts-lib-mq').RabbitMQ;
const queue = new RabbitMQ('test');
queue.sendToQueue({
    key: 'value'
});
queue.on('error', (err) => {
    console.error(err);
});
```

Manual test
```bash
# start a rabbitmq server on mac and tail its log
rabbitmq-server 
tail -f /usr/local/var/log/rabbitmq/rabbit@localhost.log
```
```bash
# start the test
DEBUG=ts-lib-mq:* node test/manual/test.js
```

#### todo
1. more unit test
2. more readme and function comments
