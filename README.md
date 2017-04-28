```javascript
const queue = new require('ts-lib-mq')('test');
queue.sendToQueue({
    key: 'value'
});
```