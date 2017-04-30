```javascript
const queue = new require('ts-lib-mq')('test');
queue.sendToQueue({
    key: 'value'
});
```

Manual test
```bash
DEBUG=ts-lib-mq:* node test/manual/test.js
```