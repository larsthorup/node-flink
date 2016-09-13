const co = require('co');

const flink = require('./flink');

function lineSplitter (value, out) {
  const tokens = value.toLowerCase().split(' ');
  for (token of tokens) {
    if (token.length > 0) {
      out.collect([token, 1]);
    }
  }
}

function running () {
  return co(function * () {
    const env = yield flink.gettingExecutionEnvironment();
    env
      .socketTextStream('localhost', 9999)
      .flatMap(lineSplitter)
      .keyBy(0)
      .sum(1)
      .print();
    yield env.executing();
  });
}

running().then(
  () => { console.log('Done'); process.exit(0); },
  (err) => { console.log('Error', err); process.exit(1); }
);