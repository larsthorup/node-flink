class Stream {
  constructor () {
    this.handler = {};
  }
  on (evt, cb) {
    this.handler[evt] = cb;
  }
  trigger (evt, value) {
    this.handler[evt](value);
  }
}


class SocketSource {
  constructor (host, port) {
    this.stream = new Stream();
  }
  start () {
    // ToDo: read from socket instead
    setTimeout(() => {
      this.stream.trigger('data', 'This is line 1');
      setTimeout(() => {
        this.stream.trigger('data', 'This is line 2');
        setTimeout(() => {
          this.stream.trigger('end');
        }, 500);
      }, 500);
    }, 500);
  }
}

class FlatMapOperator {
  constructor (fn, source) {
    this.source = source;
    this.stream = new Stream();
    this.fn = fn;
  }
  start () {
    this.source.stream.on('data', (value) => {
      const out = {
        collect: (value) => {
          this.stream.trigger('data', value);
        }
      };
      this.fn(value, out);
    });
    this.source.stream.on('end', () => { this.stream.trigger('end'); })
    this.source.start();
  }
}

class KeyByOperator {
  constructor (index, source) {
    this.source = source;
    this.stream = new Stream();
    this.index = index;
  }
  start () {
    this.source.stream.on('data', (value) => {
      const key = value[this.index];
      const keyedValue = {key: key, value: value};
      this.stream.trigger('data', keyedValue);
    });
    this.source.stream.on('end', () => { this.stream.trigger('end'); })
    this.source.start();
  }
}

class SumOperator {
  constructor (index, source) {
    this.source = source;
    this.stream = new Stream();
    this.index = index;
    this.state = {};
  }
  start () {
    this.source.stream.on('data', (value) => {
      const key = value.key;
      const val = value.value[this.index];
      const oldValue = this.state.hasOwnProperty(key) ? this.state[key] : 0;
      const newValue = oldValue + val;
      this.state[key] = newValue;
      const keyedValue = {key: key, value: newValue};
      this.stream.trigger('data', keyedValue);
    });
    this.source.stream.on('end', () => { this.stream.trigger('end'); })
    this.source.start();
  }
}

class PrintSink {
  constructor (source) {
    this.source = source;
    this.stream = new Stream();
  }
  start () {
    this.source.stream.on('data', console.log);
    this.source.stream.on('end', () => { this.stream.trigger('end'); })
    this.source.start();
  }
}

class DataStream {
  constructor (env) {
    this.env = env;
  }
  flatMap (fn) {
    this.env.plan = new FlatMapOperator(fn, this.env.plan);
    return this;
  }
  keyBy (index) {
    this.env.plan = new KeyByOperator(index, this.env.plan);
    return this;
  }
  sum (index) {
    this.env.plan = new SumOperator(index, this.env.plan)
    return this;
  }
  print () {
    this.env.plan = new PrintSink(this.env.plan);
  }
}

class StreamExecutionEnvironment {
  socketTextStream (host, port) {
    const dataStream = new DataStream(this);
    this.plan = new SocketSource (host, port);
    return dataStream;
  }

  executing () {
    return new Promise(function (resolve, reject) {
      this.plan.stream.on('end', resolve);
      this.plan.stream.on('error', reject);
      this.plan.start();
    }.bind(this));
  }
}

function gettingExecutionEnvironment () {
  const env = new StreamExecutionEnvironment();
  return Promise.resolve(env);
}

module.exports = {
  gettingExecutionEnvironment
};
