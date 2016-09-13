class Source {
  constructor (host, port) {
    this.handler = {};
  }
  on (evt, cb) {
    this.handler[evt] = cb;
  }
  trigger (evt, value) {
    this.handler[evt](value);
  }
  start () {
    // ToDo: read from socket instead
    setTimeout(() => {
      this.trigger('data', 'This is line 1');
      setTimeout(() => {
        this.trigger('data', 'This is line 2');
        setTimeout(() => {
          this.trigger('end');
        }, 500);
      }, 500);
    }, 500);
  }
}

class PrintSink {
  constructor (source) {
    this.source = source;
    this.handler = {};
  }
  on (evt, cb) { // ToDo: shared stream implementation
    this.handler[evt] = cb;
  }
  trigger (evt, value) {
    this.handler[evt](value);
  }
  start () {
    this.source.on('data', console.log);
    this.source.on('end', () => { this.trigger('end'); })
    this.source.start();
  }
}

class DataStream {
  constructor (env) {
    this.env = env;
  }
  flatMap (fn) {
    // ToDo: add flatMap to plan
    return this;
  }
  keyBy (index) {
    // ToDo: add keyBy to plan
    return this;
  }
  sum () {
    // ToDo: add sum to plan
    return this;
  }
  print () {
    this.env.plan = new PrintSink(this.env.plan);
    // ToDo: add print to plan
  }
}

class StreamExecutionEnvironment {
  socketTextStream (host, port) {
    const dataStream = new DataStream(this);
    this.plan = new Source (host, port);
    return dataStream;
  }

  executing () {
    return new Promise(function (resolve, reject) {
      this.plan.on('end', resolve);
      this.plan.on('error', reject);
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
