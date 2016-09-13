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


class Source {
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
