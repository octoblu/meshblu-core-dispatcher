commander    = require 'commander'
async        = require 'async'
redis        = require 'redis'
debug        = require('debug')('meshblu-core-dispatcher:command')
packageJSON  = require './package.json'
QueueWorker  = require './src/queue-worker'

class Command
  parseList: (val) =>
    val.split ','

  parseOptions: =>
    commander
      .version packageJSON.version
      .option '--internal-namespace <meshblu:internal>', 'job handler queue namespace.', 'meshblu:internal'
      .option '-j, --jobs <job1,job2>', 'jobs this worker is willing to do', @parseList
      .option '-s, --single-run', 'perform only one job.'
      .option '-t, --timeout <n>', 'seconds to wait for a next job.', parseInt, 30
      .parse process.argv

    {@internalNamespace,@singleRun,@timeout,@jobs} = commander
    @client = redis.createClient process.env.REDIS_URI

  run: =>
    @parseOptions()

    queueWorker = new QueueWorker
      timeout:   @timeout
      namespace: @internalNamespace
      client:    @client
      jobs:      @jobs

    if @singleRun
      queueWorker.run(@panic)
      return

    async.forever queueWorker.run, @panic

  panic: (error) =>
    console.error error.stack
    process.exit 1

command = new Command()
command.run()
