_            = require 'lodash'
commander    = require 'commander'
async        = require 'async'
redis        = require 'redis'
redisMock    = require 'fakeredis'
debug        = require('debug')('meshblu-core-dispatcher:command')
packageJSON  = require './package.json'
Dispatcher   = require './src/dispatcher'
JobAssembler = require './src/job-assembler'
QueueWorker  = require './src/queue-worker'

class CommandDispatch
  @ALL_JOBS: ['authenticate']

  parseList: (val) =>
    val.split ','

  parseOptions: =>
    commander
      .version packageJSON.version
      .usage 'Run the dispatch worker. All jobs not outsourced will be run in-process.'
      .option '-n, --namespace <meshblu>', 'request/response queue namespace.', 'meshblu'
      .option '-i, --internal-namespace <meshblu:internal>', 'job handler queue namespace.', 'meshblu:internal'
      .option '-o, --outsource-jobs <job1,job2>', 'jobs for external workers', @parseList
      .option '-s, --single-run', 'perform only one job.'
      .option '-t, --timeout <n>', 'seconds to wait for a next job.', parseInt, 30
      .parse process.argv

    {@namespace,@internalNamespace,@outsourceJobs,@singleRun,@timeout} = commander
    @redisUri = process.env.REDIS_URI

    @localHandlers = _.difference CommandDispatch.ALL_JOBS, @outsourceJobs
    @remoteHandlers = _.intersection CommandDispatch.ALL_JOBS, @outsourceJobs

  run: =>
    @parseOptions()

    dispatcher = new Dispatcher
      client:  redis.createClient @redisUri
      namespace: @namespace
      timeout:   @timeout
      jobHandlers: @assembleJobHandlers()

    dispatcher.on 'job', (job) =>
      debug 'doing a job: ', JSON.stringify job

    queueWorker = new QueueWorker
      timeout:   @timeout
      namespace: @internalNamespace
      client:    redisMock.createClient(@internalNamespace)
      jobs:      @localHandlers

    if @singleRun
      async.parallel [
        async.apply dispatcher.dispatch
        async.apply queueWorker.run
      ], @tentativePanic
      return

    async.forever queueWorker.run, @panic
    async.forever dispatcher.dispatch, @panic

  assembleJobHandlers: =>
    localClient = redisMock.createClient(@internalNamespace)
    remoteClient = redis.createClient @redisUri

    jobAssembler = new JobAssembler
      timeout: @timeout
      namespace: @internalNamespace
      localClient: localClient
      remoteClient: remoteClient
      localHandlers: @localHandlers
      remoteHandlers: @remoteHandlers

    jobAssembler.on 'response', (response) =>
      debug 'response', JSON.stringify response

    jobAssembler.assemble()

  panic: (error) =>
    console.error error.stack
    process.exit 1

  tentativePanic: (error) =>
    return process.exit(0) unless error?
    console.error error.stack
    process.exit 1

commandDispatch = new CommandDispatch()
commandDispatch.run()
