_                = require 'lodash'
commander        = require 'commander'
async            = require 'async'
redis            = require 'redis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-dispatcher:command')
packageJSON      = require './package.json'
CacheFactory     = require './src/cache-factory'
DatastoreFactory = require './src/datastore-factory'
Dispatcher       = require './src/dispatcher'
JobAssembler     = require './src/job-assembler'
JobRegistry      = require './src/job-registry'
QueueWorker      = require './src/queue-worker'

class CommandDispatch
  @ALL_JOBS: ['Authenticate', 'Idle']

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
      .option '-t, --timeout <30>', 'seconds to wait for a next job.', parseInt, 30
      .parse process.argv

    {@namespace,@internalNamespace,@outsourceJobs,@singleRun,@timeout} = commander
    @redisUri   = process.env.REDIS_URI
    @mongoDBUri = process.env.MONGODB_URI
    @pepper     = process.env.TOKEN

    @localHandlers = _.difference CommandDispatch.ALL_JOBS, @outsourceJobs
    @remoteHandlers = _.intersection CommandDispatch.ALL_JOBS, @outsourceJobs

  run: =>
    @parseOptions()

    dispatcher = new Dispatcher
      client:  @getDispatchClient()
      timeout:   @timeout
      jobHandlers: @assembleJobHandlers()

    dispatcher.on 'job', (job) =>
      debug 'doing a job: ', JSON.stringify job

    queueWorker = new QueueWorker
      pepper:    @pepper
      timeout:   @timeout
      jobs:      @localHandlers
      client:    @getLocalClient()
      jobRegistry:  (new JobRegistry).toJSON()
      cacheFactory:     new CacheFactory client: redis.createClient(@redisUri)
      datastoreFactory: new DatastoreFactory database: @mongoDBUri

    if @singleRun
      async.parallel [
        async.apply dispatcher.dispatch
        async.apply queueWorker.run
      ], @tentativePanic
      return

    async.forever queueWorker.run, @panic
    async.forever dispatcher.dispatch, @panic

  assembleJobHandlers: =>
    jobAssembler = new JobAssembler
      timeout: @timeout
      localClient: @getLocalClient()
      remoteClient: @getRemoteClient()
      localHandlers: @localHandlers
      remoteHandlers: @remoteHandlers

    jobAssembler.on 'response', (response) =>
      debug 'response', JSON.stringify response

    jobAssembler.assemble()

  getDispatchClient: =>
    new RedisNS @namespace, redis.createClient(@redisUri)

  getLocalClient: =>
    new RedisNS @internalNamespace, redis.createClient @redisUri

  getRemoteClient: =>
    new RedisNS @internalNamespace, redis.createClient @redisUri

  panic: (error) =>
    console.error error.stack
    process.exit 1

  tentativePanic: (error) =>
    return process.exit(0) unless error?
    console.error error.stack
    process.exit 1

commandDispatch = new CommandDispatch()
commandDispatch.run()
