_                = require 'lodash'
commander        = require 'commander'
async            = require 'async'
mongojs          = require 'mongojs'
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
  @ALL_JOBS: [
    'Authenticate'
    'GetDevice'
    'Idle'
    'SendMessage'
    'SubscriptionList'
    'UpdateDevice'
  ]

  parseInt: (int) =>
    parseInt int

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
      .option '-t, --timeout <15>', 'seconds to wait for a next job.', @parseInt, 15
      .parse process.argv

    {@namespace,@internalNamespace,@outsourceJobs,@singleRun,@timeout} = commander
    @redisUri   = process.env.REDIS_URI
    @mongoDBUri = process.env.MONGODB_URI
    @pepper     = process.env.TOKEN
    @aliasServerUri = process.env.ALIAS_SERVER_URI

    @localHandlers = _.difference CommandDispatch.ALL_JOBS, @outsourceJobs
    @remoteHandlers = _.intersection CommandDispatch.ALL_JOBS, @outsourceJobs

  run: =>
    @parseOptions()

    process.on 'SIGTERM', =>
      console.error 'exiting...'
      @terminate = true

    return @doSingleRun @tentativePanic if @singleRun
    async.until @terminated, @runDispatcher, @tentativePanic
    async.until @terminated, @runQueueWorker, @tentativePanic

  doSingleRun: (callback) =>
    @runDispatcher callback
    @runQueueWorker =>

  runDispatcher: (callback) =>
    dispatcher = new Dispatcher
      client:  @getDispatchClient()
      timeout:   @timeout
      jobHandlers: @assembleJobHandlers()

    dispatcher.dispatch callback

  runQueueWorker: (callback) =>
    queueWorker = new QueueWorker
      aliasServerUri: @aliasServerUri
      pepper:    @pepper
      timeout:   @timeout
      jobs:      @localHandlers
      client:    @getLocalQueueWorkerClient()
      jobRegistry:  (new JobRegistry).toJSON()
      cacheFactory:     @getCacheFactory()
      datastoreFactory: @getDatastoreFactory()

    queueWorker.run callback

  assembleJobHandlers: =>
    jobAssembler = new JobAssembler
      timeout: @timeout
      localClient: @getLocalJobHandlerClient()
      remoteClient: @getRemoteJobHandlerClient()
      localHandlers: @localHandlers
      remoteHandlers: @remoteHandlers

    jobAssembler.assemble()

  getCacheFactory: =>
    @cacheFactory ?= new CacheFactory client: redis.createClient @redisUri
    @cacheFactory

  getDatastoreFactory: =>
    @datastoreFactory ?= new DatastoreFactory database: mongojs @mongoDBUri
    @datastoreFactory

  getDispatchClient: =>
    @dispatchClient ?= new RedisNS @namespace, redis.createClient @redisUri
    @dispatchClient

  getLocalJobHandlerClient: =>
    @localJobHandlerClient ?= new RedisNS @internalNamespace, redis.createClient @redisUri
    @localJobHandlerClient

  getLocalQueueWorkerClient: =>
    @localQueueWorkerClient ?= new RedisNS @internalNamespace, redis.createClient @redisUri
    @localQueueWorkerClient

  getRemoteJobHandlerClient: =>
    @remoteClient ?= new RedisNS @internalNamespace, redis.createClient @redisUri
    @remoteClient

  panic: (error) =>
    console.error error.stack
    process.exit 1

  tentativePanic: (error) =>
    return process.exit(0) unless error?
    console.error error.stack
    process.exit 1

  terminated: => @terminate

commandDispatch = new CommandDispatch()
commandDispatch.run()
