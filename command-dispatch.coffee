_                = require 'lodash'
commander        = require 'commander'
async            = require 'async'
MeshbluConfig    = require 'meshblu-config'
mongojs          = require 'mongojs'
redis            = require 'ioredis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-dispatcher:command')
packageJSON      = require './package.json'
CacheFactory     = require './src/cache-factory'
DatastoreFactory = require './src/datastore-factory'
Dispatcher       = require './src/dispatcher'
JobAssembler     = require './src/job-assembler'
JobRegistry      = require './src/job-registry'
QueueWorker      = require './src/queue-worker'
ArrayLogger      = require './src/array-logger'
DeviceLogger     = require './src/device-logger'
JobLogger        = require 'job-logger'

class CommandDispatch
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
      .option '-o, --outsource-jobs <job1,job2>', 'jobs for external workers', ''
      .option '-s, --single-run', 'perform only one job.'
      .option '-t, --timeout <15>', 'seconds to wait for a next job.', @parseInt, 15
      .parse process.argv

    {@singleRun} = commander
    @redisUri            = process.env.REDIS_URI
    @mongoDBUri          = process.env.MONGODB_URI
    @pepper              = process.env.TOKEN
    @aliasServerUri      = process.env.ALIAS_SERVER_URI
    @logJobs             = process.env.LOG_JOBS == 'true'
    @namespace           = process.env.NAMESPACE || commander.namespace
    @internalNamespace   = process.env.INTERNAL_NAMESPACE || commander.internalNamespace
    @outsourceJobs       = @parseList(process.env.OUTSOURCE_JOBS || commander.outsourceJobs)
    @timeout             = @parseInt(process.env.TIMEOUT || commander.timeout)
    @workerName          = process.env.WORKER_NAME
    @jobLogRedisUri      = process.env.JOB_LOG_REDIS_URI
    @jobLogQueue         = process.env.JOB_LOG_QUEUE
    @jobLogSampleRate    = process.env.JOB_LOG_SAMPLE_RATE
    @deviceLogQueue      = process.env.DEVICE_LOG_QUEUE
    @deviceLogSampleRate = process.env.DEVICE_LOG_SAMPLE_RATE
    @deviceLogUuid       = process.env.DEVICE_LOG_UUID
    @intervalBetweenJobs = parseInt(process.env.INTERVAL_BETWEEN_JOBS || 0)

    unless @redisUri?
      throw new Error 'Missing mandatory parameter: REDIS_URI'

    unless @mongoDBUri?
      throw new Error 'Missing mandatory parameter: MONGODB_URI'

    unless @jobLogRedisUri?
      throw new Error 'Missing mandatory parameter: JOB_LOG_REDIS_URI'

    unless @jobLogQueue?
      throw new Error 'Missing mandatory parameter: JOB_LOG_QUEUE'

    unless @jobLogSampleRate?
      throw new Error 'Missing mandatory parameter: JOB_LOG_SAMPLE_RATE'

    unless @pepper?
      throw new Error 'Missing mandatory parameter: TOKEN'

    @jobLogSampleRate = parseFloat @jobLogSampleRate
    @deviceLogSampleRate = parseFloat @deviceLogSampleRate

    if process.env.PRIVATE_KEY_BASE64? && process.env.PRIVATE_KEY_BASE64 != ''
      @privateKey = new Buffer(process.env.PRIVATE_KEY_BASE64, 'base64').toString('utf8')

    if process.env.PUBLIC_KEY_BASE64? && process.env.PUBLIC_KEY_BASE64 != ''
      @publicKey = new Buffer(process.env.PUBLIC_KEY_BASE64, 'base64').toString('utf8')

    allJobs = _.keys @getJobRegistry()
    @localHandlers = _.difference allJobs, @outsourceJobs
    @remoteHandlers = _.intersection allJobs, @outsourceJobs
    @meshbluConfig = new MeshbluConfig().toJSON()

  run: =>
    @parseOptions()

    process.on 'SIGTERM', =>
      console.error 'exiting...'
      @terminate = true

    @database = mongojs @mongoDBUri

    # ensure we are connected to mongo before proceeding
    @database.runCommand {ping: 1}, (error) =>
      @panic error if error?

      @database.on 'error', @panic

      setInterval =>
        @database.runCommand {ping: 1}, (error) =>
          @panic error if error?
      , @timeout * 1000

      @prepareConnections (error) =>
        @panic error if error?

        return @doSingleRun @closeAndTentativePanic if @singleRun
        async.until @terminated, @runDispatcher, @closeAndTentativePanic
        async.until @terminated, @runQueueWorker, @closeAndTentativePanic

  doSingleRun: (callback) =>
    _.delay @_doSingleRun, @intervalBetweenJobs, callback

  _doSingleRun: (callback) =>
    async.parallel [
      async.apply @runDispatcher
      async.apply @runQueueWorker
    ], (error) =>

      callback error

  prepareConnections: (callback) =>
    async.series [
      async.apply @prepareCacheFactory
      async.apply @prepareDispatchClient
      async.apply @prepareLogClient
      async.apply @prepareLocalJobHandlerClient
      async.apply @prepareLocalQueueWorkerClient
      async.apply @prepareRemoteJobHandlerClient
      async.apply @prepareTaskJobManagerClient
    ], callback

  getReadyRedis: (redisUri, callback) =>
    callback = _.once callback
    client = redis.createClient redisUri
    client.once 'ready', =>
      callback null, client
    client.once 'error', (error) =>
      callback error

  prepareCacheFactory: (callback) =>
    @getReadyRedis @redisUri, (error, client) =>
      return callback error if error?
      @cacheFactory = new CacheFactory {client}
      callback()

  prepareDispatchClient: (callback) =>
    @getReadyRedis @redisUri, (error, client) =>
      return callback error if error?
      @dispatchClient = new RedisNS @namespace, client
      callback()

  prepareLogClient: (callback) =>
    @getReadyRedis @jobLogRedisUri, (error, client) =>
      return callback error if error?
      @logClient = client
      callback()

  prepareLocalJobHandlerClient: (callback) =>
    @getReadyRedis @redisUri, (error, client) =>
      return callback error if error?
      @localJobHandlerClient = new RedisNS @internalNamespace, client
      callback()

  prepareLocalQueueWorkerClient: (callback) =>
    @getReadyRedis @redisUri, (error, client) =>
      return callback error if error?
      @localQueueWorkerClient = new RedisNS @internalNamespace, client
      callback()

  prepareRemoteJobHandlerClient: (callback) =>
    @getReadyRedis @redisUri, (error, client) =>
      return callback error if error?
      @remoteJobHandlerClient = new RedisNS @internalNamespace, client
      callback()

  prepareTaskJobManagerClient: (callback) =>
    @getReadyRedis @redisUri, (error, client) =>
      return callback error if error?
      @taskJobManagerClient = new RedisNS @namespace, client
      callback()

  getDatastoreFactory: =>
    @datastoreFactory ?= new DatastoreFactory database: @database, cacheFactory: @cacheFactory
    @datastoreFactory

  runDispatcher: (callback) =>
    dispatcher = new Dispatcher
      client:              @dispatchClient
      timeout:             @timeout
      jobHandlers:         @assembleJobHandlers()
      logJobs:             @logJobs
      workerName:          @workerName
      dispatchLogger:      @getDispatchLogger()
      memoryLogger:        @getMemoryLogger()
      createPopLogger:     @getCreatePopLogger()
      createRespondLogger: @getCreateRespondLogger()
      jobLogger:           @getJobLogger()

    dispatcher.dispatch callback

  runQueueWorker: (callback) =>
    queueWorker = new QueueWorker
      aliasServerUri:      @aliasServerUri
      pepper:              @pepper
      privateKey:          @privateKey
      publicKey:           @publicKey
      timeout:             @timeout
      jobs:                @localHandlers
      client:              @localQueueWorkerClient
      jobRegistry:         @getJobRegistry()
      cacheFactory:        @cacheFactory
      datastoreFactory:    @getDatastoreFactory()
      meshbluConfig:       @meshbluConfig
      forwardEventDevices: @forwardEventDevices
      externalClient:      @taskJobManagerClient
      logJobs:             @logJobs
      workerName:          @workerName
      taskLogger:          @getTaskLogger()

    queueWorker.run callback

  assembleJobHandlers: =>
    return @assembledJobHandlers if @assembledJobHandlers?

    jobAssembler = new JobAssembler
      timeout: @timeout
      localClient: @localJobHandlerClient
      remoteClient: @remoteJobHandlerClient
      localHandlers: @localHandlers
      remoteHandlers: @remoteHandlers

    @assembledJobHandlers = jobAssembler.assemble()

  getDispatchLogger: =>
    @dispatchLogger ?= new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @dispatchLogger

  getMemoryLogger: =>
    @memoryLogger ?= new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher-memory'
      type: 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @memoryLogger

  getCreatePopLogger: =>
    @createPopLogger ?= new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu'
      type: 'create-pop'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @createPopLogger

  getCreateRespondLogger: =>
    @createRespondLogger ?= new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu'
      type: 'create-respond'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @createRespondLogger

  getJobLogger: =>
    @jobLogger ?= new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:job'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @jobLogger

  getJobRegistry: =>
    @jobRegistry ?= (new JobRegistry).toJSON()
    @jobRegistry

  getTaskLogger: =>
    return @taskLogger if @taskLogger?

    jobLogger = new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:task'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate

    deviceLogger = new DeviceLogger
      client: @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher-device'
      type: 'meshblu-core-dispatcher:task'
      jobLogQueue: @deviceLogQueue
      sampleRate: @deviceLogSampleRate
      filterUuid: @deviceLogUuid

    @taskLogger = new ArrayLogger loggers: [jobLogger, deviceLogger]
    return @taskLogger

  panic: (error) =>
    console.error error.stack
    process.exit 1

  closeAndTentativePanic: (error) =>
    return @tentativePanic error unless @database?
    @database.close (dbError) =>
      debug 'closed database'
      return @tentativePanic error if error?
      @tentativePanic dbError

  tentativePanic: (error) =>
    return process.exit(0) unless error?
    console.error error.stack
    process.exit 1

  terminated: => @terminate

commandDispatch = new CommandDispatch()
commandDispatch.run()
