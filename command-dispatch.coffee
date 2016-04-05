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
    @database.on 'error', @panic

    return @doSingleRun @closeAndTentativePanic if @singleRun
    async.until @terminated, @runDispatcher, @closeAndTentativePanic
    async.until @terminated, @runQueueWorker, @closeAndTentativePanic

  doSingleRun: (callback) =>
    async.parallel [
      async.apply @runDispatcher
      async.apply @runQueueWorker
    ], callback

  runDispatcher: (callback) =>
    dispatcher = new Dispatcher
      client:  @getDispatchClient()
      timeout:   @timeout
      jobHandlers: @assembleJobHandlers()
      logJobs: @logJobs
      workerName: @workerName
      dispatchLogger: @getDispatchLogger()
      jobLogger: @getJobLogger()

    dispatcher.dispatch callback

  runQueueWorker: (callback) =>
    queueWorker = new QueueWorker
      aliasServerUri:      @aliasServerUri
      pepper:              @pepper
      privateKey:          @privateKey
      publicKey:           @publicKey
      timeout:             @timeout
      jobs:                @localHandlers
      client:              @getLocalQueueWorkerClient()
      jobRegistry:         @getJobRegistry()
      cacheFactory:        @getCacheFactory()
      datastoreFactory:    @getDatastoreFactory()
      meshbluConfig:       @meshbluConfig
      forwardEventDevices: @forwardEventDevices
      externalClient:      @getTaskJobManagerClient()
      logJobs:             @logJobs
      workerName:          @workerName
      taskLogger:          @getTaskLogger()

    queueWorker.run callback

  assembleJobHandlers: =>
    return @assembledJobHandlers if @assembledJobHandlers?

    jobAssembler = new JobAssembler
      timeout: @timeout
      localClient: @getLocalJobHandlerClient()
      remoteClient: @getRemoteJobHandlerClient()
      localHandlers: @localHandlers
      remoteHandlers: @remoteHandlers

    @assembledJobHandlers = jobAssembler.assemble()

  getCacheFactory: =>
    @cacheFactory ?= new CacheFactory client: redis.createClient @redisUri
    @cacheFactory

  getDatastoreFactory: =>
    @datastoreFactory ?= new DatastoreFactory database: @database
    @datastoreFactory

  getDispatchClient: =>
    @dispatchClient ?= _.bindAll new RedisNS @namespace, redis.createClient @redisUri
    @dispatchClient

  getDispatchLogger: =>
    @dispatchLogger ?= new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @dispatchLogger

  getJobLogger: =>
    @jobLogger ?= new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:job'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate
    @jobLogger

  getJobRegistry: =>
    @jobRegistry ?= (new JobRegistry).toJSON()
    @jobRegistry

  getLogClient: =>
    @logClient ?= redis.createClient @jobLogRedisUri
    @logClient

  getLocalJobHandlerClient: =>
    @localJobHandlerClient ?= _.bindAll new RedisNS @internalNamespace, redis.createClient @redisUri
    @localJobHandlerClient

  getLocalQueueWorkerClient: =>
    @localQueueWorkerClient ?= _.bindAll new RedisNS @internalNamespace, redis.createClient @redisUri
    @localQueueWorkerClient

  getRemoteJobHandlerClient: =>
    @remoteClient ?= _.bindAll new RedisNS @internalNamespace, redis.createClient @redisUri
    @remoteClient

  getTaskJobManagerClient: =>
    @taskJobManagerClient ?= _.bindAll new RedisNS @namespace, redis.createClient @redisUri
    @taskJobManagerClient

  getTaskLogger: =>
    return @taskLogger if @taskLogger?

    jobLogger = new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:task'
      jobLogQueue: @jobLogQueue
      sampleRate: @jobLogSampleRate

    deviceLogger = new DeviceLogger
      client: @getLogClient()
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
