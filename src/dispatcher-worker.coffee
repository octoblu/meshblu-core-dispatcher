_                       = require 'lodash'
OctobluRaven            = require 'octoblu-raven'
async                   = require 'async'
http                    = require 'http'
JobLogger               = require 'job-logger'
mongojs                 = require 'mongojs'
Redis                   = require 'ioredis'
RedisNS                 = require '@octoblu/redis-ns'
SimpleBenchmark         = require 'simple-benchmark'
UuidAliasResolver       = require 'meshblu-uuid-alias-resolver'
debug                   = require('debug')('meshblu-core-dispatcher:dispatcher-worker')
debugBenchmark          = require('debug')('meshblu-core-dispatcher:dispatcher-worker:benchmark')

CacheFactory            = require './cache-factory'
RedisFactory            = require './cache-factory'
DatastoreFactory        = require './datastore-factory'
JobRegistry             = require './job-registry'
TaskJobManager          = require './task-job-manager'
TaskRunner              = require './task-runner'

{ JobManagerResponder, JobManagerRequester } = require 'meshblu-core-job-manager'

class DispatcherWorker
  constructor: (options) ->
    {
      @namespace
      @timeoutSeconds
      @redisUri
      @cacheRedisUri
      @firehoseRedisUri
      @mongoDBUri
      @pepper
      @workerName
      @aliasServerUri
      @jobLogRedisUri
      @jobLogQueue
      @jobLogNamespace
      @jobLogSampleRate
      @jobLogSampleRateOverrideUuids
      @privateKey
      @publicKey
      @singleRun
      @ignoreResponse
      @requestQueueName
      @responseQueueName
      @datastoreCacheTTL
      @concurrency
    } = options
    throw new Error 'DispatcherWorker constructor is missing "@namespace"'        unless @namespace?
    throw new Error 'DispatcherWorker constructor is missing "@timeoutSeconds"'   unless @timeoutSeconds?
    throw new Error 'DispatcherWorker constructor is missing "@redisUri"'         unless @redisUri?
    throw new Error 'DispatcherWorker constructor is missing "@cacheRedisUri"'    unless @cacheRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@firehoseRedisUri"' unless @firehoseRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@mongoDBUri"'       unless @mongoDBUri?
    throw new Error 'DispatcherWorker constructor is missing "@pepper"'           unless @pepper?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogRedisUri"'   unless @jobLogRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogQueue"'      unless @jobLogQueue?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'DispatcherWorker constructor is missing "@privateKey"'       unless @privateKey?
    throw new Error 'DispatcherWorker constructor is missing "@publicKey"'        unless @publicKey?
    throw new Error 'DispatcherWorker constructor is missing "@requestQueueName"' unless @requestQueueName?
    throw new Error 'DispatcherWorker constructor is missing "@concurrency"'      unless @concurrency?
    @octobluRaven = new OctobluRaven
    @jobRegistry  = new JobRegistry().toJSON()

    @indexJobPrefix = "metric:meshblu-core-dispatcher"
    @indexJobPrefix = "#{@indexJobPrefix}:#{@jobLogNamespace}" unless _.isEmpty @jobLogNamespace

    @indexTaskPrefix = "metric:meshblu-core-dispatcher-task"
    @indexTaskPrefix = "#{@indexTaskPrefix}:#{@jobLogNamespace}" unless _.isEmpty @jobLogNamespace

  catchErrors: =>

  reportError: =>
    @octobluRaven.reportError arguments...

  panic: (@error) =>
    console.error "PANIC:", @error.stack
    @reportError @error
    @stopRunning = true
    @_disconnect()
    setTimeout =>
      process.exit 1
    , 1000

  _disconnect: =>
    @database?.close _.noop

  prepare: (callback) =>
    # order is important
    async.series [
      @_prepareClient
      @_prepareLogClient
      @_prepareFirehoseClient
      @_prepareMongoDB
      @_prepareCacheFactory
      @_prepareRedisFactory
      @_prepareDatastoreFactory
      @_prepareUuidAliasResolver
      @_prepareJobManager
      @_prepareDispatchLogger
      @_prepareJobLogger
      @_prepareTaskLogger
      @_prepareTaskJobManager
    ], callback

  run: (callback) =>
    @jobManager.start callback

  stop: (callback) =>
    @stopRunning = true
    return callback() unless @taskJobManager?
    return callback() unless @jobManager?
    @taskJobManager.stop()
    @jobManager.stop(callback)

  do: (request, callback) =>
    dispatchBenchmark = new SimpleBenchmark label: 'meshblu-core-dispatcher:dispatch'
    return unless request?
    jobBenchmark = new SimpleBenchmark label: 'meshblu-core-dispatcher:job'
    @_logDispatch {dispatchBenchmark, request}, (logError) =>
      console.error logError.stack if logError?
      @_handleRequest request, (error, response) =>
        console.error error.stack if error?
        @_logJob {jobBenchmark, request, response}, (logError) =>
          console.error logError.stack if logError?
          jobType = _.get(request, 'metadata.jobType')
          responseCode = _.get(response, 'metadata.code')
          debugBenchmark("#{jobType}[#{responseCode}] #{jobBenchmark.elapsed()}ms (dispatch #{dispatchBenchmark.elapsed()}ms)")
          callback error, response

  _handleRequest: (request, callback) =>
    config = @jobRegistry[request.metadata.jobType]
    return callback new Error "jobType '#{request.metadata.jobType}' not found" unless config?

    taskRunner = new TaskRunner {
      config
      request
      @datastoreFactory
      @pepper
      @cacheFactory
      @redisFactory
      @uuidAliasResolver
      @workerName
      @privateKey
      @publicKey
      @taskLogger
      @taskJobManager
      @firehoseClient
      @timeoutSeconds
    }
    taskRunner.run (error, response) =>
      response = @_processErrorResponse {error, request, response}
      debug '_handleRequest, got response:', {request, response}
      callback null, response

  _logDispatch: ({dispatchBenchmark, request}, callback) =>
    @dispatchLogger.log {
      elapsedTime: dispatchBenchmark.elapsed()
      request:
        metadata:
          jobType: 'Idle'
          workerName: @workerName
      response:
        metadata:
          code: 0
          jobLogs: request.metadata?.jobLogs
    }, callback

  _logJob: ({jobBenchmark, request, response}, callback) =>
    @jobLogger.log {request, response, elapsedTime: jobBenchmark.elapsed()}, callback

  _prepareClient: (callback) =>
    @_prepareRedis @cacheRedisUri, (error, @cacheClient) =>
      return callback error if error?
      @_prepareRedis @redisUri, (error, @redisClient) =>
        return callback error if error?
        callback null

  _prepareCacheFactory: (callback) =>
    @cacheFactory = new CacheFactory {client: @cacheClient}
    callback()

  _prepareRedisFactory: (callback) =>
    @redisFactory = new RedisFactory {client: @redisClient}
    callback()

  _prepareDatastoreFactory: (callback) =>
    @datastoreFactory = new DatastoreFactory {@database, @cacheFactory, @datastoreCacheTTL}
    callback()

  _prepareDispatchLogger: (callback) =>
    @dispatchLogger = new JobLogger
      client     : @logClient
      indexPrefix: @indexJobPrefix
      type       : 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: @jobLogQueue
    callback()

  _prepareFirehoseClient: (callback) =>
    @_prepareRedis @firehoseRedisUri, (error, @firehoseClient) =>
      callback error

  _prepareLogClient: (callback) =>
    @_prepareRedis @jobLogRedisUri, (error, @logClient) =>
      callback error

  _prepareJobLogger: (callback) =>
    @jobLogger = new JobLogger
      client: @logClient
      indexPrefix: @indexJobPrefix
      type: 'meshblu-core-dispatcher:job'
      jobLogQueue: @jobLogQueue
    callback()

  _prepareJobManager: (callback) =>
    @_prepareRedis @redisUri, (error, @jobManagerClient) =>
      return callback error if error?
      @_prepareRedis @redisUri, (error, @jobManagerQueueClient) =>
        return callback error if error?

        @jobManager = new JobManagerResponder {
          @redisUri
          @namespace
          @concurrency
          maxConnections: @concurrency
          jobTimeoutSeconds: @timeoutSeconds
          queueTimeoutSeconds: @timeoutSeconds
          @jobLogSampleRate
          @requestQueueName
          workerFunc: @do
        }

        callback()

  _prepareTaskJobManager: (callback) =>
    cache = new RedisNS 'meshblu-token-one-time', @cacheClient # must be the same as the cache client
    jobManager = new JobManagerRequester {
      @redisUri
      @namespace
      maxConnections: @concurrency
      jobTimeoutSeconds: @timeoutSeconds
      queueTimeoutSeconds: @timeoutSeconds
      @jobLogSampleRate
      @jobLogSampleRateOverrideUuids
      @requestQueueName
      responseQueueName: "v2:meshblu:task:response:queue"
    }

    datastore = @datastoreFactory.build 'tokens'
    @taskJobManager = new TaskJobManager {jobManager, cache, datastore, @pepper, @uuidAliasResolver, @ignoreResponse}
    @taskJobManager.start (error) =>
      @taskJobManager._stopProcessing =>
      callback error

  _prepareTaskLogger: (callback) =>
    @taskLogger = new JobLogger
      client: @logClient
      indexPrefix: @indexTaskPrefix
      type: 'meshblu-core-dispatcher:task'
      jobLogQueue: @jobLogQueue
    callback()

  _prepareMongoDB: (callback) =>
    @database = mongojs @mongoDBUri
    @database.runCommand {ping: 1}, (error) =>
      return callback error if error?

      setInterval =>
        @database.runCommand {ping: 1}, (error) =>
          @panic error if error?
      , @timeoutSeconds * 1000

      callback()

  _prepareRedis: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client = _.bindAll client, _.functionsIn(client)
    client.ping (error) =>
      return callback error if error?
      client.once 'error', @panic
      callback null, client

  _prepareUuidAliasResolver: (callback) =>
    cache = new RedisNS 'uuid-alias', @cacheClient
    @uuidAliasResolver = new UuidAliasResolver {cache, @aliasServerUri}
    callback()

  _processResponse: ({request, response}) =>
    {metadata, data, rawData} = response
    metadata.responseId = request.metadata.responseId
    rawData = JSON.stringify(data) if data?
    return {metadata, rawData}

  _processErrorResponse: ({error, request, response}) =>
    return @_processResponse {request, response} unless error?
    @reportError error, { request, response }
    code = error.code
    code = 504 if code == 'ETIMEDOUT'
    code = 500 unless http.STATUS_CODES[code]?
    return {
      metadata:
        code: code
        responseId: request.metadata.responseId
        status: http.STATUS_CODES[code]
        error:
          message: error.message
    }
module.exports = DispatcherWorker
