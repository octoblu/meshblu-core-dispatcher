_                 = require 'lodash'
OctobluRaven      = require 'octoblu-raven'
async             = require 'async'
CacheFactory      = require './cache-factory'
DatastoreFactory  = require './datastore-factory'
http              = require 'http'
JobManager        = require 'meshblu-core-job-manager'
JobRegistry       = require './job-registry'
JobLogger         = require 'job-logger'
mongojs           = require 'mongojs'
Redis             = require 'ioredis'
RedisNS           = require '@octoblu/redis-ns'
SimpleBenchmark   = require 'simple-benchmark'
TaskJobManager    = require './task-job-manager'
TaskRunner        = require './task-runner'
UuidAliasResolver = require 'meshblu-uuid-alias-resolver'
debug             = require('debug')('meshblu-core-dispatcher:dispatcher-worker')
{ version }       = require '../package.json'

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
      @jobLogSampleRate
      @privateKey
      @publicKey
      @singleRun
      @ignoreResponse
    } = options
    throw new Error 'DispatcherWorker constructor is missing "@namespace"' unless @namespace?
    throw new Error 'DispatcherWorker constructor is missing "@timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'DispatcherWorker constructor is missing "@redisUri"' unless @redisUri?
    throw new Error 'DispatcherWorker constructor is missing "@cacheRedisUri"' unless @cacheRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@firehoseRedisUri"' unless @firehoseRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@mongoDBUri"' unless @mongoDBUri?
    throw new Error 'DispatcherWorker constructor is missing "@pepper"' unless @pepper?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogRedisUri"' unless @jobLogRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogQueue"' unless @jobLogQueue?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'DispatcherWorker constructor is missing "@privateKey"' unless @privateKey?
    throw new Error 'DispatcherWorker constructor is missing "@publicKey"' unless @publicKey?
    @octobluRaven = new OctobluRaven({ release: version })
    @jobRegistry  = new JobRegistry().toJSON()

  catchErrors: =>
    @octobluRaven.patchGlobal()

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
      @_prepareDatastoreFactory
      @_prepareUuidAliasResolver
      @_prepareJobManager
      @_prepareDispatchLogger
      @_prepareJobLogger
      @_prepareTaskLogger
      @_prepareTaskJobManager
    ], callback

  run: (callback) =>
    @stopRunning = true if @singleRun
    async.doUntil @_doWithNextTick, @_shouldStop, (error) =>
      return callback error if error?
      return callback @error if @error?
      callback()

  stop: (callback) =>
    @stopRunning = true
    callback()

  _doWithNextTick: (callback) =>
    # give some time for garbage collection
    process.nextTick =>
      @_do (error) =>
        process.nextTick =>
          callback error

  _do: (callback) =>
    dispatchBenchmark = new SimpleBenchmark label: 'meshblu-core-dispatcher:dispatch'
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?

      jobBenchmark = new SimpleBenchmark label: 'meshblu-core-dispatcher:job'
      @_logDispatch {dispatchBenchmark, request}, (error) =>
        console.error error.stack if error?
        @_handleRequest request, (error, response) =>
          console.error error.stack if error?
          @_logJob {jobBenchmark, request, response}, (error) =>
            console.error error.stack if error?
            callback()

  _handleRequest: (request, callback) =>
    config = @jobRegistry[request.metadata.jobType]
    return callback new Error "jobType '#{jobType}' not found" unless config?

    taskRunner = new TaskRunner {
      config
      request
      @datastoreFactory
      @pepper
      @cacheFactory
      @uuidAliasResolver
      @workerName
      @privateKey
      @publicKey
      @taskLogger
      @taskJobManager
      @firehoseClient
    }
    taskRunner.run (error, response) =>
      response = @_processErrorResponse {error, request, response}
      debug '_handleRequest, got response:', {request, response}
      @jobManager.createResponse 'response', response, callback

  _logDispatch: ({dispatchBenchmark, request}, callback) =>
    response =
      metadata:
        code: 200
        jobLogs: request.metadata?.jobLogs

    @dispatchLogger.log {request, response, elapsedTime: dispatchBenchmark.elapsed()}, callback

  _logJob: ({jobBenchmark, request, response}, callback) =>
    @jobLogger.log {request, response, elapsedTime: jobBenchmark.elapsed()}, callback

  _prepareClient: (callback) =>
    @_prepareRedis @cacheRedisUri, (error, @client) =>
      callback error

  _prepareCacheFactory: (callback) =>
    @cacheFactory = new CacheFactory {@client}
    callback()

  _prepareDatastoreFactory: (callback) =>
    @datastoreFactory = new DatastoreFactory {@database, @cacheFactory}
    callback()

  _prepareDispatchLogger: (callback) =>
    @dispatchLogger = new JobLogger
      client     : @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher'
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
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:job'
      jobLogQueue: @jobLogQueue
    callback()

  _prepareJobManager: (callback) =>
    @_prepareRedis @redisUri, (error, @jobManagerClient) =>
      return callback error if error?
      client = new RedisNS @namespace, @jobManagerClient
      @jobManager = new JobManager {client, @timeoutSeconds, @jobLogSampleRate}
      callback()

  _prepareTaskJobManager: (callback) =>
    @_prepareRedis @redisUri, (error, client) =>
      return callback error if error?
      cache = new RedisNS 'meshblu-token-one-time', @client # must be the same as the cache client
      client = new RedisNS @namespace, client
      jobManager = new JobManager {client, @timeoutSeconds, @jobLogSampleRate}
      datastore = @datastoreFactory.build 'tokens'
      @taskJobManager = new TaskJobManager {jobManager, cache, datastore, @pepper, @uuidAliasResolver, @ignoreResponse}
      callback()

  _prepareTaskLogger: (callback) =>
    @taskLogger = new JobLogger
      client: @logClient
      indexPrefix: 'metric:meshblu-core-dispatcher-task'
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
    client.once 'ready', =>
      client.on 'error', @panic
      callback null, client

    client.once 'error', callback

  _prepareUuidAliasResolver: (callback) =>
    cache = new RedisNS 'uuid-alias', @client
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
    code = error.code ? 500
    return {
      metadata:
        code: code
        responseId: request.metadata.responseId
        status: http.STATUS_CODES[code]
        error:
          message: error.message
    }

  _shouldStop: =>
    return @stopRunning ? false

module.exports = DispatcherWorker
