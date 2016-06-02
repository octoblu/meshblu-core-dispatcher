_                 = require 'lodash'
async             = require 'async'
CacheFactory      = require './cache-factory'
DatastoreFactory  = require './datastore-factory'
JobManager        = require 'meshblu-core-job-manager'
mongojs           = require 'mongojs'
Redis             = require 'ioredis'
RedisNS           = require '@octoblu/redis-ns'
TaskJobManager    = require './task-job-manager'
UuidAliasResolver = require 'meshblu-uuid-alias-resolver'

class DispatcherWorker
  constructor: (options) ->
    {
      @namespace
      @timeoutSeconds
      @redisUri
      @mongoDBUri
      @pepper
      @workerName
      @aliasServerUri
      @jobLogRedisUri
      @jobLogQueue
      @jobLogSampleRate
      @intervalBetweenJobs
      @privateKey
      @publicKey
      @singleRun
    } = options
    throw new Error 'DispatcherWorker constructor is missing "@namespace"' unless @namespace?
    throw new Error 'DispatcherWorker constructor is missing "@timeoutSeconds"' unless @timeoutSeconds?
    throw new Error 'DispatcherWorker constructor is missing "@redisUri"' unless @redisUri?
    throw new Error 'DispatcherWorker constructor is missing "@mongoDBUri"' unless @mongoDBUri?
    throw new Error 'DispatcherWorker constructor is missing "@pepper"' unless @pepper?
    throw new Error 'DispatcherWorker constructor is missing "@workerName"' unless @workerName?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogRedisUri"' unless @jobLogRedisUri?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogQueue"' unless @jobLogQueue?
    throw new Error 'DispatcherWorker constructor is missing "@jobLogSampleRate"' unless @jobLogSampleRate?
    throw new Error 'DispatcherWorker constructor is missing "@intervalBetweenJobs"' unless @intervalBetweenJobs?
    throw new Error 'DispatcherWorker constructor is missing "@privateKey"' unless @privateKey?
    throw new Error 'DispatcherWorker constructor is missing "@publicKey"' unless @publicKey?

  panic: (@error) =>
    @stopRunning = true

  run: (callback) =>
    @stopRunning = true if @singleRun

    @_prepare (error) =>
      return callback error if error?
      async.doUntil @_do, @stopRunning, (error) =>
        process.nextTick =>
          return callback error if error?
          return callback @error if @error?
          callback()

  stop: (callback) =>
    @stopRunning = true
    callback()

  _do: (callback) =>
    @jobManager.getRequest ['request'], (error, request) =>
      @jobManager.createResponse request.metadata.responseId, request, callback

  _prepare: (callback) =>
    async.series [
      @_prepareClient
      @_prepareLogClient
      @_prepareMongoDB
      @_prepareCacheFactory
      @_prepareDatastoreFactory
      @_prepareJobManager
      @_prepareTaskJobManager
      @_prepareUuidAliasResolver
    ], callback

  _prepareClient: (callback) =>
    @_prepareRedis @redisUri, (error, @client) =>
      callback error

  _prepareCacheFactory: (callback) =>
    @cacheFactory = new CacheFactory {@client}
    callback()

  _prepareDatastoreFactory: (callback) =>
    @datastoreFactory = new DatastoreFactory {@database, @cacheFactory}
    callback()

  _prepareLogClient: (callback) =>
    @_prepareRedis @jobLogRedisUri, (error, @logClient) =>
      callback error

  _prepareJobManager: (callback) =>
    client = new RedisNS @namespace, @client
    @jobManager = new JobManager {client, @timeoutSeconds, @jobLogSampleRate}
    callback()

  _prepareTaskJobManager: (callback) =>
    client = new RedisNS @namespace, @client
    cache   = new RedisNS 'meshblu-token-one-time', @client
    jobManager = new JobManager {client, @timeoutSeconds, @jobLogSampleRate}
    @taskJobManager = new TaskJobManager {jobManager, cache, @pepper, @uuidAliasResolver}
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
      callback null, client

    client.once 'error', (error) =>
      callback error

  _prepareUuidAliasResolver: (callback) =>
    cache = new RedisNS 'uuid-alias', @client
    @uuidAliasResolver = new UuidAliasResolver {cache, @aliasServerUri}
    callback()

module.exports = DispatcherWorker
