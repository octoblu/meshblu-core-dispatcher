_                       = require 'lodash'
UUID                    = require 'uuid'
async                   = require 'async'
DispatcherWorker        = require '../../src/dispatcher-worker'
HydrantManager          = require 'meshblu-core-manager-hydrant'
mongojs                 = require 'mongojs'
Redis                   = require 'ioredis'
RedisNS                 = require '@octoblu/redis-ns'
debug                   = require('debug')('meshblu-core-dispatcher:test-dispatcher-worker')
{ JobManagerResponder, JobManagerRequester } = require 'meshblu-core-job-manager'

class TestDispatcherWorker
  constructor: ->
    queueId = UUID.v4()
    @requestQueueName = "test:meshblu:request:#{queueId}"
    @responseQueueName = "test:meshblu:response:#{queueId}"
    @dispatcherWorker = new DispatcherWorker
      namespace:           'meshblu-test'
      timeoutSeconds:      1
      redisUri:            'redis://localhost:6379'
      cacheRedisUri:       'redis://localhost:6379'
      firehoseRedisUri:    'redis://localhost:6379'
      mongoDBUri:          'meshblu-core-test'
      pepper:              'pepper'
      workerName:          'test-worker'
      jobLogRedisUri:      'redis://localhost:6379'
      jobLogQueue:         'sample-rate:1.00'
      jobLogSampleRate:    1
      intervalBetweenJobs: 1
      privateKey:          'private'
      publicKey:           'public'
      singleRun:           true
      ignoreResponse:      false
      requestQueueName:    @requestQueueName

  clearAndGetCollection: (name, callback) =>
    db = mongojs @dispatcherWorker.mongoDBUri
    collection = db.collection name
    collection.drop =>
      callback null, collection

  doSingleRun: (callback) =>
    async.series [
      @_clearDatastoreCache
      @dispatcherWorker.run
    ], callback

  getHydrant: (callback) =>
    @_prepareRedis @dispatcherWorker.firehoseRedisUri, (error, client) =>
      return callback error if error?
      client = new RedisNS 'messages', client
      uuidAliasResolver = @dispatcherWorker.uuidAliasResolver
      @hydrant = new HydrantManager {client, uuidAliasResolver}
      callback null, @hydrant

  generateJobs: (job, callback) =>
    debug 'generateJobs for', job?.metadata?.jobType, job?.metadata?.responseId

    @jobManagerRequester.startProcessing()
    @jobManagerRequester.do job, (error, response) =>
      return callback error if error?

      @_getGeneratedJobs (error, newJobs) =>
        return callback error if error?
        return callback null, [] if _.isEmpty newJobs
        async.mapSeries newJobs, @generateJobs, (error, newerJobs) =>
          return callback(error) if error?
          newerJobs = _.flatten newerJobs
          allJobs = newJobs.concat newerJobs
          @jobManagerRequester.stopProcessing()
          callback null, allJobs

    @doSingleRun (error) => throw error if error?

  prepare: (callback) =>
    async.series [
      @dispatcherWorker.prepare
      @_prepareGeneratorJobManagerRequester
      @_prepareGeneratorJobManagerResponder
      @_clearRequestQueue
    ], callback

  _clearDatastoreCache: (callback) =>
    @_prepareRedis @dispatcherWorker.cacheRedisUri, (error, client) =>
      return callback error if error?
      client.keys 'datastore:*', (error, keys) =>
        return callback error if error?
        return callback() if _.isEmpty keys
        client.del keys..., callback

  _clearRequestQueue: (callback) =>
    @jobManagerRequesterClient.del @requestQueueName, callback

  _getGeneratedJobs: (callback) =>
    requests = []
    @jobManagerRequesterClient.llen @requestQueueName, (error, responseCount) =>
      return callback error if error?

      getJob = (number, callback) =>
        @jobManagerResponder.getRequest (error, request) =>
          return callback error if error?
          requests.push request
          callback()

      async.timesSeries responseCount, getJob, (error) =>
        return callback error if error?
        callback null, requests

  _prepareGeneratorJobManagerRequester: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      @_prepareRedis @dispatcherWorker.redisUri, (error, queueClient) =>
        return callback error if error?
        @jobManagerRequesterClient = new RedisNS @dispatcherWorker.namespace, client
        @jobManagerRequesterQueueClient = new RedisNS @dispatcherWorker.namespace, queueClient
        @jobManagerRequester = new JobManagerRequester
          client: @jobManagerRequesterClient
          queueClient: @jobManagerRequesterQueueClient
          jobTimeoutSeconds: 1
          queueTimeoutSeconds: 1
          jobLogSampleRate: 1
          requestQueueName: @requestQueueName
          responseQueueName: @responseQueueName
        callback()

  _prepareGeneratorJobManagerResponder: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      @_prepareRedis @dispatcherWorker.redisUri, (error, queueClient) =>
        return callback error if error?
        @jobManagerResponderClient = new RedisNS @dispatcherWorker.namespace, client
        @jobManagerResponderQueueClient = new RedisNS @dispatcherWorker.namespace, queueClient
        @jobManagerResponder = new JobManagerResponder
          client: @jobManagerResponderClient
          queueClient: @jobManagerResponderQueueClient
          jobTimeoutSeconds: 1
          queueTimeoutSeconds: 1
          jobLogSampleRate: 1
          requestQueueName: @requestQueueName
        callback()

  _prepareRedis: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client.on 'ready', (error) =>
      return callback error if error?
      callback null, client

    client.on 'error', callback

module.exports = TestDispatcherWorker
