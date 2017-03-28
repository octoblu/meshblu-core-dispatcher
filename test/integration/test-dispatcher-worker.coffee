_                       = require 'lodash'
UUID                    = require 'uuid'
async                   = require 'async'
DispatcherWorker        = require '../../src/dispatcher-worker'
HydrantManager          = require 'meshblu-core-manager-hydrant'
mongojs                 = require 'mongojs'
Redis                   = require 'ioredis'
RedisNS                 = require '@octoblu/redis-ns'
{ JobManagerResponder, JobManagerRequester } = require 'meshblu-core-job-manager'

class TestDispatcherWorker
  constructor: ->
    queueId = UUID.v4()
    @requestQueueName = "test:meshblu:request:#{queueId}"
    @responseQueueName = "test:meshblu:response:#{queueId}"
    @namespace = 'ns'
    @redisUri = 'redis://localhost'
    @dispatcherWorker = new DispatcherWorker
      namespace:           @namespace
      timeoutSeconds:      1
      redisUri:            @redisUri
      cacheRedisUri:       @redisUri
      firehoseRedisUri:    @redisUri
      mongoDBUri:          'meshblu-core-test'
      pepper:              'pepper'
      workerName:          'test-worker'
      jobLogRedisUri:      @redisUri
      jobLogQueue:         'sample-rate:1.00'
      jobLogSampleRate:    0
      intervalBetweenJobs: 1
      privateKey:          'private'
      publicKey:           'public'
      ignoreResponse:      false
      requestQueueName:    @requestQueueName

  clearAndGetCollection: (name, callback) =>
    db = mongojs @dispatcherWorker.mongoDBUri
    collection = db.collection name
    collection.drop =>
      callback null, collection

  getHydrant: (callback) =>
    @_prepareRedis @dispatcherWorker.firehoseRedisUri, (error, client) =>
      return callback error if error?
      client = new RedisNS 'messages', client
      uuidAliasResolver = @dispatcherWorker.uuidAliasResolver
      @hydrant = new HydrantManager {client, uuidAliasResolver}
      callback null, @hydrant

  start: (callback) =>
    async.series [
      @dispatcherWorker.prepare
      @_prepareClient
      @_prepareGeneratorJobManagerRequester
      @_prepareGeneratorJobManagerResponder
      @_clearDatastoreCache
    ], (error) =>
      return callback error if error?
      @dispatcherWorker.run =>
      callback()

  stop: (callback) =>
    @dispatcherWorker.stop =>
      @client.quit =>
        @jobManagerResponder.stop =>
          @jobManagerRequester.stop callback

  _clearDatastoreCache: (callback) =>
    @_prepareRedis @dispatcherWorker.cacheRedisUri, (error, client) =>
      return callback error if error?
      client.keys 'datastore:*', (error, keys) =>
        return callback error if error?
        return callback() if _.isEmpty keys
        client.del keys..., callback

  _prepareClient: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, @client) =>
      callback error

  _prepareGeneratorJobManagerRequester: (callback) =>
    @jobManagerRequester = new JobManagerRequester {
      @namespace
      @redisUri
      maxConnections: 1
      jobTimeoutSeconds: 1
      queueTimeoutSeconds: 1
      jobLogSampleRate: 0
      requestQueueName: @requestQueueName
      responseQueueName: @responseQueueName
    }
    @jobManagerRequester.start callback

  _prepareGeneratorJobManagerResponder: (callback) =>
    @jobManagerResponder = new JobManagerResponder {
      @namespace
      @redisUri
      maxConnections: 1
      jobTimeoutSeconds: 1
      queueTimeoutSeconds: 1
      jobLogSampleRate: 0
      requestQueueName: @requestQueueName
    }
    @jobManagerResponder.start callback

  _prepareRedis: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client = _.bindAll client, _.functionsIn(client)
    client.ping (error) =>
      return callback error if error?
      client.once 'error', @stop
      callback null, client
    return # redis fix

module.exports = TestDispatcherWorker
