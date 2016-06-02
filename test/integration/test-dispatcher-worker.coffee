_                = require 'lodash'
async            = require 'async'
DispatcherWorker = require '../../src/dispatcher-worker'
HydrantManager   = require 'meshblu-core-manager-hydrant'
JobManager       = require 'meshblu-core-job-manager'
mongojs          = require 'mongojs'
Redis            = require 'ioredis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-dispatcher:test-dispatcher-worker')

class TestDispatcherWorker
  constructor: ->
    @dispatcherWorker = new DispatcherWorker
      namespace:           'meshblu-test'
      timeoutSeconds:      1
      redisUri:            'redis://localhost:6379'
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
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      client = new RedisNS 'messages', client
      uuidAliasResolver = @dispatcherWorker.uuidAliasResolver
      @hydrant = new HydrantManager {client, uuidAliasResolver}
      callback null, @hydrant

  generateJobs: (job, callback) =>
    debug 'generateJobs for', job?.metadata?.jobType, job?.metadata?.responseId

    @jobManager.do 'request', 'response', job, (error, response) =>
      return callback error if error?

      @_getGeneratedJobs (error, newJobs) =>
        return callback error if error?
        return callback null, [] if _.isEmpty newJobs
        async.mapSeries newJobs, @generateJobs, (error, newerJobs) =>
          return callback(error) if error?
          newerJobs = _.flatten newerJobs
          allJobs = newJobs.concat newerJobs
          callback null, allJobs

    @doSingleRun (error) => throw error if error?

  getJobManager: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      jobManager = new JobManager
        client: client
        timeoutSeconds: 1
        jobLogSampleRate: 0
      callback null, jobManager

  prepare: (callback) =>
    async.series [
      @dispatcherWorker.prepare
      @_prepareClient
      @_prepareGeneratorJobManager
      @_clearRequestQueue
    ], callback

  _clearDatastoreCache: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      client.keys 'datastore:*', (error, keys) =>
        return callback error if error?
        return callback() if _.isEmpty keys
        client.del keys..., callback

  _clearRequestQueue: (callback) =>
    @client.del 'request:queue', callback

  _getGeneratedJobs: (callback) =>
    requests = []
    @client.llen 'request:queue', (error, responseCount) =>
      return callback error if error?

      getJob = (number, callback) =>
        @jobManager.getRequest ['request'], (error, request) =>
          return callback error if error?
          requests.push request
          callback()

      async.timesSeries responseCount, getJob, (error) =>
        return callback error if error?
        callback null, requests

  _prepareClient: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      @client = new RedisNS @dispatcherWorker.namespace, client
      callback()

  _prepareGeneratorJobManager: (callback) =>
    @_prepareRedis @dispatcherWorker.redisUri, (error, client) =>
      return callback error if error?
      client = new RedisNS @dispatcherWorker.namespace, client
      @jobManager = new JobManager
        client: client
        timeoutSeconds: 1
        jobLogSampleRate: 1
      callback()

  _prepareRedis: (redisUri, callback) =>
    callback = _.once callback
    client = new Redis redisUri, dropBufferSupport: true
    client.on 'ready', (error) =>
      return callback error if error?
      callback null, client

    client.on 'error', callback

module.exports = TestDispatcherWorker
