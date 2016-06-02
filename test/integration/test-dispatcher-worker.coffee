_                = require 'lodash'
async            = require 'async'
DispatcherWorker = require '../../src/dispatcher-worker'
JobManager       = require 'meshblu-core-job-manager'
Redis            = require 'ioredis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-dispatcher:test-dispatcher-worker')

class TestDispatcherWorker
  constructor: ->
    @dispatcherWorker = new DispatcherWorker
      namespace:           'meshblu-test'
      timeoutSeconds:      1
      redisUri:            'redis://localhost:6379'
      mongoDBUri:          'mongodb://localhost'
      pepper:              'im-a-pepper'
      workerName:          'test-worker'
      aliasServerUri:      null
      jobLogRedisUri:      'redis://localhost:6379'
      jobLogQueue:         'sample-rate:1.00'
      jobLogSampleRate:    1
      intervalBetweenJobs: 1
      privateKey:          'private'
      publicKey:           'public'
      singleRun:           true

    @client = new RedisNS 'meshblu-test', new Redis(@dispatcherWorker.redisUri, dropBufferSupport: true)

  doSingleRun: (callback) =>
    async.series [
      @clearRedis
      @dispatcherWorker.run
    ], callback

  clearRedis: (callback) =>
    callback = _.once callback
    @client.keys 'datastore:*', (error, keys) =>
      return callback error if error?
      @client.del keys, callback
    @client.on 'error', callback

  generateJobs: (job, callback) =>
    debug 'generateJobs for', job?.metadata?.jobType, job?.metadata?.responseId
    jobManager = new JobManager
      client: @client
      timeoutSeconds: 1
      jobLogSampleRate: 1

    jobManager.do 'request', 'response', job, (error, response) =>
      return callback (error) if error?

      @getGeneratedJobs (error, newJobs) =>
        return callback error if error?
        return callback null, [] if _.isEmpty newJobs
        async.mapSeries newJobs, @generateJobs, (error, newerJobs) =>
          return callback(error) if error?
          newerJobs = _.flatten newerJobs
          allJobs = newJobs.concat newerJobs
          callback null, allJobs

    @doSingleRun (error) => throw error if error?

  getGeneratedJobs: (callback) =>
    jobManager = new JobManager
      client: @client
      timeoutSeconds: 1
      jobLogSampleRate: 1

    requests = []
    @client.llen 'request:queue', (error, responseCount) =>
      getJob = (number, callback) =>
        jobManager.getRequest ['request'], (error, request) =>
          requests.push request
          callback()

      async.timesSeries responseCount, getJob, (error) =>
        return callback error if error?
        callback null, requests

module.exports = TestDispatcherWorker
