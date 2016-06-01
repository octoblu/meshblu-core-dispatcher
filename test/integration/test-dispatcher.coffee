path             = require 'path'
_                = require 'lodash'
cson             = require 'cson'
async            = require 'async'
MeshbluConfig    = require 'meshblu-config'
mongojs          = require 'mongojs'
redis            = require 'ioredis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-dispatcher:test-dispatcher')
JobManager       = require 'meshblu-core-job-manager'
CacheFactory     = require '../../src/cache-factory'
DatastoreFactory = require '../../src/datastore-factory'
Dispatcher       = require '../../src/dispatcher'
JobAssembler     = require '../../src/job-assembler'
JobRegistry      = require '../../src/job-registry'
JobManager       = require 'meshblu-core-job-manager'

QueueWorker      = require '../../src/queue-worker'
JobLogger        = require 'job-logger'

class TestDispatcher
  constructor: ({@publicKey} = {})->
    jobs = cson.parseFile(path.join __dirname, '../../job-registry.cson')

    @redisUri          = process.env.REDIS_URI
    @mongoDBUri        = 'localhost:27017/meshblu-core-test'
    @pepper            = 'pepper'
    @namespace         = 'meshblu-test'
    @namespaceInternal = 'meshblu-test:internal'
    @meshbluConfig     = new MeshbluConfig().toJSON()
    @jobNames          = _.keys(jobs)

  doSingleRun: (callback) =>
    async.parallel [
      async.apply @clearRedis
      async.apply @runDispatcher
      async.apply @runQueueWorker
    ], callback

  clearRedis: (callback) =>
    @getCacheFactory()
    @cacheFactory.client.keys 'datastore:*', (error, keys) =>
      async.eachSeries keys, (key, done) =>
        @cacheFactory.client.del key, done
      , callback

  runDispatcher: (callback) =>
    dispatcher = new Dispatcher
      jobManager: @getDispatcherJobManager()
      timeout:   15
      jobHandlers: @assembleJobHandlers()
      jobLogger: @getJobLogger()
      memoryLogger: @getMemoryLogger()
      dispatchLogger: @getDispatchLogger()
      jobLogSampleRate: 1

    dispatcher.dispatch callback

  runQueueWorker: (callback) =>
    queueWorker = new QueueWorker
      aliasServerUri:      undefined
      timeout:             15
      pepper:              @pepper
      publicKey:           @publicKey
      jobs:                @jobNames
      jobRegistry:         @getJobRegistry()
      cacheFactory:        @getCacheFactory()
      datastoreFactory:    @getDatastoreFactory()
      meshbluConfig:       @meshbluConfig
      forwardEventDevices: []
      jobManager:          @getQueueWorkerJobManager()
      externalJobManager:  @getTaskRunnerJobManager()
      taskLogger:          @getTaskLogger()
      ignoreResponse:      false
      jobLogSampleRate:    1

    queueWorker.run callback

  assembleJobHandlers: =>
    return @assembledJobHandlers if @assembledJobHandlers?
    jobAssembler = new JobAssembler
      timeout:        15
      localJobManager: @getLocalJobManager()
      remoteJobManager: @getRemoteJobManager()
      localHandlers:  @jobNames
      remoteHandlers: []
      jobLogSampleRate: 1

    @assembledJobHandlers = jobAssembler.assemble()

  getCacheFactory: =>
    @cacheFactory ?= new CacheFactory client: _.bindAll redis.createClient @redisUri, dropBufferSupport: true
    @cacheFactory

  getDatastoreFactory: =>
    database = mongojs @mongoDBUri
    @datastoreFactory ?= new DatastoreFactory {database, @cacheFactory}
    @datastoreFactory

  getDispatchLogger: =>
    @dispatchLogger ?= new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: 'some-queue'
    @dispatchLogger

  getMemoryLogger: =>
    @memoryLogger ?= new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher-memory'
      type: 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: 'some-other-queue'
    @memoryLogger

  getJobLogger: =>
    @jobLogger ?= new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:job'
      jobLogQueue: 'some-queue'
    @jobLogger

  getJobRegistry: =>
    @jobRegistry ?= (new JobRegistry).toJSON()
    @jobRegistry

  getLogClient: =>
    @logClient ?= _.bindAll redis.createClient @redisUri, dropBufferSupport: true
    @logClient

  getDispatchClient: =>
    _.bindAll new RedisNS @namespace, redis.createClient @redisUri, dropBufferSupport: true

  getLocalJobHandlerClient: =>
    _.bindAll new RedisNS @namespaceInternal, redis.createClient @redisUri, dropBufferSupport: true

  getLocalQueueWorkerClient: =>
    _.bindAll new RedisNS @namespaceInternal, redis.createClient @redisUri, dropBufferSupport: true

  getRemoteJobHandlerClient: =>
    _.bindAll new RedisNS @namespaceInternal, redis.createClient @redisUri, dropBufferSupport: true

  getTaskJobManagerClient: =>
    _.bindAll new RedisNS @namespace, redis.createClient @redisUri, dropBufferSupport: true

  getTaskLogger: =>
    @taskLogger ?= new JobLogger
      client: @getLogClient()
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:task'
      jobLogQueue: 'some-queue'
    @taskLogger

  generateJobs: (job, callback) =>
    debug 'generateJobs for', job?.metadata?.jobType, job?.metadata?.responseId
    jobManager = new JobManager
      client: _.bindAll new RedisNS 'meshblu-test', redis.createClient(@redisUri, dropBufferSupport: true)
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
    client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(@redisUri, dropBufferSupport: true)
    jobManager = new JobManager
      client: _.bindAll new RedisNS 'meshblu-test', redis.createClient(@redisUri, dropBufferSupport: true)
      timeoutSeconds: 1
      jobLogSampleRate: 1

    requests = []
    client.llen 'request:queue', (error, responseCount) =>
      getJob = (number, callback) =>
        jobManager.getRequest ['request'], (error, request) =>
          requests.push request
          callback()

      async.timesSeries responseCount, getJob, (error) =>
        return callback error if error?
        callback null, requests

  getQueueWorkerJobManager: =>
    @queueWorkerJobManager ?= new JobManager {
      timeoutSeconds: 1
      client: @getLocalQueueWorkerClient()
      jobLogSampleRate: 1
    }

    @queueWorkerJobManager

  getTaskRunnerJobManager: =>
    @taskRunnerJobManager ?= new JobManager {
      timeoutSeconds: 1
      client: @getTaskJobManagerClient()
      jobLogSampleRate: 1
    }

    @taskRunnerJobManager

  getLocalJobManager: =>
    @localJobManager ?= new JobManager {
      client: @getLocalJobHandlerClient()
      timeoutSeconds: 1
      jobLogSampleRate: 1
    }

    @localJobManager

  getRemoteJobManager: =>
    @remoteJobManager ?= new JobManager {
      client: @getRemoteJobHandlerClient()
      timeoutSeconds: 1
      jobLogSampleRate: 1
    }

    @remoteJobManager

  getDispatcherJobManager: =>
    @dispatcherJobManager ?= new JobManager {
      client: @getDispatchClient()
      timeoutSeconds: 1
      jobLogSampleRate: 1
    }

    @dispatcherJobManager

module.exports = TestDispatcher
