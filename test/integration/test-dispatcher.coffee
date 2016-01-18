path             = require 'path'
_                = require 'lodash'
cson             = require 'cson'
async            = require 'async'
MeshbluConfig    = require 'meshblu-config'
mongojs          = require 'mongojs'
redis            = require 'redis'
RedisNS          = require '@octoblu/redis-ns'
debug            = require('debug')('meshblu-core-dispatcher:test-dispatcher')
CacheFactory     = require '../../src/cache-factory'
DatastoreFactory = require '../../src/datastore-factory'
Dispatcher       = require '../../src/dispatcher'
JobAssembler     = require '../../src/job-assembler'
JobRegistry      = require '../../src/job-registry'
QueueWorker      = require '../../src/queue-worker'

class TestDispatcher
  constructor: ->
    jobs = cson.parseFile( path.join __dirname, '../../job-registry.cson')

    @redisUri          = process.env.REDIS_URI
    @mongoDBUri        = 'localhost:27017/meshblu-core-test'
    @pepper            = 'pepper'
    @namespace         = 'meshblu-test'
    @namespaceInternal = 'meshblu-test:internal'
    @meshbluConfig     = new MeshbluConfig().toJSON()
    @jobNames          = _.keys(jobs)

  doSingleRun: (callback) =>
    async.parallel [
      async.apply @runDispatcher
      async.apply @runQueueWorker
    ], callback

  runDispatcher: (callback) =>
    dispatcher = new Dispatcher
      client:  @getDispatchClient()
      timeout:   15
      jobHandlers: @assembleJobHandlers()

    dispatcher.dispatch callback

  runQueueWorker: (callback) =>
    queueWorker = new QueueWorker
      aliasServerUri:   undefined
      timeout:          15
      pepper:           @pepper
      jobs:             @jobNames
      client:           @getLocalQueueWorkerClient()
      jobRegistry:      @getJobRegistry()
      cacheFactory:     @getCacheFactory()
      datastoreFactory: @getDatastoreFactory()
      meshbluConfig:    @meshbluConfig
      forwardEventDevices: []
      externalClient:      @getTaskJobManagerClient()

    queueWorker.run callback

  assembleJobHandlers: =>
    return @assembledJobHandlers if @assembledJobHandlers?
    jobAssembler = new JobAssembler
      timeout:        15
      localClient:    @getLocalJobHandlerClient()
      remoteClient:   @getRemoteJobHandlerClient()
      localHandlers:  @jobNames
      remoteHandlers: []

    @assembledJobHandlers = jobAssembler.assemble()

  getCacheFactory: =>
    @cacheFactory ?= new CacheFactory client: redis.createClient @redisUri
    @cacheFactory

  getDatastoreFactory: =>
    @datastoreFactory ?= new DatastoreFactory database: mongojs @mongoDBUri
    @datastoreFactory

  getJobRegistry: =>
    @jobRegistry ?= (new JobRegistry).toJSON()
    @jobRegistry

  getDispatchClient: =>
    @dispatchClient ?= _.bindAll new RedisNS @namespace, redis.createClient @redisUri
    @dispatchClient

  getLocalJobHandlerClient: =>
    @localJobHandlerClient ?= _.bindAll new RedisNS @namespaceInternal, redis.createClient @redisUri
    @localJobHandlerClient

  getLocalQueueWorkerClient: =>
    @localQueueWorkerClient ?= _.bindAll new RedisNS @namespaceInternal, redis.createClient @redisUri
    @localQueueWorkerClient

  getRemoteJobHandlerClient: =>
    @remoteClient ?= _.bindAll new RedisNS @namespaceInternal, redis.createClient @redisUri
    @remoteClient

  getTaskJobManagerClient: =>
    @taskJobManagerClient ?= _.bindAll new RedisNS @namespace, redis.createClient @redisUri
    @taskJobManagerClient

module.exports = TestDispatcher
