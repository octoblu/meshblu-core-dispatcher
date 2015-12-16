_             = require 'lodash'
redis         = require 'redis'
RedisNS       = require '@octoblu/redis-ns'
MeshbluHttp   = require 'meshblu-http'

TestDispatcher = require './test-dispatcher'
JobManager    = require 'meshblu-core-job-manager'

describe 'MeshbluCoreDispatcher', ->
  beforeEach ->
    @meshbluHttp = new MeshbluHttp()
    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher


    client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri)

    client.del 'request:queue'

    @jobManager = new JobManager
      client: client
      timeoutSeconds: 15


  describe 'GetDevice', ->
    beforeEach 'register devices', (done) ->
      @meshbluHttp.register type: 'device:auth', (error, @authDevice) =>
        discovererDeviceData = type: 'device:discoverer', discoverAsWhitelist: [@authDevice.uuid]
        @meshbluHttp.register discovererDeviceData, (error, @discovererDevice) =>
          discovereeDeviceData = type: 'device:discoveree', discoverWhitelist: [@discovererDevice.uuid]
          @meshbluHttp.register discovereeDeviceData, (error, @discovererDevice) => done()

    beforeEach (done) ->
      auth =
        uuid: @authDevice.uuid
        token: @authDevice.token

      job =
        metadata:
          auth: auth
          fromUuid: auth.uuid
          toUuid: auth.uuid
          jobType: 'GetDevice'


      @jobManager.do 'request', 'response', job, (@error, @response) => done()

      @dispatcher.doSingleRun =>

    it 'should give us a response', ->
      console.log 'error', @error, 'response', @response
      expect(@response).to.exist
