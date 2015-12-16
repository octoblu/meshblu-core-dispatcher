_             = require 'lodash'
redis         = require 'redis'
RedisNS       = require '@octoblu/redis-ns'
MeshbluHttp   = require 'meshblu-http'

TestDispatcher = require './test-dispatcher'
JobManager    = require 'meshblu-core-job-manager'

describe 'MeshbluCoreDispatcher', ->
  beforeEach ->
    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher


    client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri)

    client.del 'request:queue'

    @jobManager = new JobManager
      client: client
      timeoutSeconds: 15


  describe 'GetDevice', ->
    # @timeout 5000
    beforeEach (done) ->
      # console.log "meshbluHttp", @meshbluHttp
      openParams =
        discoverWhitelist: ['*']
        configureWhitelist: ['*']
        receiveWhitelist: ['*']
        sendWhitelist: ['*']

      @authData = _.defaults type: 'device:auth-device', openParams
      @discovererData = _.extend {}, @authData, type: "device:device-discoverer"
      @discovereeData = _.extend {}, @authData, type: "device:device-discoveree"

      @meshbluHttp = new MeshbluHttp({
        server: "meshblu.octoblu.com"
        port: 443
      })

      @meshbluHttp.register @authData, (error, credentials) =>
        console.log 'Register:authdevice', error, @authCredentials

      @meshbluHttp.register @discovererData, (error, @deviceDiscoverer) =>
        console.log 'Register:device-discoverer', error, @deviceDiscoverer

      @meshbluHttp.register @discovereeData, (error, @deviceDiscoveree) =>
        console.log 'Register:device-discoveree', error, @deviceDiscoveree

      auth =
        uuid: 'hi'
        token: 'earth'

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
