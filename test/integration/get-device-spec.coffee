_             = require 'lodash'
redis         = require 'redis'
RedisNS       = require '@octoblu/redis-ns'
MeshbluHttp   = require 'meshblu-http'

TestDispatcher = require './test-dispatcher'
JobManager    = require 'meshblu-core-job-manager'

describe 'MeshbluCoreDispatcher', ->
  beforeEach ->
    @meshbluHttp = new MeshbluHttp port: 3000, server: 'localhost'
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
          @meshbluHttp.register discovereeDeviceData, (error, @discovereeDevice) => done()

    describe 'when a device requests itself (whoami)', ->
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

      it 'should give us a device', ->
        device = JSON.parse @response.rawData
        expect(device.type).to.equal 'device:auth'

    describe 'when a device requests itself with a bad token (whoami)', ->
      beforeEach (done) ->
        auth =
          uuid: @authDevice.uuid
          token: '12344'

        job =
          metadata:
            auth: auth
            fromUuid: auth.uuid
            toUuid: auth.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403

    xdescribe "when auth tries to discover discovererDevice but is only in it's discoverAsWhitelist", ->
      beforeEach (done) ->
        auth =
          uuid: @authDevice.uuid
          token: @authDevice.token

        job =
          metadata:
            auth: auth
            fromUuid: auth.uuid
            toUuid: @discovererDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403

    describe "when authDevice tries to discover discovererDevice as the discovererDevice", ->
      beforeEach (done) ->
        auth =
          uuid: @authDevice.uuid
          token: @authDevice.token

        job =
          metadata:
            auth: auth
            fromUuid: @discovererDevice.uuid
            toUuid: @discovererDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it 'should give us a device', ->
        device = JSON.parse @response.rawData
        expect(device.type).to.equal 'device:discoverer'


    describe "when authDevice tries to discover discovereeDevice", ->
      beforeEach (done) ->
        auth =
          uuid: @authDevice.uuid
          token: @authDevice.token

        job =
          metadata:
            auth: auth
            fromUuid: auth.uuid
            toUuid: @discovereeDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403


    describe "when authDevice tries to discover discovereeDevice as discovererDevice", ->
      beforeEach (done) ->
        auth =
          uuid: @authDevice.uuid
          token: @authDevice.token

        job =
          metadata:
            auth: auth
            fromUuid: @discovererDevice.uuid
            toUuid: @discovereeDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it 'should give us a device', ->
        device = JSON.parse @response.rawData
        expect(device.type).to.equal 'device:discoveree'

    describe "when authDevice tries to discover discovereeDevice as discovereeDevice", ->
      beforeEach (done) ->
        auth =
          uuid: @authDevice.uuid
          token: @authDevice.token

        job =
          metadata:
            auth: auth
            fromUuid: @discovereeDevice.uuid
            toUuid: @discovereeDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403
