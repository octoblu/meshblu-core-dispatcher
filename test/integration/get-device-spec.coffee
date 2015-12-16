_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'redis'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'MeshbluCoreDispatcher', ->

  beforeEach (done)->
    @db = mongojs 'localhost:27017/meshblu-core-test'
    @collection = @db.collection 'devices'
    @collection.drop (error) => done()

    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher

    client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri)

    client.del 'request:queue'

    @jobManager = new JobManager
      client: client
      timeoutSeconds: 15

  describe 'GetDevice', ->
    beforeEach 'register devices', (done) ->
      doneThrice = _.after done, 3

      @auth =
        uuid: '4b95391b-e64e-436d-b1ed-3d0d72ade8da'
        token: '714930d5629f843ef69bd5b064f31d8644de7fee'

      @authDevice =
        uuid: '4b95391b-e64e-436d-b1ed-3d0d72ade8da'
        token: '$2a$08$igLdPKPvc0HNGZt5MrasheHQ0s6ItfpRJ7syqjcPhVZ36IC3AvASe'
        type: 'device:auth'

      @discovererDevice =
        uuid: '4e7a7334-329d-4d7c-993d-f1c162ba37cc'
        type: 'device:discoverer'
        discoverAsWhitelist: [@authDevice.uuid]
        discoverWhitelist: []

      @discovereeDevice =
        uuid: '9a594952-9e39-478a-9819-9022afd89ad3'
        type: 'device:discoveree'
        discoverWhitelist: [@discovererDevice.uuid]


      @collection.insert @authDevice, doneThrice
      @collection.insert @discovererDevice, doneThrice
      @collection.insert @discovereeDevice, doneThrice

    describe 'when a device requests itself (whoami)', ->
      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: @auth.uuid
            toUuid: @auth.uuid
            jobType: 'GetDevice'

        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it 'should give us a device', ->
        device = JSON.parse @response.rawData
        expect(device.type).to.equal 'device:auth'

    describe 'when a device requests itself with a bad token (whoami)', ->
      beforeEach (done) ->

        job =
          metadata:
            auth:
              uuid: @auth.uuid
              token: 'truuusssst me'
            fromUuid: @auth.uuid
            toUuid: @auth.uuid
            jobType: 'GetDevice'

        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403

    describe "when auth tries to discover discovererDevice but is only in it's discoverAsWhitelist", ->
      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: @auth.uuid
            toUuid: @discovererDevice.uuid
            jobType: 'GetDevice'

        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403

    describe "when authDevice tries to discover discovererDevice as the discovererDevice", ->
      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
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
        job =
          metadata:
            auth: @auth
            fromUuid: @auth.uuid
            toUuid: @discovereeDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403


    describe "when authDevice tries to discover discovereeDevice as discovererDevice", ->
      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
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
        job =
          metadata:
            auth: @auth
            fromUuid: @discovereeDevice.uuid
            toUuid: @discovereeDevice.uuid
            jobType: 'GetDevice'


        @jobManager.do 'request', 'response', job, (@error, @response) => done()

        @dispatcher.doSingleRun =>

      it "should tell us we're not allowed", ->
        expect(@response.metadata.code).to.equal 403
