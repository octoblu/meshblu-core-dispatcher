_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'redis'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'GetDevice', ->
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

  beforeEach 'register devices', (done) ->
    doneThrice = _.after done, 3

    @auth =
      uuid: 'lack_of_lifeboats'
      token: 'leak'

    @authDevice =
      uuid: 'lack_of_lifeboats'
      token: bcrypt.hashSync @auth.token, 8
      type: 'device:auth'

    @discovererDevice =
      uuid: 'deep-freeze'
      type: 'device:discoverer'
      discoverAsWhitelist: [@authDevice.uuid]
      discoverWhitelist: []

    @discovereeDevice =
      uuid: 'premature-bird'
      type: 'device:discoveree'
      discoverWhitelist: [@discovererDevice.uuid]


    @collection.insert @authDevice, doneThrice
    @collection.insert @discovererDevice, doneThrice
    @collection.insert @discovereeDevice, doneThrice

  describe "when a device requests itself without as'ing", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'GetDevice'

      @jobManager.do 'request', 'response', job, (@error, @response) => done()

      @dispatcher.doSingleRun =>

    it 'should give us a device', ->
      device = JSON.parse @response.rawData
      expect(device.type).to.equal 'device:auth'

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

    it "should tell us we're not allowed", ->
      expect(@response.metadata.code).to.equal 403

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

  describe "when authDevice tries to discover discovererDevice as discovereeDevice", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          fromUuid: @discovereeDevice.uuid
          toUuid: @discovererDevice.uuid
          jobType: 'GetDevice'


      @jobManager.do 'request', 'response', job, (@error, @response) => done()

      @dispatcher.doSingleRun =>

    it "should tell us we're not allowed", ->
      expect(@response.metadata.code).to.equal 403
