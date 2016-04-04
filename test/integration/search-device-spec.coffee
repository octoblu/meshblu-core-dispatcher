_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'SearchDevice', ->
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

    @auth =
      uuid: 'entomologist'
      token: 'i-love-bugs'

    @authDevice =
      uuid: @auth.uuid
      token: bcrypt.hashSync @auth.token, 8
      type: 'human'

    @aphidDevice =
      uuid: 'redaphid'
      type: 'bug'
      discoverWhitelist: ['*']
      color: 'red'

    @flyDevice =
      uuid: 'blackfly'
      type: 'bug'
      color: 'black'
      discoverAsWhitelist: [@authDevice.uuid]

    @beetleDevice =
      uuid: 'love-bug'
      type: 'bug'
      color: 'red'
      discoverWhitelist: [@flyDevice.uuid]

    @trexDevice =
      uuid: 'king-kong'
      type: 'dinosaur'
      color: 'pink'
      discoverWhitelist: [@flyDevice.uuid]

    @collection.insert [@authDevice, @aphidDevice, @flyDevice, @beetleDevice, @trexDevice], done

  describe "when a device is lookin' for bugs", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'SearchDevices'
        data:
          type: 'bug'
      @jobManager.do 'request', 'response', job, (error, @response) =>  done error

      @dispatcher.doSingleRun (error) =>
        throw error if error?

    it 'should give us a device', ->
      devices = JSON.parse @response.rawData
      expect(devices.length).to.equal 1

  describe "when a device is lookin' for bug as a fly", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          fromUuid: @flyDevice.uuid
          jobType: 'SearchDevices'
        data:
          type: 'bug'
      @jobManager.do 'request', 'response', job, (error, @response) => done error

      @dispatcher.doSingleRun (error) =>
        throw error if error?

    it 'should give us a device', ->
      expect(@response.metadata.code).to.equal 200
      devices = JSON.parse @response.rawData
      expect(devices.length).to.equal 3

  describe "when a device is trying to discover a fly as a fly", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @flyDevice.uuid
          fromUuid: @flyDevice.uuid
          jobType: 'SearchDevices'
        data:
          uuid: @flyDevice.uuid

      @jobManager.do 'request', 'response', job, (error, @response) => done error

      @dispatcher.doSingleRun (error) =>
        throw error if error?

    it 'should not give us a device', ->
      devices = JSON.parse @response.rawData
      expect(devices).to.not.exist

    it 'should tell us we\'re not allowed', ->
      expect(@response.metadata.code).to.equal 403

  describe "when a device is lookin' for a dinosaur as a fly", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          fromUuid: @flyDevice.uuid
          jobType: 'SearchDevices'
        data:
          type: 'dinosaur'
      @jobManager.do 'request', 'response', job, (error, @response) => done error

      @dispatcher.doSingleRun (error) =>
        throw error if error?

    it 'should give us a device', ->
      devices = JSON.parse @response.rawData
      expect(devices.length).to.equal 1
