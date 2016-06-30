_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
async          = require 'async'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'
HydrantManager = require 'meshblu-core-manager-hydrant'

describe 'Query cache', ->

  beforeEach (done) ->
    @db            = mongojs 'meshblu-core-test'
    @devices       = @db.collection 'devices'
    @subscriptions = @db.collection 'subscriptions'
    @uuidAliasResolver =
      resolve: (uuid, callback) =>
        callback null, uuid

    @subscriptions.drop =>
      @devices.drop =>
        done()

  beforeEach (done) ->
    @redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher
    client = new RedisNS 'meshblu-test', redis.createClient(@redisUri)
    client.del 'request:queue', done

  beforeEach 'create finder device', (done) ->

    @finderDevice =
      uuid: 'finder-uuid'
      type: 'device:finder'
      token: bcrypt.hashSync 'token', 8
      meshblu:
        version: '2.0.0'

    @devices.insert @finderDevice, done

  beforeEach 'create searchResult device', (done) ->
    @searchResultDevice =
      uuid: 'search-result-uuid'
      token: bcrypt.hashSync 'token', 8
      type: 'device:search-result'
      yo: 'whatever=='
      meshblu:
        version: '2.0.0'
        whitelists:
          discover:
            view: [{uuid: 'finder-uuid'}]

    @devices.insert @searchResultDevice, done

  context 'When searching for a device', ->
    beforeEach 'search for a device', (done) ->
      job =
        metadata:
          auth:
            uuid: 'finder-uuid'
            token: 'token'
          jobType: 'SearchDevices'
        data:
          uuid: 'search-result-uuid'
          yo: 'whatever=='

      @dispatcher.generateJobs job, (error, generatedJobs, response) =>
        @devices = JSON.parse(response.rawData)
        done(error)

    beforeEach 'search for a device', (done) ->
      job =
        metadata:
          auth:
            uuid: 'finder-uuid'
            token: 'token'
          jobType: 'SearchDevices'
        data:
          uuid: 'search-result-uuid'
          yo: 'whatever=='

      @dispatcher.generateJobs job, (error, generatedJobs, response) =>
        @devices = JSON.parse(response.rawData)
        done(error)



    it 'should give us a device', ->
      expect(@devices.length).to.equal 1

    it 'should give us the right device', ->
      device = _.first @devices
      expect(device.uuid).to.equal 'search-result-uuid'

    context "when the device's whitelist has been cleared", ->
      beforeEach 'update the device', (done) ->
        job =
          metadata:
            auth:
              uuid: 'search-result-uuid'
              token: 'token'
            jobType: 'UpdateDevice'
            toUuid: 'search-result-uuid'
          data:
            $set: 'meshblu.whitelists.discover.view': []

        @dispatcher.generateJobs job, done

      beforeEach 'search for a device', (done) ->
        job =
          metadata:
            auth:
              uuid: 'finder-uuid'
              token: 'token'
            jobType: 'SearchDevices'
          data:
            uuid: 'search-result-uuid'
            yo: 'whatever=='

        @dispatcher.generateJobs job, (error, generatedJobs, response) =>
          @devices = JSON.parse(response.rawData)
          done(error)

      it 'should not give us a device', ->
        expect(@devices).to.be.empty
