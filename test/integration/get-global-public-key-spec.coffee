_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'GetGlobalPublicKey', ->
  describe 'when the dispatcher has a publicKey', ->
    beforeEach ->
      redisUri = process.env.REDIS_URI
      @sut = new TestDispatcher publicKey: 'hi mom!'

      client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri)
      client.del 'request:queue'

      @jobManager = new JobManager
        client: client
        timeoutSeconds: 15

    describe "when the publicKey is requested", ->
      beforeEach (done) ->
        job =
          metadata:
            jobType: 'GetGlobalPublicKey'

        @jobManager.do 'request', 'response', job, (error, @response) => done error

        @sut.doSingleRun (error) =>
          throw error if error?

      it 'should give a status code 200', ->
        expect(@response.metadata.code).to.equal 200

      it 'should give us the public key', ->
        data = JSON.parse @response.rawData
        expect(data.publicKey).to.equal 'hi mom!'

  describe 'when the dispatcher doesnt has a publicKey', ->
    beforeEach ->
      redisUri = process.env.REDIS_URI
      @sut = new TestDispatcher publicKey: undefined

      client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri)
      client.del 'request:queue'

      @jobManager = new JobManager
        client: client
        timeoutSeconds: 15

    describe "when the publicKey is requested", ->
      beforeEach (done) ->
        job =
          metadata:
            jobType: 'GetGlobalPublicKey'

        @jobManager.do 'request', 'response', job, (error, @response) => done error

        @sut.doSingleRun (error) =>
          throw error if error?

      it 'should give us a 204', ->
        expect(@response.metadata.code).to.equal 204
