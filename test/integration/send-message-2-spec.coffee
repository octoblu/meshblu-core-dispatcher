_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
async          = require 'async'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'SendMessage2: broadcast+send', ->
  beforeEach (done)->
    @db = mongojs 'meshblu-core-test'
    @collection = @db.collection 'devices'
    @collection.drop => done()

  beforeEach ->
    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher

    @client = new RedisNS 'meshblu-test', redis.createClient(redisUri)

    @client.del 'request:queue'

    @jobManager = new JobManager
      client: new RedisNS 'meshblu-test', redis.createClient(redisUri)
      timeoutSeconds: 1

  beforeEach (done) ->
    @auth =
      uuid: 'sender-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8
      receiveWhitelist: [ 'receiver-uuid' ]

    @collection.insert @senderDevice, done

  beforeEach (done) ->
    @receiverDevice =
      uuid: 'receiver-uuid'
      type: 'device:receiver'

    @collection.insert @receiverDevice, done

  context "sending to a device with sendWhitelist", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'SendMessage2'
        rawData: JSON.stringify devices:['receiver-uuid'], payload: 'boo'

      @jobManager.do 'request', 'response', job, (error, @response) => done error

      @dispatcher.doSingleRun (error) =>
        throw error if error?

    it 'should return a 204', ->
      expectedResponse =
        metadata:
          code: 204
          status: 'No Content'

      expect(@response).to.containSubset expectedResponse

    context.only 'The dispatcher has generated the new jobs' ->
      beforeEach (done) ->
        @requests = []

        @client.llen 'request:queue', (error, responseCount) =>
          getJob = (number, callback) =>
            @jobManager.getRequest ['request'], (error, request) =>
              @requests.push request
              callback()

          async.times responseCount, getJob, done

    context 'DeliverMessageSent', ->
      beforeEach (done) ->
        job = _.find @requests, metadata: jobType: 'DeliverMessageSent'

        @jobManager.do 'request', 'response', job, (error, @response) =>
          console.log {@response}
          done error

        @dispatcher.doSingleRun (error) =>
          throw error if error?

      it 'should send a DeliverBroadcastSent', ->
        deliverBroadcastSent =
          metadata:
            auth:
              uuid: 'sender-uuid'
            jobType: 'DeliverBroadcastSent'
            toUuid: 'sender-uuid'
          rawData: JSON.stringify devices: ['*', 'red-dawn'], payload: 'boo'

        expect(@requests).to.containSubset [deliverBroadcastSent]

      it 'should send a DeliverMessageSent', ->
        deliverMessageSent =
          metadata:
            auth:
              uuid: 'sender-uuid'
            jobType: 'DeliverMessageSent'
            fromUuid: 'sender-uuid'
          rawData: JSON.stringify devices: ['*', 'red-dawn'], payload: 'boo'
        expect(@requests).to.containSubset [deliverMessageSent]

      it 'should send a DeliverMessageReceived', ->
        deliverMessageReceived =
          metadata:
            auth:
              uuid: 'sender-uuid'
            jobType: 'DeliverMessageReceived'
            fromUuid: 'sender-uuid'
          rawData: JSON.stringify devices: ['*', 'red-dawn'], payload: 'boo'

        expect(@requests).to.containSubset [deliverMessageReceived]
