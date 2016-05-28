_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'SendMessage: direct', ->
  beforeEach (done)->
    @db = mongojs 'meshblu-core-test'
    @collection = @db.collection 'devices'
    @collection.drop => done()

  beforeEach ->
    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher

    client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri, dropBufferSupport: true)

    client.del 'request:queue'

    @jobManager = new JobManager
      client: client
      timeoutSeconds: 15

  beforeEach (done) ->
    @auth =
      uuid: 'sender-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8

    @collection.insert @senderDevice, done

  beforeEach (done) ->
    @receiverDevice =
      uuid: 'receiver-uuid'
      type: 'device:receiver'
      sendWhitelist: [ 'sender-uuid' ]

    @collection.insert @receiverDevice, done

  describe "sending to a device with sendWhitelist", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'SendMessage'
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

    describe 'JobManager gets DeliverSentMessage job', (done) ->
      beforeEach (done) ->
        getJob = =>
          @jobManager.getRequest ['request'], (error, request) =>
            return done error if error?
            return getJob() unless request.metadata.jobType == 'DeliverSentMessage'
            @request = request
            return done()
        getJob()

      it 'should be a sent messageType', ->
        auth =
          uuid: 'sender-uuid'
          token: 'leak'

        {rawData, metadata} = @request
        expect(metadata.auth.uuid).to.deep.equal auth.uuid
        expect(metadata.auth.token).to.exist
        expect(metadata.jobType).to.equal 'DeliverSentMessage'
        expect(metadata.messageType).to.equal 'sent'
        expect(metadata.toUuid).to.equal 'sender-uuid'
        expect(metadata.fromUuid).to.equal 'sender-uuid'
        expect(rawData).to.equal JSON.stringify devices:['receiver-uuid'], payload: 'boo', fromUuid: 'sender-uuid'

      describe 'JobManager gets DeliverReceivedMessage job', (done) ->
        beforeEach (done) ->
          @jobManager.getRequest ['request'], (error, @request) =>
            done error

        it 'should be a sent messageType', ->
          auth =
            uuid: 'sender-uuid'
            token: 'leak'

          {rawData, metadata} = @request
          expect(metadata.auth.uuid).to.deep.equal auth.uuid
          expect(metadata.auth.token).to.exist
          expect(metadata.jobType).to.equal 'DeliverReceivedMessage'
          expect(metadata.messageType).to.equal 'received'
          expect(metadata.toUuid).to.equal 'receiver-uuid'
          expect(metadata.fromUuid).to.equal 'sender-uuid'
          expect(rawData).to.equal JSON.stringify devices:['receiver-uuid'], payload: 'boo', fromUuid: 'sender-uuid'
