_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'redis'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'

describe 'DeliverReceivedMessage', ->
  beforeEach (done)->
    @db = mongojs 'meshblu-core-test'
    @collection = @db.collection 'devices'
    @collection.drop => done()

  beforeEach ->
    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher

    client = _.bindAll new RedisNS 'meshblu-test', redis.createClient(redisUri)

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
          jobType: 'DeliverReceivedMessage'
          messageType: 'received'
          toUuid: 'receiver-uuid'
          fromUuid: 'sender-uuid'
        rawData: JSON.stringify devices:['receiver-uuid'], payload: 'boo', fromUuid: 'sender-uuid'

      @jobManager.do 'request', 'response', job, (error, @response) => done error

      @dispatcher.doSingleRun (error) =>
        throw error if error?

    it 'should return a 204', ->
      expectedResponse =
        metadata:
          code: 204
          status: 'No Content'

      expect(@response).to.containSubset expectedResponse