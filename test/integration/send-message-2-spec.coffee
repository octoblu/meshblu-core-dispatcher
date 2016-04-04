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
    client = new RedisNS 'meshblu-test', redis.createClient(redisUri)
    client.del 'request:queue'

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

      @dispatcher.generateJobs job, (error, @generatedJobs) =>
        done()

    it.only 'should return a 204', ->
      expectedResponse =
        metadata:
          code: 204
          status: 'No Content'

      expect(@generatedJobs).to.exist
