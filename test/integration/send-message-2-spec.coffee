_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
async          = require 'async'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'


xdescribe 'SendMessage2: broadcast+send', ->
  beforeEach (done) ->
    @db            = mongojs 'meshblu-core-test'
    @devices    = @db.collection 'devices'
    @subscriptions = @db.collection 'subscriptions'

    @subscriptions.drop =>
      @devices.drop =>
        done()

  beforeEach ->
    redisUri = process.env.REDIS_URI
    @dispatcher = new TestDispatcher
    client = new RedisNS 'meshblu-test', redis.createClient(redisUri)
    client.del 'request:queue'

  beforeEach 'create sender device', (done) ->
    @auth =
      uuid: 'sender-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8
      receiveWhitelist: [ 'receiver-uuid' ]

    @devices.insert @senderDevice, done

  beforeEach 'create receiver device', (done) ->
    @receiverDevice =
      uuid: 'receiver-uuid'
      type: 'device:receiver'

    @devices.insert @receiverDevice, done

  beforeEach 'create message sent subscription', (done) ->
    subscription =
      type: 'message.sent'
      emitterUuid: 'sender-uuid'
      subscriberUuid: 'sender-uuid'

    @subscriptions.insert subscription, done

  beforeEach 'create message received subscription', (done) ->
    subscription =
      type: 'message.received'
      emitterUuid: 'sender-uuid'
      subscriberUuid: 'sender-uuid'

    @subscriptions.insert subscription, done

  context "sending to a device with sendWhitelist", ->
    beforeEach (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'SendMessage2'
        rawData: JSON.stringify devices:['receiver-uuid'], payload: 'boo'

      @dispatcher.generateJobs job, (error, @generatedJobs) => done()

    it 'should return a 204', ->
      console.log JSON.stringify @generatedJobs, null, 2
      expect(@generatedJobs).to.exist
