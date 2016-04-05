_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
async          = require 'async'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'
HydrantManager = require 'meshblu-core-manager-hydrant'

describe.only 'SendMessage2: broadcast+send', ->
  beforeEach (done) ->
    @db            = mongojs 'meshblu-core-test'
    @devices    = @db.collection 'devices'
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


  beforeEach 'create sender device', (done) ->
    @auth =
      uuid: 'sender-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8

    @devices.insert @senderDevice, done

  beforeEach 'create receiver device', (done) ->
    @receiverDevice =
      uuid: 'receiver-uuid'
      type: 'device:receiver'
      sendWhitelist: [ 'sender-uuid' ]

    @devices.insert @receiverDevice, done

  context "When a device is subscribed to it's own sent whitelist", ->
    @timeout 5000
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

    context 'When sending a message to another device', ->
      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            toUuid: @auth.uuid
            jobType: 'SendMessage2'
          rawData: JSON.stringify devices:['receiver-uuid'], payload: 'boo'

        client = new RedisNS 'messages', redis.createClient(@redisUri)
        @hydrant = new HydrantManager {client, @uuidAliasResolver}
        @hydrant.connect uuid: @auth.uuid, (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            done()

          @dispatcher.generateJobs job, (error, @generatedJobs) =>

      it 'should deliver the sent message to the sender', ->
        expect(@message).to.exist
