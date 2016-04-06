_              = require 'lodash'
mongojs        = require 'mongojs'
redis          = require 'ioredis'
async          = require 'async'
bcrypt         = require 'bcrypt'
RedisNS        = require '@octoblu/redis-ns'

TestDispatcher = require './test-dispatcher'
JobManager     = require 'meshblu-core-job-manager'
HydrantManager = require 'meshblu-core-manager-hydrant'

xdescribe 'SendMessage2: send-as', ->
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

  beforeEach 'create sender device', (done) ->
    @auth =
      uuid: 'imposter-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      meshblu:
        version: '2.0.0'
        whitelists:
          message:
            as: 'imposter-uuid': {}

    @devices.insert @senderDevice, done

  beforeEach 'create receiver device', (done) ->
    @receiverDevice =
      uuid: 'receiver-uuid'
      type: 'device:receiver'
      meshblu:
        version: '2.0.0'
        whitelists:
          message:
            from: 'sender-uuid': {}

    @devices.insert @receiverDevice, done

  beforeEach 'create imposter device', (done) ->
    @imposterDevice =
      uuid: 'imposter-uuid'
      token: bcrypt.hashSync @auth.token, 8
      type: 'device:imposter'

    @devices.insert @imposterDevice, done

  context 'When sending a message as another device', ->
    context "sender-uuid receiving it's sent messages", ->
      @timeout 5000
      beforeEach 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'received-uuid'
          subscriberUuid: 'received-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        doneTwice = _.after 2, done
        job =
          metadata:
            auth:
              uuid: 'imposter-uuid'
              token: 'leak'
              as: 'sender-uuid'
            jobType: 'SendMessage2'
          data:
            devices: ['receiver-uuid'], payload: 'boo'

        client = new RedisNS 'messages', redis.createClient(@redisUri)
        @hydrant = new HydrantManager {client, @uuidAliasResolver}
        @hydrant.connect uuid: 'receiver-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @dispatcher.generateJobs job, (error, @generatedJobs) => doneTwice()

      it 'should deliver the sent message to the sender', ->
        expect(@message).to.exist
