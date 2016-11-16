_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

describe 'SendMessage: send-as', ->
  @timeout 5000
  beforeEach 'prepare TestDispatcherWorker', (done) ->
    @testDispatcherWorker = new TestDispatcherWorker
    @testDispatcherWorker.prepare done

  beforeEach 'clearAndGetCollection devices', (done) ->
    @testDispatcherWorker.clearAndGetCollection 'devices', (error, @devices) =>
      done error

  beforeEach 'clearAndGetCollection subscriptions', (done) ->
    @testDispatcherWorker.clearAndGetCollection 'subscriptions', (error, @subscriptions) =>
      done error

  beforeEach 'getHydrant', (done) ->
    @testDispatcherWorker.getHydrant (error, @hydrant) =>
      done error

  beforeEach 'create sender device', (done) ->

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      meshblu:
        version: '2.0.0'
        whitelists:
          message:
            as: [{uuid: 'imposter-uuid'}]

    @devices.insert @senderDevice, done

  beforeEach 'create receiver device', (done) ->
    @receiverDevice =
      uuid: 'receiver-uuid'
      type: 'device:receiver'
      meshblu:
        version: '2.0.0'
        whitelists:
          message:
            from: [{uuid: 'sender-uuid'}]

    @devices.insert @receiverDevice, done

  beforeEach 'create imposter device', (done) ->
    @imposterDevice =
      uuid: 'imposter-uuid'
      token: bcrypt.hashSync 'leak', 8
      type: 'device:imposter'

    @devices.insert @imposterDevice, done

  beforeEach 'create bananas device', (done) ->
    @imposterDevice =
      uuid: 'bananas-uuid'
      token: bcrypt.hashSync 'leak', 8
      type: 'device:bananas'

    @devices.insert @imposterDevice, done

  context 'When sending a message as another device', ->
    context "receiver-uuid receiving it's received messages", ->
      beforeEach 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'receiver-uuid'
          subscriberUuid: 'receiver-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        doneTwice = _.after 2, done
        job =
          metadata:
            auth:
              uuid: 'imposter-uuid'
              token: 'leak'
              as: 'sender-uuid'
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['receiver-uuid'], payload: 'boo'

        @hydrant.connect uuid: 'receiver-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => doneTwice()
        return # fix redis promise issue

      it 'should deliver the received message to the receiver', ->
        expect(@message).to.exist
    context "sender-uuid receiving it's sent messages", ->
      beforeEach 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'sender-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create message sent subscription', (done) ->
        subscription =
          type: 'message.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'sender-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        doneTwice = _.after 2, done
        job =
          metadata:
            auth:
              uuid: 'imposter-uuid'
              token: 'leak'
              as: 'sender-uuid'
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['receiver-uuid'], payload: 'boo'

        @hydrant.connect uuid: 'sender-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => doneTwice()
        return # fix redis promise issue

      it 'should deliver the sent message to the sender', ->
        expect(@message).to.exist

  context 'When sending a message as another device, and not in the as whitelist', ->
    context "receiver-uuid receiving it's received messages", ->
      beforeEach 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'receiver-uuid'
          subscriberUuid: 'receiver-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth:
              uuid: 'bananas-uuid'
              token: 'leak'
              as: 'sender-uuid'
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['receiver-uuid'], payload: 'boo'

        @hydrant.connect uuid: 'receiver-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) =>
            setTimeout done, 2000
        return # fix redis promise issue

      it 'should not deliver the received message to the receiver', ->
        expect(@message).not.to.exist
    context "sender-uuid receiving it's sent messages", ->
      beforeEach 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'sender-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create message sent subscription', (done) ->
        subscription =
          type: 'message.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'sender-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth:
              uuid: 'bananas-uuid'
              token: 'leak'
              as: 'sender-uuid'
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['receiver-uuid'], payload: 'boo'

        @hydrant.connect uuid: 'sender-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) =>
            setTimeout done, 2000
        return # fix redis promise issue

      it 'should deliver the sent message to the sender', ->
        expect(@message).not.to.exist
