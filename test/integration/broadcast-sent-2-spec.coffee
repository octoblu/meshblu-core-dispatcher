{describe,context,beforeEach,afterEach,expect,it} = global
bcrypt         = require 'bcryptjs'
TestDispatcherWorker = require './test-dispatcher-worker'

describe 'BroadcastSent(2): send', ->
  @timeout 5000
  beforeEach 'prepare TestDispatcherWorker', (done) ->
    @testDispatcherWorker = new TestDispatcherWorker
    @testDispatcherWorker.start done

  afterEach (done) ->
    @testDispatcherWorker.stop done

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
    @auth =
      uuid: 'sender-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8
      meshblu:
        version: '2.0.0'
        whitelists:
          broadcast:
            sent: [{uuid: 'spy-uuid'}]

    @devices.insert @senderDevice, done

  beforeEach 'create spy device', (done) ->
    @spyDevice =
      uuid: 'spy-uuid'
      type: 'device:spy'
      meshblu:
        version: '2.0.0'
        whitelists:
          broadcast:
            received: [{uuid: 'nsa-uuid'}]

    @devices.insert @spyDevice, done

  beforeEach 'create nsa device', (done) ->
    @nsaDevice =
      uuid: 'nsa-uuid'
      type: 'device:nsa'

    @devices.insert @nsaDevice, done

  context 'When sending a broadcast message', ->
    context "sender-uuid receiving its sent messages", ->
      beforeEach 'create broadcast sent subscription', (done) ->
        subscription =
          type: 'broadcast.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'sender-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'sender-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create SendMessage job', (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: @auth.uuid
            jobType: 'SendMessage'
          rawData: JSON.stringify devices:['*'], payload: 'boo'

        @hydrant.connect uuid: @auth.uuid, (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            done()

          @testDispatcherWorker.jobManagerRequester.do job, (error) =>
            done error if error?
        return # fix redis promise issue

      it 'should deliver the sent broadcast to the sender', ->
        expect(@message).to.exist

    context 'subscribed to someone elses sent broadcasts', ->
      beforeEach 'create broadcast sent subscription', (done) ->
        subscription =
          type: 'broadcast.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['*']
            payload: 'boo'

        @hydrant.connect uuid: 'spy-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            done()

          @testDispatcherWorker.jobManagerRequester.do job, (error) =>
            done error if error?

        return # fix redis promise issue

      it 'should deliver the sent broadcast to the receiver', ->
        expect(@message).to.exist

    context 'subscribed to someone elses sent broadcasts, but is not authorized', ->
      beforeEach 'create broadcast sent subscription', (done) ->
        subscription =
          type: 'broadcast.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'nsa-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['*']
            payload: 'boo'

        @hydrant.connect uuid: 'nsa-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) => @hydrant.close()

          @testDispatcherWorker.jobManagerRequester.do job, (error) =>
            return done error if error?
            setTimeout done, 2000
        return # fix redis promise issue

      it 'should not deliver the sent broadcast to the receiver', ->
        expect(@message).to.not.exist

    context 'subscribed to someone elses received broadcasts', ->
      beforeEach 'create broadcast sent subscription', (done) ->
        subscription =
          type: 'broadcast.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'nsa-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: 'sender-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['*'], payload: 'boo'

        @hydrant.connect uuid: 'nsa-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            done()

          @testDispatcherWorker.jobManagerRequester.do job, (error) =>
            done error if error?
        return # fix redis promise issue

      it 'should deliver the sent message to the receiver', ->
        expect(@message).to.exist

    context 'subscribed to someone elses received messages, but is not authorized', ->
      beforeEach 'create broadcast sent subscription', (done) ->
        subscription =
          type: 'broadcast.sent'
          emitterUuid: 'sender-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'broadcast.received'
          emitterUuid: 'nsa-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            fromUuid: @auth.uuid
            jobType: 'SendMessage'
          data:
            devices: ['*']
            payload: 'boo'

        @hydrant.connect uuid: 'spy-uuid', (error) =>
          return done(error) if error?
          @hydrant.once 'message', (@message) => @hydrant.close()

          @testDispatcherWorker.jobManagerRequester.do job, (error) =>
            return done error if error?
            setTimeout done, 2000
        return # fix redis promise issue

      it 'should not deliver the sent message to the receiver', ->
        expect(@message).to.not.exist
