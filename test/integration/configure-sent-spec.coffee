_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

describe 'ConfigureSent', ->
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
    @auth =
      uuid: 'emitter-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'emitter-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8
      meshblu:
        version: '2.0.0'
        whitelists:
          configure:
            sent: [{uuid: 'spy-uuid'}]

    @devices.insert @senderDevice, done

  beforeEach 'create spy device', (done) ->
    @spyDevice =
      uuid: 'spy-uuid'
      type: 'device:spy'
      meshblu:
        version: '2.0.0'
        whitelists:
          configure:
            received: [{uuid: 'nsa-uuid'}]

    @devices.insert @spyDevice, done

  beforeEach 'create nsa device', (done) ->
    @nsaDevice =
      uuid: 'nsa-uuid'
      type: 'device:nsa'

    @devices.insert @nsaDevice, done

  context 'When sending a configuring a device', ->
    context "emitter-uuid receiving own configure.sent events", ->
      beforeEach 'create configure sent subscription', (done) ->
        subscription =
          type: 'configure.sent'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'emitter-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'emitter-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create UpdateDevice job', (done) ->
        doneTwice = _.after 2, done
        job =
          metadata:
            auth: @auth
            toUuid: 'emitter-uuid'
            fromUuid: 'emitter-uuid'
            jobType: 'UpdateDevice'
          data:
            $set:
              foo: 'bar'

        @hydrant.connect uuid: @auth.uuid, (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => doneTwice()
        return # fix redis promise issue

      it 'should deliver the sent configure to the sender', ->
        expect(@message).to.exist

    context 'subscribed to someone elses sent configs', ->
      beforeEach 'create configure sent subscription', (done) ->
        subscription =
          type: 'configure.sent'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        doneTwice = _.after 2, done
        job =
          metadata:
            auth: @auth
            toUuid: 'emitter-uuid'
            jobType: 'UpdateDevice'
          data:
            $set:
              foo: 'bar'

        @hydrant.connect uuid: 'spy-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => doneTwice()
        return # fix redis promise issue

      it 'should deliver the sent configure to the receiver', ->
        expect(@message).to.exist

    context 'subscribed to someone elses sent config, but is not authorized', ->
      beforeEach 'create configure sent subscription', (done) ->
        subscription =
          type: 'configure.sent'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create broadcast received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'nsa-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            toUuid: 'emitter-uuid'
            jobType: 'UpdateDevice'
          data:
            $set:
              foo: 'bar'

        @hydrant.connect uuid: 'nsa-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) => @hydrant.close()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) =>
            setTimeout done, 2000
        return # fix redis promise issue

      it 'should not deliver the sent configure to the receiver', ->
        expect(@message).to.not.exist

    context 'subscribed to someone elses received config', ->
      beforeEach 'create configure sent subscription', (done) ->
        subscription =
          type: 'configure.sent'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'nsa-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        doneTwice = _.after 2, done
        job =
          metadata:
            auth: @auth
            toUuid: 'emitter-uuid'
            jobType: 'UpdateDevice'
          data:
            $set:
              foo: 'bar'

        @hydrant.connect uuid: 'nsa-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => doneTwice()
        return # fix redis promise issue

      it 'should deliver the sent configure to the receiver', ->
        expect(@message).to.exist

    context 'subscribed to someone elses received configs, but is not authorized', ->
      beforeEach 'create configure sent subscription', (done) ->
        subscription =
          type: 'configure.sent'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'nsa-uuid'
          subscriberUuid: 'nsa-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            toUuid: 'emitter-uuid'
            jobType: 'UpdateDevice'
          data:
            $set:
              foo: 'bar'

        @hydrant.connect uuid: 'spy-uuid', (error) =>
          return done(error) if error?
          @hydrant.once 'message', (@message) => @hydrant.close()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) =>
            setTimeout done, 2000
        return # fix redis promise issue

      it 'should not deliver the sent message to the receiver', ->
        expect(@message).to.not.exist
