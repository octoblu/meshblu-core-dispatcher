_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

xdescribe 'CrazyConfigureSent', ->
  @timeout 10000

  beforeEach 'prepare TestDispatcherWorker', (done) ->
    @testDispatcherWorker = new TestDispatcherWorker
    @testDispatcherWorker.prepare done

  beforeEach 'getJobManager', (done) ->
    @testDispatcherWorker.getJobManager (error, @jobManager) =>
      done error

  beforeEach 'clearAndGetCollection devices', (done) ->
    @testDispatcherWorker.clearAndGetCollection 'devices', (error, @devices) =>
      done error

  beforeEach 'clearAndGetCollection subscriptions', (done) ->
    @testDispatcherWorker.clearAndGetCollection 'subscriptions', (error, @subscriptions) =>
      done error

  beforeEach 'getHydrant', (done) ->
    @testDispatcherWorker.getHydrant (error, @hydrant) =>
      done error

  beforeEach 'create update device', (done) ->
    @authUpdate =
      uuid: 'update-uuid'
      token: 'leak'

    @updateDevice =
      uuid: 'update-uuid'
      type: 'device:updater'
      token: bcrypt.hashSync @authUpdate.token, 8

      meshblu:
        version: '2.0.0'
        whitelists:
          configure:
            sent: [{uuid: 'emitter-uuid'}]

    @devices.insert @updateDevice, done

  beforeEach 'create sender device', (done) ->
    @authEmitter =
      uuid: 'emitter-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'emitter-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @authEmitter.token, 8

      meshblu:
        version: '2.0.0'
        whitelists:
          configure:
            received: [{uuid: 'spy-uuid'}]

    @devices.insert @senderDevice, done

  beforeEach 'create spy device', (done) ->
    @spyDevice =
      uuid: 'spy-uuid'
      type: 'device:spy'

    @devices.insert @spyDevice, done

  context 'When sending a configuring a device', ->

    context 'subscribed to someone elses received configures', ->
      beforeEach 'create configure sent subscription', (done) ->
        subscription =
          type: 'configure.sent'
          emitterUuid: 'update-uuid'
          subscriberUuid: 'emitter-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'emitter-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create configure received subscription', (done) ->
        subscription =
          type: 'configure.received'
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
            auth: @authUpdate
            toUuid: 'update-uuid'
            jobType: 'UpdateDevice'
          data:
            $set:
              foo: 'bar'

        finishTimeout = null
        @messages = []
        @messageCount = 0

        @hydrant.connect uuid: 'spy-uuid', (error) =>
          return done(error) if error?

          @hydrant.on 'message', (message) =>
            # console.log JSON.stringify({message},null,2)
            @messages[@messageCount] = message
            @messageCount++
            finishTimeout = setTimeout(() =>
                @hydrant.close()
                doneTwice()
              , 1000) unless finishTimeout

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) =>
            doneTwice()
        return # fix redis promise issue

      it 'should deliver one message', ->
        expect(@messageCount).to.equal 1

      it 'should deliver the sent configuration to the receiver with the receiver in the route', ->
        expect(_.last(@messages?[0]?.metadata?.route)?.to).to.equal 'spy-uuid'
