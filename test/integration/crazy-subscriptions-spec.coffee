_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

describe 'ConfigureSent', ->
  @timeout 5000

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

  beforeEach 'define whitelists', () ->
    @meshbluWhitelists =
      version: "2.0.0"
      whitelists:
        broadcast:
          as: [uuid:"*"]
          received: [uuid:"*"]
          sent: [uuid:"*"]
        discover:
          as: [uuid:"*"]
          view: [uuid:"*"]
        configure:
          as: [uuid:"*"]
          received: [uuid:"*"]
          sent: [uuid:"*"]
          update: [uuid:"*"]
        message:
          as: [uuid:"*"]
          from: [uuid:"*"]
          received: [uuid:"*"]
          sent: [uuid:"*"]

  beforeEach 'create update device', (done) ->
    @authUpdate =
      uuid: 'update-uuid'
      token: 'leak'

    @updateDevice =
      uuid: 'update-uuid'
      type: 'device:updater'
      token: bcrypt.hashSync @authUpdate.token, 8
      meshblu: @meshbluWhitelists

        # version: '2.0.0'
        # whitelists:
        #   configure:
        #     received: [{uuid: 'emitter-uuid'}]

      # configureWhitelist: ['*']
      # discoverWhitelist: ['*']
      # receiveWhitelist: ['*']
      # sendWhitelist: ['*']

    @devices.insert @updateDevice, done

  beforeEach 'create sender device', (done) ->
    @authEmitter =
      uuid: 'emitter-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'emitter-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @authEmitter.token, 8
      meshblu: @meshbluWhitelists

      #   version: '2.0.0'
      #   whitelists:
      #     configure:
      #       update: [{uuid: 'update-uuid'}]
      #       as: [{uuid: 'update-uuid'}]
      #       received: [{uuid: 'update-uuid'}]
      #       sent: [{uuid: 'update-uuid'}]

      # configureWhitelist: ['*']
      # discoverWhitelist: ['*']
      # receiveWhitelist: ['*']
      # sendWhitelist: ['*']


    @devices.insert @senderDevice, done

  beforeEach 'create spy device', (done) ->
    @spyDevice =
      uuid: 'spy-uuid'
      type: 'device:spy'
      meshblu: @meshbluWhitelists

        # version: '2.0.0'
        # whitelists:
        #   configure:
        #     received: [{uuid: 'emitter-uuid'}]

      # configureWhitelist: ['*']
      # discoverWhitelist: ['*']
      # receiveWhitelist: ['*']
      # sendWhitelist: ['*']


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
      #
      # beforeEach 'create configure received subscription', (done) ->
      #   subscription =
      #     type: 'configure.sent'
      #     emitterUuid: 'emitter-uuid'
      #     subscriberUuid: 'spy-uuid'
      #
      #   @subscriptions.insert subscription, done

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

        @hydrant.connect uuid: 'spy-uuid', (error) =>
          return done(error) if error?

          @hydrant.once 'message', (@message) =>
            @hydrant.close()
            doneTwice()

          @testDispatcherWorker.generateJobs job, (error, @generatedJobs) =>
            doneTwice()
        return # fix redis promise issue

      it.only 'should deliver the sent configuration to the receiver with the receiver in the route', ->
        expect(_.last(@message.metadata.route).to).to.equal 'spy-uuid'
