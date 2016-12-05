_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

xdescribe 'CrazyMessageSent', ->
  @timeout 10000

  before 'prepare TestDispatcherWorker', (done) ->
    @testDispatcherWorker = new TestDispatcherWorker
    @testDispatcherWorker.start done

  after (done) ->
    @testDispatcherWorker.stop done

  before 'clearAndGetCollection devices', (done) ->
    @testDispatcherWorker.clearAndGetCollection 'devices', (error, @devices) =>
      done error

  before 'clearAndGetCollection subscriptions', (done) ->
    @testDispatcherWorker.clearAndGetCollection 'subscriptions', (error, @subscriptions) =>
      done error

  before 'getHydrant', (done) ->
    @testDispatcherWorker.getHydrant (error, @hydrant) =>
      done error

  before 'create send device', (done) ->
    @authUpdate =
      uuid: 'send-uuid'
      token: 'leak'

    @sendDevice =
      uuid: 'send-uuid'
      type: 'device:sendr'
      token: bcrypt.hashSync @authUpdate.token, 8

      meshblu:
        version: '2.0.0'
        whitelists:
          message:
            sent: [{uuid: 'emitter-uuid'}]

    @devices.insert @sendDevice, done

  before 'create sender device', (done) ->
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
          message:
            received: [{uuid: 'spy-uuid'}]
            from: [{uuid:'send-uuid'}]

    @devices.insert @senderDevice, done

  before 'create spy device', (done) ->
    @spyDevice =
      uuid: 'spy-uuid'
      type: 'device:spy'

    @devices.insert @spyDevice, done

  context 'When sending a messaging a device', ->

    context 'subscribed to someone elses received and from messages', ->
      before 'create message sent subscription', (done) ->
        subscription =
          type: 'message.sent'
          emitterUuid: 'send-uuid'
          subscriberUuid: 'emitter-uuid'

        @subscriptions.insert subscription, done

      before 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'emitter-uuid'

        @subscriptions.insert subscription, done

      before 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'emitter-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      before 'create message received subscription', (done) ->
        subscription =
          type: 'message.received'
          emitterUuid: 'spy-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      before (done) ->
        job =
          metadata:
            auth: @authUpdate
            toUuid: 'send-uuid'
            jobType: 'SendMessage'
          data:
            devices: ['emitter-uuid']
            payload: foo: 'bar'

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
                done()
              , 1000) unless finishTimeout

          @testDispatcherWorker.jobManagerRequester.do job, (error) =>
            done error if error?
        return # fix redis promise issue

      it 'should deliver one message', ->
        expect(@messageCount).to.equal 1

      it 'should deliver the sent message to the receiver with the receiver in the route', ->
        expect(_.last(@messages?[0]?.metadata?.route)?.to).to.equal 'spy-uuid'
