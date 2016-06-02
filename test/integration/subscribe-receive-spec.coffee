_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

describe 'DeliverReceivedMessage', ->
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
      sendWhitelist: ['sender-uuid', 'spy-uuid']
    @devices.insert @receiverDevice, done

  beforeEach 'create spy device', (done) ->
    @spyDevice =
      uuid: 'spy-uuid'
      type: 'device:spy'

    @devices.insert @spyDevice, done

  beforeEach 'create nsa device', (done) ->
    @nsaDevice =
      uuid: 'nsa-uuid'
      type: 'device:nsa'

    @devices.insert @nsaDevice, done

  context 'When sending a message to another device', ->
    context "sender-uuid receiving its sent messages", ->
      beforeEach 'create message old message received subscription', (done) ->
        subscription =
          type: 'received'
          emitterUuid: 'receiver-uuid'
          subscriberUuid: 'receiver-uuid'

        @subscriptions.insert subscription, done

      beforeEach 'create spy received subscription for receiver-uuid', (done) ->
        subscription =
          type: 'received'
          emitterUuid: 'receiver-uuid'
          subscriberUuid: 'spy-uuid'

        @subscriptions.insert subscription, done

      beforeEach (done) ->
        job =
          metadata:
            auth: @auth
            toUuid: @auth.uuid
            jobType: 'SendMessage'
          data:
            devices: ['receiver-uuid'], payload: 'boo'

        @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => done()

      it 'should not deliver the received message to the spy', ->
        expect(@generatedJobs).to.not.containSubset [
          metadata:
            toUuid: 'spy-uuid'
        ]

      it 'should deliver the received message to the receiver-uuid', ->
        expect(@generatedJobs).to.containSubset [
          metadata:
            jobType: 'DeliverReceivedMessage'
            toUuid: 'receiver-uuid'
            fromUuid: 'receiver-uuid'
        ]
