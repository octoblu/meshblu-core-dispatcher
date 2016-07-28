_              = require 'lodash'
bcrypt         = require 'bcrypt'
TestDispatcherWorker = require './test-dispatcher-worker'

describe 'Unregister', ->
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

  beforeEach 'create device', (done) ->
    @auth =
      uuid: 'sender-uuid'
      token: 'leak'

    @senderDevice =
      uuid: 'sender-uuid'
      type: 'device:sender'
      token: bcrypt.hashSync @auth.token, 8

    @devices.insert @senderDevice, done


  beforeEach 'send itself a message', (done) ->
    job =
      metadata:
        auth: @auth
        toUuid: @auth.uuid
        jobType: 'SendMessage'
      data:
        devices: [@auth.uuid], payload: 'boo'

    @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => done()

  it 'should have sent us a message', ->
    expect(@generatedJobs).to.containSubset [ metadata: jobType: 'DeliverReceivedMessage']

  context 'after unregistering the device', ->
    beforeEach 'unregister the device', (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'UnregisterDevice'

      @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => done()


    beforeEach 'send itself a message', (done) ->
      job =
        metadata:
          auth: @auth
          toUuid: @auth.uuid
          jobType: 'SendMessage'
        data:
          devices: [@auth.uuid], payload: 'boo'

      @testDispatcherWorker.generateJobs job, (error, @generatedJobs) => done()

    it 'should not have sent us a message', ->
      expect(@generatedJobs).not.to.containSubset [ metadata: jobType: 'DeliverReceivedMessage']
