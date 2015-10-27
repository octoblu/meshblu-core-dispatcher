QueueWorker = require '../src/queue-worker'
JobManager  = require 'meshblu-core-job-manager'
redisMock   = require 'fakeredis'
_           = require 'lodash'
uuid        = require 'uuid'

describe 'QueueWorker', ->
  beforeEach ->
    @localClientId  = "local-#{uuid.v4()}"
    @remoteClientId = "remote-#{uuid.v4()}"

    @localClient = _.bindAll redisMock.createClient @localClientId
    @remoteClient = _.bindAll redisMock.createClient @remoteClientId

    @localJobManager = new JobManager
      client: @localClient
      namespace: 'test:internal'
      timeoutSeconds: 1
      responseQueue: 'authenticate'
      requestQueue: 'authenticate'

    @remoteJobManager = new JobManager
      client: @remoteClient
      namespace: 'test'
      timeoutSeconds: 1
      responseQueue: 'authenticate'
      requestQueue: 'authenticate'

    @tasks =
      'meshblu-task-authenticate': sinon.stub().yields null, {}

  describe '->run', ->
    describe 'when using remoteClient', ->
      beforeEach ->
        @sut = new QueueWorker
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: []
          remoteHandlers: ['authenticate']
          tasks: {}
          namespace: 'test:internal'
          timeout: 1

      describe 'when called and job is pushed into queue', ->
        beforeEach (done) ->
          @sut.runJob = sinon.spy()
          @sut.run()
          responseKey = 'test:internal:sometin'
          @remoteClient.lpush 'test:internal:authenticate:sometin', responseKey, done

        it 'should place the job in the queue', (done) ->
          @remoteClient.brpop 'test:internal:authenticate:sometin', 1, (error, result) =>
            return done error if error?
            [channel, responseKey] = result
            expect(responseKey).to.equal 'test:internal:sometin'
            done()

      describe 'when called and different job is pushed into queue', ->
        beforeEach (done) ->
          @sut.runJob = sinon.spy()
          @sut.run()
          responseKey = 'test:internal:sometin-cool'
          @remoteClient.lpush 'test:internal:authenticate:sometin-cool', responseKey, done

        it 'should place the job in the queue', (done) ->
          @remoteClient.brpop 'test:internal:authenticate:sometin-cool', 1, (error, result) =>
            return done error if error?
            [channel, responseKey] = result
            expect(responseKey).to.equal 'test:internal:sometin-cool'
            done()

    describe 'when using localClient', ->
      beforeEach ->
        @sut = new QueueWorker
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: ['authenticate']
          remoteHandlers: []
          tasks: {}
          namespace: 'test:internal'
          timeout: 1

      describe 'when called and job is pushed into queue', ->
        beforeEach (done) ->
          @sut.runJob = sinon.spy()
          @sut.run()
          responseKey = 'test:internal:sometin'
          @localClient.lpush 'test:internal:authenticate:sometin', responseKey, done

        it 'should place the job in the queue', (done) ->
          @localClient.brpop 'test:internal:authenticate:sometin', 1, (error, result) =>
            return done error if error?
            [channel, responseKey] = result
            expect(responseKey).to.equal 'test:internal:sometin'
            done()

      describe 'when called and different job is pushed into queue', ->
        beforeEach (done) ->
          @sut.runJob = sinon.spy()
          @sut.run()
          responseKey = 'test:internal:sometin-cool'
          @localClient.lpush 'test:internal:authenticate:sometin-cool', responseKey, done

        it 'should place the job in the queue', (done) ->
          @localClient.brpop 'test:internal:authenticate:sometin-cool', 1, (error, result) =>
            return done error if error?
            [channel, responseKey] = result
            expect(responseKey).to.equal 'test:internal:sometin-cool'
            done()

        it 'should not place the job in the remote queue', (done) ->
          @timeout 3000
          @remoteClient.brpop 'test:internal:authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            expect(result).not.to.exist
            done()

  describe '->runJob', ->
    beforeEach ->
      @sut = new QueueWorker
        localClient: redisMock.createClient @localClientId
        remoteClient: redisMock.createClient @remoteClientId
        localHandlers: []
        remoteHandlers: ['authenticate']
        tasks: @tasks
        namespace: 'test'
        timeout: 1

    describe 'when called with an authenticate job', ->
      beforeEach (done) ->
        @timeout 3000
        metadata = uuid: 'uuid', token: 'token', jobType: 'authenticate', responseId: 'cool-beans'
        job = metadata: metadata, rawData: 'null'
        @tasks['meshblu-task-authenticate'] = (job, callback) =>
          callback null, metadata: metadata, rawData: 'bacon is good'
        @sut.runJob job, =>
          @remoteJobManager.getResponse 'cool-beans', (error, @job) => done error

      it 'should have the original metadata', ->
        expect(JSON.stringify @job.metadata).to.equal JSON.stringify uuid: 'uuid', token: 'token', jobType: 'authenticate', responseId: 'cool-beans'

      it 'should have the new rawData', ->
        expect(@job.rawData).to.equal 'bacon is good'

    describe 'when called with a different authenticate job', ->
      beforeEach (done) ->
        @timeout 3000
        metadata = uuid: 'crazy', token: 'business', jobType: 'authenticate', responseId: 'create-madness'
        job = metadata: metadata, rawData: 'null'
        @tasks['meshblu-task-authenticate'] = (job, callback) =>
          callback null, metadata: metadata, rawData: 'something is neat'
        @sut.runJob job, =>
          @remoteJobManager.getResponse 'create-madness', (error, @job) => done error

      it 'should have the original metadata', ->
        expect(JSON.stringify @job.metadata).to.equal JSON.stringify uuid: 'crazy', token: 'business', jobType: 'authenticate', responseId: 'create-madness'

      it 'should have the new rawData', ->
        expect(@job.rawData).to.equal 'something is neat'
