QueueWorker = require '../src/queue-worker'
redisMock   = require 'fakeredis'
_           = require 'lodash'
uuid        = require 'uuid'

describe 'QueueWorker', ->
  beforeEach ->
    @localClientId  = "local-#{uuid.v4()}"
    @remoteClientId = "remote-#{uuid.v4()}"

    @localClient = _.bindAll redisMock.createClient @localClientId
    @remoteClient = _.bindAll redisMock.createClient @remoteClientId

    @meshbluTaskAuthenticate = sinon.spy()

  describe '->run', ->
    describe 'when using remoteClient', ->
      beforeEach ->
        @sut = new QueueWorker
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: []
          remoteHandlers: ['authenticate']
          tasks:
            'meshblu-task-authenticate': @meshbluTaskAuthenticate
          namespace: 'test:internal'
          timeout: 1

      describe 'when called and job is pushed into queue', ->
        beforeEach (done) ->
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
          tasks:
            'meshblu-task-authenticate': @meshbluTaskAuthenticate
          namespace: 'test:internal'
          timeout: 1

      describe 'when called and job is pushed into queue', ->
        beforeEach (done) ->
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
