JobAssembler = require '../src/job-assembler'
redisMock = require 'fakeredis'
uuid = require 'uuid'

describe 'JobAssembler', ->
  beforeEach ->
    @localClient = redisMock.createClient("local-#{uuid.v4()}")
    @remoteClient = redisMock.createClient("remote-#{uuid.v4()}")

  describe '->assemble', ->
    context 'when authenticate is in remoteHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          namespace: 'test'
          localClient: @localClient
          remoteClient: @remoteClient
          localHandlers: []
          remoteHandlers: ['authenticate']

        @result = @sut.assemble()

      context 'when authenticate is called', ->
        beforeEach ->
          @result.authenticate ["duel: i'm just in it for the glove slapping"], ->

        it 'should place the job in a queue', (done) ->
          @timeout 3000
          @remoteClient.brpop 'test:authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            [channel, requestStr] = result
            request = JSON.parse requestStr
            expect(request).to.deep.equal ["duel: i'm just in it for the glove slapping"]
            done()

        it 'should not place the job in the local queue', (done) ->
          @localClient.llen 'test:authenticate:queue', (error, result) =>
            return done(error) if error?
            expect(result).to.equal 0
            done()

    context 'when authenticate is in localHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          namespace: 'test'
          localClient: @localClient
          remoteClient: @remoteClient
          localHandlers: ['authenticate']
          remoteHandlers: []

        @result = @sut.assemble()

      it 'should return a map of jobHandlers', ->
        expect(@result).to.be.an 'object'

      context 'when authenticate is called', ->
        beforeEach ->
          @callback = sinon.spy()
          @result.authenticate [{responseId: 'r-id'}, "duel: i'm just in it for the glove slapping"], @callback

        it 'should place the job in a queue', (done) ->
          @timeout 3000
          @localClient.brpop 'test:authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            [channel, requestStr] = result
            request = JSON.parse requestStr
            expect(request).to.deep.equal [{responseId: 'r-id'}, "duel: i'm just in it for the glove slapping"]
            done()

        it 'should not place the job in the remote queue', (done) ->
          @remoteClient.llen 'test:authenticate:queue', (error, result) =>
            return done(error) if error?
            expect(result).to.equal 0
            done()

        context 'when authenticate responds', ->
          beforeEach (done) ->
            response = JSON.stringify [{responseId: 'r-id'}, {authenticated: true}]
            @localClient.lpush 'test:authenticate:r-id', response, done

          it 'should call the callback with the response', (done) ->
            setTimeout =>
              expect(@callback).to.have.been.calledWith null, [{responseId: 'r-id'}, {authenticated: true}]
              done()
            , 1000

        context 'when authenticate responds differently', ->
          beforeEach (done) ->
            response = JSON.stringify [{responseId: 'r-id'}, {authenticated: false}]
            @localClient.lpush 'test:authenticate:r-id', response, done

          it 'should call the callback with the response', (done) ->
            setTimeout =>
              expect(@callback).to.have.been.calledWith null, [{responseId: 'r-id'}, {authenticated: false}]
              done()
            , 1000
