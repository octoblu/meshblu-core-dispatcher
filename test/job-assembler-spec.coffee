JobAssembler = require '../src/job-assembler'
redisMock = require 'fakeredis'
uuid = require 'uuid'

describe 'JobAssembler', ->
  beforeEach ->
    @localClientId  = "local-#{uuid.v4()}"
    @remoteClientId = "remote-#{uuid.v4()}"

    @localClient = redisMock.createClient @localClientId
    @remoteClient = redisMock.createClient @remoteClientId

  describe.only '->assemble', ->
    context 'when authenticate is in remoteHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          namespace: 'test:internal'
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: []
          remoteHandlers: ['authenticate']

        @result = @sut.assemble()

      context 'when authenticate is called', ->
        beforeEach (done) ->
          responseKey = 'test:internal:some-response'
          @remoteClient.lpush 'test:internal:authenticate:some-response', responseKey, done

        beforeEach (done) ->
          request =
            metadata:
              duel: "i'm just in it for the glove slapping"
              responseId: 'some-response'
            rawData: null
          @result.authenticate request, done

        it 'should place the job in a queue', (done) ->
          @timeout 3000
          @remoteClient.brpop 'test:internal:authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            [channel, responseKey] = result
            expect(responseKey).to.deep.equal 'test:internal:some-response'
            done()

        it 'should put the metadata in its place', (done) ->
          @remoteClient.hget 'test:internal:some-response', 'request:metadata', (error, metadataStr) =>
            metadata = JSON.parse metadataStr
            expect(metadata).to.deep.equal
              duel: "i'm just in it for the glove slapping"
              responseId: 'some-response'
            done()

        it 'should not place the job in the local queue', (done) ->
          @localClient.llen 'test:internal:authenticate:queue', (error, result) =>
            return done(error) if error?
            expect(result).to.equal 0
            done()

    xcontext 'when authenticate is in localHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          namespace: 'test:internal'
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
