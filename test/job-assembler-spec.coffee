async     = require 'async'
redisMock = require 'fakeredis'
uuid      = require 'uuid'
_         = require 'lodash'
JobManager = require 'meshblu-core-job-manager'
JobAssembler = require '../src/job-assembler'

describe 'JobAssembler', ->
  beforeEach ->
    @localClientId  = "local-#{uuid.v4()}"
    @remoteClientId = "remote-#{uuid.v4()}"

    @localClient = _.bindAll redisMock.createClient @localClientId
    @remoteClient = _.bindAll redisMock.createClient @remoteClientId

    @localJobManager = new JobManager
      client: @localClient
      timeoutSeconds: 1

  describe '->assemble', ->
    describe 'when authenticate is in remoteHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: []
          remoteHandlers: ['Authenticate']

        @result = @sut.assemble()

      describe 'when authenticate is called', ->
        beforeEach (done) ->
          responseKey = 'some-response'
          responseStr = '{"responseId": "some-response"}'
          async.series [
            async.apply @remoteClient.hset, 'some-response', 'response:metadata', responseStr
            async.apply @remoteClient.lpush, 'Authenticate:some-response', responseKey
          ], done

        beforeEach (done) ->
          request =
            metadata:
              duel: "i'm just in it for the glove slapping"
              responseId: 'some-response'
            rawData: "null"
          @result.Authenticate request, done

        it 'should place the job in a queue', (done) ->
          @timeout 3000
          @remoteClient.brpop 'Authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            [channel, responseKey] = result
            expect(responseKey).to.deep.equal 'some-response'
            done()

        it 'should put the metadata in its place', (done) ->
          @remoteClient.hget 'some-response', 'request:metadata', (error, metadataStr) =>
            metadata = JSON.parse metadataStr
            expect(metadata).to.deep.equal
              duel: "i'm just in it for the glove slapping"
              responseId: 'some-response'
            done()

        it 'should not place the job in the local queue', (done) ->
          @localClient.llen 'Authenticate:queue', (error, result) =>
            return done(error) if error?
            expect(result).to.equal 0
            done()

    describe 'when authenticate is in localHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: ['Authenticate']
          remoteHandlers: []

        @result = @sut.assemble()

      describe 'when authenticate is called', ->
        beforeEach ->
          request =
            metadata:
              misfiled: "paperwork"
              responseId: 'r-id'
            rawData: "null"

          @callback = sinon.spy()
          @result.Authenticate request, @callback

        it 'should place the jobKey in a queue', (done) ->
          @timeout 3000
          @localClient.brpop 'Authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            [channel,jobKey] = result
            expect(jobKey).to.deep.equal 'r-id'
            done()

        it 'should not place the job in the remote queue', (done) ->
          @timeout 3000
          @remoteClient.brpop 'Authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            expect(result).not.to.exist
            done()

        describe 'when authenticate responds', ->
          beforeEach (done) ->
            options =
              metadata:
                responseId: 'r-id'
              data:
                authenticated: true

            @localJobManager.createResponse 'Authenticate', options, done

            metadataStr = '{"responseId": "r-id"}'
            dataStr     = '{"authenticated": true}'

          it 'should call the callback with the response', (done) ->
            setTimeout =>
              expectedResponse =
                metadata:
                  responseId: 'r-id'
                rawData: '{"authenticated":true}'
              expect(@callback).to.have.been.calledWith null, expectedResponse
              done()
            , 1000

        describe 'when authenticate responds differently', ->
          beforeEach (done) ->
            options =
              metadata:
                responseId: 'r-id'
              data:
                authenticated: false

            @localJobManager.createResponse 'Authenticate', options, done

          it 'should call the callback with the response', (done) ->
            setTimeout =>
              expectedResponse =
                metadata:
                  responseId: 'r-id'
                rawData: '{"authenticated":false}'

              expect(@callback).to.have.been.calledWith null, expectedResponse
              done()
            , 1000
