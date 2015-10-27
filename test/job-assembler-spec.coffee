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
      namespace: 'test:internal'
      timeoutSeconds: 1
      responseQueue: 'authenticate'
      requestQueue: 'authenticate'

    @remoteJobManager = new JobManager
      client: @remoteClient
      namespace: 'test'
      timeoutSeconds: 1

  describe '->assemble', ->
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
            rawData: "null"
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

    context 'when authenticate is in localHandlers', ->
      beforeEach ->
        @sut = new JobAssembler
          timeout: 1
          namespace: 'test:internal'
          localClient: redisMock.createClient @localClientId
          remoteClient: redisMock.createClient @remoteClientId
          localHandlers: ['authenticate']
          remoteHandlers: []

        @result = @sut.assemble()

      context 'when authenticate is called', ->
        beforeEach ->
          request =
            metadata:
              misfiled: "paperwork"
              responseId: 'r-id'
            rawData: "null"

          @callback = sinon.spy()
          @result.authenticate request, @callback

        it 'should place the jobKey in a queue', (done) ->
          @timeout 3000
          @localClient.brpop 'test:internal:authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            [channel,jobKey] = result
            expect(jobKey).to.deep.equal 'test:internal:r-id'
            done()

        it 'should not place the job in the remote queue', (done) ->
          @timeout 3000
          @remoteClient.brpop 'test:internal:authenticate:queue', 1, (error, result) =>
            return done(error) if error?
            expect(result).not.to.exist
            done()

        context 'when authenticate responds', ->
          beforeEach (done) ->
            metadata =
              responseId: 'r-id'
            data =
              authenticated: true

            options =
              responseId: 'r-id'
              metadata: metadata
              data: data

            @localJobManager.createResponse options, done

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

        context 'when authenticate responds differently', ->
          beforeEach (done) ->
            metadata =
              responseId: 'r-id'
            data =
              authenticated: false

            options =
              responseId: 'r-id'
              metadata: metadata
              data: data

            @localJobManager.createResponse options, done

          it 'should call the callback with the response', (done) ->
            setTimeout =>
              expectedResponse =
                metadata:
                  responseId: 'r-id'
                rawData: '{"authenticated":false}'

              expect(@callback).to.have.been.calledWith null, expectedResponse
              done()
            , 1000
