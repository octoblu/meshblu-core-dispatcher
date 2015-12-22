Dispatcher = require '../src/dispatcher'
async = require 'async'
redis = require 'fakeredis'
uuid = require 'uuid'
_ = require 'lodash'

describe 'Dispatcher', ->
  describe '-> dispatch', ->
    describe 'instantiated without an elastic search client', ->
      describe 'when doAuthenticateJob yields a result', ->
        beforeEach ->
          response =
            metadata:
              jobType: 'Authenticate'
              responseId: 'a-response-id'
            rawData: '{ "authenticated": true }'

          @doAuthenticateJob = sinon.stub().yields null, response
          @client = _.bindAll redis.createClient uuid.v1()

          @sut = new Dispatcher
            client: @client
            timeout: 1
            jobHandlers:
              Authenticate: @doAuthenticateJob

        context 'when the queue contains a request', ->
          beforeEach (done) ->
            metadata =
              jobType: 'Authenticate'
              responseId: 'a-response-id'

            async.series [
              async.apply @client.hset, 'a-response-id', 'request:metadata', JSON.stringify(metadata)
              async.apply @client.lpush, 'request:queue', 'a-response-id'
            ], done

          beforeEach (done) ->
            @sut.dispatch done

          it 'should remove the job from the queue', (done) ->
            @client.llen 'request:queue', (error, llen) =>
              return done error if error?
              expect(llen).to.equal 0
              done()

          it 'should call the Authenticate', ->
            expect(@doAuthenticateJob).to.have.been.called

          it 'should respond with the result', (done) ->
            @timeout(3000)

            @client.brpop 'response:a-response-id', 1, (error, result) =>
              return done error if error?
              [channel,responseKey] = result

              expect(responseKey).to.deep.equal 'a-response-id'
              done()

          it 'should set the response:metadata hkey', (done) ->
            @client.hget 'a-response-id', 'response:metadata', (error, metadataStr) =>
              expect(metadataStr).to.exist
              expect(JSON.parse(metadataStr)).to.deep.equal jobType: 'Authenticate', responseId: 'a-response-id'
              done()

          it 'should set the response:data hkey', (done) ->
            @client.hget 'a-response-id', 'response:data', (error, dataStr) =>
              expect(dataStr).to.exist
              expect(JSON.parse(dataStr)).to.deep.equal authenticated: true
              done()

        context 'when the queue is empty', ->
          beforeEach (done) ->
            async.series [
              async.apply @client.del, 'request:queue'
              async.apply @client.del, 'response:a-response-id'
            ], done

          it 'should call the callback', (done) ->
            @timeout 3000
            @sut.dispatch done

    describe 'instantiated with an elastic search client', ->
      describe 'when doAuthenticateJob yields a result', ->
        beforeEach ->
          response =
            metadata:
              jobType: 'Authenticate'
              responseId: 'a-response-id'
              code: 200
            rawData: '{ "authenticated": true }'

          @doAuthenticateJob = sinon.stub().yields null, response
          @client = _.bindAll redis.createClient uuid.v1()
          @elasticsearch = create: sinon.stub().yields(null)

          @sut = new Dispatcher
            client: @client
            timeout: 1
            elasticsearch: @elasticsearch
            jobHandlers:
              Authenticate: @doAuthenticateJob

        context 'when the queue contains a request', ->
          beforeEach (done) ->
            metadata =
              jobType: 'Authenticate'
              responseId: 'a-response-id'
              auth:
                uuid: 'a-uuid'
                token: 'a-token'

            async.series [
              async.apply @client.hset, 'a-response-id', 'request:metadata', JSON.stringify(metadata)
              async.apply @client.lpush, 'request:queue', 'a-response-id'
            ], done

          beforeEach (done) ->
            @sut.dispatch done

          it 'should remove the job from the queue', (done) ->
            @client.llen 'request:queue', (error, llen) =>
              return done error if error?
              expect(llen).to.equal 0
              done()

          it 'should call the Authenticate', ->
            expect(@doAuthenticateJob).to.have.been.called

          it 'should log the elapsed time', ->
            expect(@elasticsearch.create).to.have.been.calledWith
              index: 'meshblu_job'
              type: 'dispatcher'
              body:
                elapsedTime: 0
                request:
                  metadata:
                    jobType: 'Authenticate'
                    responseId: 'a-response-id'
                    auth:
                      uuid: 'a-uuid'
                response:
                  metadata:
                    jobType: 'Authenticate'
                    responseId: 'a-response-id'
                    code: 200

          it 'should respond with the result', (done) ->
            @timeout(3000)

            @client.brpop 'response:a-response-id', 1, (error, result) =>
              return done error if error?
              [channel,responseKey] = result

              expect(responseKey).to.deep.equal 'a-response-id'
              done()

          it 'should set the response:metadata hkey', (done) ->
            @client.hget 'a-response-id', 'response:metadata', (error, metadataStr) =>
              expect(metadataStr).to.exist
              expect(JSON.parse(metadataStr)).to.deep.equal
                code: 200
                jobType: 'Authenticate'
                responseId: 'a-response-id'
              done()

          it 'should set the response:data hkey', (done) ->
            @client.hget 'a-response-id', 'response:data', (error, dataStr) =>
              expect(dataStr).to.exist
              expect(JSON.parse(dataStr)).to.deep.equal authenticated: true
              done()

        context 'when the queue is empty', ->
          beforeEach (done) ->
            async.series [
              async.apply @client.del, 'request:queue'
              async.apply @client.del, 'response:a-response-id'
            ], done

          it 'should call the callback', (done) ->
            @timeout 3000
            @sut.dispatch done

      describe 'when doAuthenticateJob yields an error', ->
        beforeEach ->
          @doAuthenticateJob = sinon.stub().yields new Error('Could not rehabilitate server')
          @client = _.bindAll redis.createClient uuid.v1()
          @elasticsearch = create: sinon.stub().yields(null)

          @sut = new Dispatcher
            client: @client
            timeout: 1
            elasticsearch: @elasticsearch
            jobHandlers:
              Authenticate: @doAuthenticateJob

        context 'when the queue contains a request', ->
          beforeEach (done) ->
            metadata =
              jobType: 'Authenticate'
              responseId: 'a-response-id'
              auth:
                uuid: 'a-uuid'
                token: 'a-token'

            async.series [
              async.apply @client.hset, 'a-response-id', 'request:metadata', JSON.stringify(metadata)
              async.apply @client.lpush, 'request:queue', 'a-response-id'
            ], done

          beforeEach (done) ->
            @sut.dispatch done

          it 'should remove the job from the queue', (done) ->
            @client.llen 'request:queue', (error, llen) =>
              return done error if error?
              expect(llen).to.equal 0
              done()

          it 'should call the Authenticate', ->
            expect(@doAuthenticateJob).to.have.been.called

          it 'should log the elapsed and error', ->
            expect(@elasticsearch.create).to.have.been.calledOnce
            [arg1] = @elasticsearch.create.firstCall.args

            expect(arg1.index).to.deep.equal 'meshblu_job'
            expect(arg1.type).to.deep.equal 'dispatcher'
            expect(arg1.body).to.containSubset
              request:
                metadata:
                  jobType: 'Authenticate'
                  responseId: 'a-response-id'
                  auth:
                    uuid: 'a-uuid'
              response:
                metadata:
                  code: 504
                  responseId: 'a-response-id'
                  status: 'Could not rehabilitate server'

          it 'should respond with the result', (done) ->
            @timeout(3000)

            @client.brpop 'response:a-response-id', 1, (error, result) =>
              return done error if error?
              [channel,responseKey] = result

              expect(responseKey).to.deep.equal 'a-response-id'
              done()

          it 'should set the response:metadata hkey', (done) ->
            @client.hget 'a-response-id', 'response:metadata', (error, metadataStr) =>
              expect(metadataStr).to.exist
              expect(JSON.parse(metadataStr)).to.deep.equal
                code: 504
                responseId: 'a-response-id'
                status: 'Could not rehabilitate server'
              done()
