Dispatcher = require '../src/dispatcher'
async = require 'async'
redis = require 'fakeredis'
uuid = require 'uuid'
_ = require 'lodash'

describe 'Dispatcher', ->
  describe '-> dispatch', ->
    beforeEach ->
      response =
        metadata:
          jobType: 'authenticate'
          responseId: 'a-response-id'
        rawData: '{ "authenticated": true }'

      @doAuthenticateJob = sinon.stub().yields null, response
      @client = redis.createClient uuid.v1()
      @client = _.bindAll @client

      @sut = new Dispatcher
        client: @client
        timeout: 1
        jobHandlers:
          authenticate: @doAuthenticateJob

    context 'when the queue contains a request', ->
      beforeEach (done) ->
        metadata =
          jobType: 'authenticate'
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

      it 'should call the authenticate', ->
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
          expect(JSON.parse(metadataStr)).to.deep.equal jobType: 'authenticate', responseId: 'a-response-id'
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
