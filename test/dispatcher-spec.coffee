Dispatcher = require '../src/dispatcher'
async = require 'async'
redis = require 'redis'
_ = require 'lodash'

describe 'Dispatcher', ->
  describe '-> dispatch', ->
    beforeEach ->
      response = [{jobType: 'authenticate', responseId: 'a-response-id'}, {authenticated: true}]
      @doAuthenticateJob = sinon.stub().yields null, response

      @sut = new Dispatcher
        namespace: 'test'
        timeout: 1
        jobHandlers:
          authenticate: @doAuthenticateJob

    context 'when the queue contains a request', ->
      beforeEach (done) ->
        @client = redis.createClient process.env.REDIS_URI
        @client = _.bindAll @client

        request = [{jobType: 'authenticate', responseId: 'a-response-id'}]

        async.series [
          async.apply @client.del, 'test:request:queue'
          async.apply @client.del, 'test:response:a-response-id'
          async.apply @client.lpush, 'test:request:queue', JSON.stringify(request)
        ], done

      beforeEach (done) ->
        @sut.dispatch done

      it 'should remove the job from the queue', (done) ->
        @client.llen 'test:request:queue', (error, llen) =>
          return done error if error?
          expect(llen).to.equal 0
          done()

      it 'should call the authenticate', ->
        expect(@doAuthenticateJob).to.have.been.called

      it 'should respond with the result', (done) ->
        @timeout(3000)

        @client.brpop 'test:response:a-response-id', 1, (error, result) =>
          return done error if error?
          expect(result).to.exist
          [channel,response] = result

          expectedResponse = [
            {jobType: 'authenticate', responseId: 'a-response-id'}
            {authenticated: true}
          ]

          expect(JSON.parse(response)).to.deep.equal expectedResponse
          done()

    context 'when the queue is empty', ->
      beforeEach (done) ->
        @client = redis.createClient process.env.REDIS_URI
        @client = _.bindAll @client

        async.series [
          async.apply @client.del, 'test:request:queue'
          async.apply @client.del, 'test:response:a-response-id'
        ], done

      it 'should call the callback', (done) ->
        @timeout 3000
        @sut.dispatch done
