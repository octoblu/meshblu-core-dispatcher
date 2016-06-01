Dispatcher = require '../src/dispatcher'
async = require 'async'
redis = require 'fakeredis'
uuid = require 'uuid'
moment = require 'moment'
_ = require 'lodash'
JobLogger = require 'job-logger'
JobManager = require 'meshblu-core-job-manager'

describe 'Dispatcher', ->
  beforeEach ->
    @redisKey = uuid.v1()
    @todaySuffix = moment.utc().format('YYYY-MM-DD')

    @dispatchLogger = new JobLogger
      client: _.bindAll redis.createClient @redisKey, dropBufferSupport: true
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:dispatch'
      jobLogQueue: 'dispatch-queue'
      sampleRate: 1.00

    @jobLogger = new JobLogger
      client: _.bindAll redis.createClient @redisKey, dropBufferSupport: true
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:job'
      jobLogQueue: 'job-log-queue'
      sampleRate: 1.00

  describe '-> dispatch', ->
    describe 'when doAuthenticateJob yields a result', ->
      beforeEach ->
        response =
          metadata:
            jobType: 'Authenticate'
            responseId: 'a-response-id'
            code: 200
          rawData: '{ "authenticated": true }'

        @doAuthenticateJob = sinon.stub().yields null, response
        @client = _.bindAll redis.createClient @redisKey, dropBufferSupport: true

        dispatcherClient = _.bindAll redis.createClient @redisKey, dropBufferSupport: true
        jobManager = new JobManager
          client: dispatcherClient
          jobLogSampleRate: 1
          timeoutSeconds: 1

        @sut = new Dispatcher
          jobHandlers:
            Authenticate: @doAuthenticateJob
          dispatchLogger: @dispatchLogger
          memoryLogger: @memoryLogger
          jobLogger: @jobLogger
          jobManager: jobManager

      context 'when the queue contains a request', ->
        beforeEach (done) ->
          metadata =
            jobLogs: ['sampled']
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
            expect(JSON.parse(metadataStr)).to.containSubset
              code: 200
              jobType: 'Authenticate'
              responseId: 'a-response-id'
            done()

        it 'should set the response:data hkey', (done) ->
          @client.hget 'a-response-id', 'response:data', (error, dataStr) =>
            expect(dataStr).to.exist
            expect(JSON.parse(dataStr)).to.deep.equal authenticated: true
            done()

        describe 'when the queue worker inserts into the log queue', ->
          beforeEach (done) ->
            @client.rpop 'dispatch-queue', (error, @dispatcherJobStr) => done error

          beforeEach (done) ->
            @client.rpop 'job-log-queue', (error, @jobStr) => done error

          it 'should log the dispatcher elapsed and error', ->
            job = JSON.parse @dispatcherJobStr

            expect(job).to.containSubset
              index: "metric:meshblu-core-dispatcher:sampled-#{@todaySuffix}"
              type: 'meshblu-core-dispatcher:dispatch'
              body:
                request:
                  metadata:
                    jobType: 'Authenticate'
                    responseId: 'a-response-id'
                    auth:
                      uuid: 'a-uuid'

            expect(job.body.elapsedTime).to.be.within 0, 300 #ms

          it 'should log the job elapsed and error', ->
            job = JSON.parse @jobStr

            expect(job).to.containSubset
              index: "metric:meshblu-core-dispatcher:sampled-#{@todaySuffix}"
              type: 'meshblu-core-dispatcher:job'

            expect(job.body).to.containSubset
              request:
                metadata:
                  jobType: 'Authenticate'
                  responseId: 'a-response-id'
                  auth:
                    uuid: 'a-uuid'
              response:
                metadata:
                  code: 200
                  responseId: 'a-response-id'
                  jobType: 'Authenticate'

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
        @client = _.bindAll redis.createClient @redisKey, dropBufferSupport: true
        jobManager = new JobManager
          client: @client
          jobLogSampleRate: 1
          timeoutSeconds: 1

        @elasticsearch = create: sinon.stub().yields(null)

        @sut = new Dispatcher
          timeout: 1
          jobHandlers:
            Authenticate: @doAuthenticateJob
          dispatchLogger: @dispatchLogger
          memoryLogger: @memoryLogger
          jobLogger: @jobLogger
          jobLogSampleRate: 1
          jobManager: jobManager

      context 'when the queue contains a request', ->
        beforeEach (done) ->
          metadata =
            jobLogs: ['sampled']
            metrics:
              enqueueRequestAt: Date.now()
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
            expect(JSON.parse(metadataStr)).to.containSubset
              code: 504
              responseId: 'a-response-id'
              status: 'Could not rehabilitate server'
            done()

        describe 'when the queue worker inserts into the log queue', ->
          beforeEach (done) ->
            @client.rpop 'dispatch-queue', (error, @dispatcherJobStr) => done error

          beforeEach (done) ->
            @client.rpop 'job-log-queue', (error, @jobStr) => done error

          it 'should log the dispatcher elapsed and error', ->
            job = JSON.parse @dispatcherJobStr

            expect(job).to.containSubset
              index: "metric:meshblu-core-dispatcher:sampled-#{@todaySuffix}"
              type: 'meshblu-core-dispatcher:dispatch'
              body:
                request:
                  metadata:
                    jobType: 'Authenticate'
                    responseId: 'a-response-id'
                    auth:
                      uuid: 'a-uuid'

            expect(job.body.elapsedTime).to.be.within 0, 300 #ms

          it 'should log the job elapsed and error', ->
            job = JSON.parse @jobStr

            expect(job).to.containSubset
              index: "metric:meshblu-core-dispatcher:sampled-#{@todaySuffix}"
              type: 'meshblu-core-dispatcher:job'

            expect(job.body).to.containSubset
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

            expect(job.body.elapsedTime).to.be.within 0, 300 #ms
