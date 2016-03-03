QueueWorker = require '../src/queue-worker'
JobManager  = require 'meshblu-core-job-manager'
redis   = require 'fakeredis'
_           = require 'lodash'
async       = require 'async'
uuid        = require 'uuid'
mongojs     = require 'mongojs'
Cache       = require 'meshblu-core-cache'
Datastore   = require 'meshblu-core-datastore'
RedisNS     = require '@octoblu/redis-ns'
moment      = require 'moment'
JobLogger   = require 'job-logger'

describe 'QueueWorker', ->
  beforeEach ->
    mongoHost = process.env.MONGODB_HOST ? 'localhost'
    mongoPort = process.env.MONGODB_PORT ? '27017'
    @database = mongojs "#{mongoHost}:#{mongoPort}/meshblu-core-test"

    @redisKey = uuid.v1()
    @cacheClientId = uuid.v1()
    @client = _.bindAll redis.createClient @redisKey
    @externalClient = _.bindAll redis.createClient @redisKey

    @datastoreFactory =
      build: (collection) =>
        new Datastore
          database: @database
          collection: collection

    @cacheFactory =
      build: (namespace) =>
        rawClient = redis.createClient @cacheClientId
        client = _.bindAll new RedisNS namespace, rawClient
        new Cache
          client: client

  beforeEach ->
    @todaySuffix = moment.utc().format('YYYY-MM-DD')

    @taskLogger = new JobLogger
      client: redis.createClient @redisKey
      indexPrefix: 'metric:meshblu-core-dispatcher'
      type: 'meshblu-core-dispatcher:task'
      jobLogQueue: 'some-queue'
      sampleRate: 1.00

  describe '->run', ->
    describe 'when using client', ->
      beforeEach ->
        jobRegistry =
          CheckToken:
            start: 'check-token'
            tasks:
              'check-token':
                task: 'meshblu-core-task-check-token'
                datastoreCollection: 'devices'

        @sut = new QueueWorker
          client: _.bindAll redis.createClient @redisKey
          localHandlers: ['CheckToken']
          remoteHandlers: []
          tasks: @tasks
          timeout: 1
          datastoreFactory: @datastoreFactory
          cacheFactory: @cacheFactory
          jobRegistry: jobRegistry
          externalClient: @externalClient
          taskLogger: @taskLogger

      describe 'when called and job is pushed into queue', ->
        beforeEach (done) ->
          @sut.run()
          responseKey = 'sometin'
          @client.lpush 'CheckToken:sometin', responseKey, done

        it 'should place the job in the queue', (done) ->
          @client.brpop 'CheckToken:sometin', 1, (error, result) =>
            return done error if error?
            [channel, responseKey] = result
            expect(responseKey).to.equal 'sometin'
            done()

      describe 'when called and different job is pushed into queue', ->
        beforeEach (done) ->
          @sut.run()
          responseKey = 'sometin-cool'
          @client.lpush 'CheckToken:sometin-cool', responseKey, done

        it 'should place the job in the queue', (done) ->
          @client.brpop 'CheckToken:sometin-cool', 1, (error, result) =>
            return done error if error?
            [channel, responseKey] = result
            expect(responseKey).to.equal 'sometin-cool'
            done()

        it 'should not place the job in the remote queue', (done) ->
          @timeout 3000
          @client.brpop 'CheckToken:queue', 1, (error, result) =>
            return done(error) if error?
            expect(result).not.to.exist
            done()

  describe '->runJob', ->
    beforeEach ->
      jobRegistry =
        CheckToken:
          start: 'check-token'
          tasks:
            'check-token':
              task: 'meshblu-core-task-check-token'
              datastoreCollection: 'theDevices'

      @sut = new QueueWorker
        client: _.bindAll redis.createClient @redisKey
        localHandlers: ['CheckToken']
        remoteHandlers: []
        tasks: @tasks
        timeout: 1
        datastoreFactory: @datastoreFactory
        cacheFactory: @cacheFactory
        jobRegistry: jobRegistry
        pepper: 'super-duper-secret'
        externalClient: @externalClient
        taskLogger: @taskLogger

    describe 'when called with an CheckToken job', ->
      beforeEach (done) ->
        datastore = new Datastore
          database: @database
          collection: 'theDevices'

        record =
          uuid: 'it-takes-a-real-hero-to-admit-when'
          token: 'this-is-the-root-token-ignore-it'
          meshblu:
            tokens: 'sUQVYy0qd0YLNSkulRP1fCAJeVBjrD9ppZoMo/p51YE=': {}

        async.series [
          async.apply datastore.remove
          async.apply datastore.insert, record
        ], done

      beforeEach (done) ->
        @timeout 3000

        request =
          metadata:
            jobType: 'CheckToken'
            responseId: 'tragic-flaw'
            auth:
              uuid: 'it-takes-a-real-hero-to-admit-when'
              token: 'doesn-t-matter-something'

        @sut.runJob request, (error) =>
          return done error if error?

          jobManager = new JobManager
            client: _.bindAll redis.createClient @redisKey
            timeoutSeconds: 1

          jobManager.getResponse 'CheckToken', 'tragic-flaw', (error, @response) =>
            done error

      it 'should have a valid response', ->
        expect(@response).to.deep.equal
          metadata:
            responseId: 'tragic-flaw'
            code: 204
            status: 'No Content'
          rawData: 'null'

  describe '->runJob with cache', ->
    beforeEach ->
      jobRegistry =
        CheckBlackList:
          start: 'check-black-list'
          tasks:
            'check-black-list':
              task: 'meshblu-core-task-check-token-black-list'
              cacheNamespace: 'black-list'

      @sut = new QueueWorker
        client: _.bindAll redis.createClient @redisKey
        localHandlers: ['CheckBlackList']
        remoteHandlers: []
        tasks: @tasks
        timeout: 1
        cacheFactory: @cacheFactory
        jobRegistry: jobRegistry
        pepper: 'super-duper-secret'
        externalClient: @externalClient
        taskLogger: @taskLogger

    describe 'when called with an CheckBlackList job', ->
      beforeEach (done) ->
        cacheClient = _.bindAll new RedisNS 'black-list', redis.createClient @cacheClientId
        cacheClient.set 'things-go-wrong:but-didnt-it-feel-so-right', '', done

      beforeEach (done) ->
        @timeout 3000

        request =
          metadata:
            jobType: 'CheckBlackList'
            responseId: 'roasted'
            auth:
              uuid: 'things-go-wrong'
              token: 'but-didnt-it-feel-so-right'

        @sut.runJob request, (error) =>
          return done error if error?
          jobManager = new JobManager
            client: _.bindAll redis.createClient @redisKey
            timeoutSeconds: 1
          jobManager.getResponse 'CheckBlackList', 'roasted', (error, @response) =>
            done error

      it 'should have a valid response', ->
        expect(@response).to.deep.equal
          metadata:
            responseId: 'roasted'
            code: 403
            status: 'Forbidden'
          rawData: 'null'
