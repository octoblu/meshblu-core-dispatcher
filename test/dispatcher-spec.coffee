Dispatcher = require '../src/dispatcher'
async = require 'async'
redis = require 'redis'
_ = require 'lodash'

describe 'Dispatcher', ->
  beforeEach (done) ->
    @sut = new Dispatcher
    @client = redis.createClient process.env.REDIS_URI
    @client = _.bindAll @client

    jorb = {responseUuid: 'a-response-uuid'}

    async.series [
      async.apply @client.del, 'test:request:queue'
      async.apply @client.lpush, 'test:request:queue', JSON.stringify(jorb)
    ], done

  describe '-> work', ->
    beforeEach (done) ->
      @sut.dispatch done

    it 'should remove the job from the queue', (done) ->
      @client.llen 'test:request:queue', (error, llen) =>
        return done error if error?
        expect(llen).to.equal 0
        done()

    it 'should call the authenticate', (done) ->
      @client.llen 'test:authenticate:queue', (error, llen) =>
        return done error if error?
        expect(llen).to.equal 0
        done()
