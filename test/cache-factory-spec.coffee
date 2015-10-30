CacheFactory = require '../src/cache-factory'
Cache = require 'meshblu-core-cache'
redis = require 'fakeredis'
uuid  = require 'uuid'

describe 'CacheFactory', ->
  beforeEach ->
    @clientId = uuid.v1()
    @client = new Cache client: redis.createClient @clientId

    @sut = new CacheFactory client: redis.createClient(@clientId)

  describe '-> build', ->
    beforeEach (done) ->
      @cache = @sut.build('impatient-vulture')
      @cache.set 'keep-calm-and-carrion', 'ugh', done

    it 'should make a cache in the namespace', (done) ->
      @client.get 'impatient-vulture:keep-calm-and-carrion', (error,record) ->
        return done error if error?
        expect(record).to.deep.equal 'ugh'
        done()
