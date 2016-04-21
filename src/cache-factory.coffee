_       = require 'lodash'
Cache   = require 'meshblu-core-cache'
RedisNS = require '@octoblu/redis-ns'

class CacheFactory
  constructor: ({@client}) ->
    throw new Error 'CacheFactory: requires client' unless @client?

  build: (namespace) =>
    redisNSClientWithRedisClient = new RedisNS namespace, @client
    new Cache client: redisNSClientWithRedisClient

module.exports = CacheFactory
