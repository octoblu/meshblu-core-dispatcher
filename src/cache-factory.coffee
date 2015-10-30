Cache   = require 'meshblu-core-cache'
RedisNS = require 'redis-ns'

class CacheFactory
  constructor: ({@client}) ->

  build: (namespace) =>
    redisNSClientWithRedisClient = new RedisNS namespace, @client
    new Cache client: redisNSClientWithRedisClient

module.exports = CacheFactory
