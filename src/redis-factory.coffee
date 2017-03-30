Cache   = require 'meshblu-core-cache'
RedisNS = require '@octoblu/redis-ns'

class RedisFactory
  constructor: ({@client}) ->
    throw new Error 'RedisFactory: requires client' unless @client?

  build: (namespace) =>
    redisNSClientWithRedisClient = new RedisNS namespace, @client
    new Cache client: redisNSClientWithRedisClient

module.exports = RedisFactory
