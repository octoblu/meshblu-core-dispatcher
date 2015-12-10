_       = require 'lodash'
Cache   = require 'meshblu-core-cache'
RedisNS = require '@octoblu/redis-ns'

class CacheFactory
  constructor: ({@client}) ->

  build: (namespace) =>
    redisNSClientWithRedisClient = _.bindAll new RedisNS namespace, @client
    new Cache client: redisNSClientWithRedisClient

module.exports = CacheFactory
