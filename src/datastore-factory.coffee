Datastore = require 'meshblu-core-datastore'
CACHE_REGISTRY = require '../datastore-cache-registry.cson'

class DatastoreFactory
  constructor: ({@database,@cacheFactory}) ->
    throw new Error 'DatastoreFactory: requires database' unless @database?
    throw new Error 'DatastoreFactory: requires cacheFactory' unless @cacheFactory?

  build: (collection) =>
    cache = @cacheFactory.build "datastore:#{collection}"
    {cacheAttributes, useQueryCache} = CACHE_REGISTRY[collection] || {}
    new Datastore {
      @database,
      collection,
      cache,
      cacheAttributes,
      useQueryCache,
    }

module.exports = DatastoreFactory
