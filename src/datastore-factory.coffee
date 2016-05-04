path      = require 'path'
cson      = require 'cson'
Datastore = require 'meshblu-core-datastore'

class DatastoreFactory
  constructor: ({@database,@cacheFactory}) ->
    throw new Error 'DatastoreFactory: requires database' unless @database?
    throw new Error 'DatastoreFactory: requires cacheFactory' unless @cacheFactory?
    @cacheRegistry = cson.parseFile(path.join __dirname, '../datastore-cache-registry.cson')

  build: (collection) =>
    cache = @cacheFactory.build "datastore:#{collection}"
    {cacheAttributes, useQueryCache} = @cacheRegistry[collection] || {}
    new Datastore {@database, collection, cache, cacheAttributes, useQueryCache}

module.exports = DatastoreFactory
