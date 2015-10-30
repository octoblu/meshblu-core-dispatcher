Datastore = require 'meshblu-core-datastore'
class DatastoreFactory
  constructor: ({@database}) ->

  build: (collection) =>
    new Datastore
      database: @database
      collection: collection

module.exports = DatastoreFactory
