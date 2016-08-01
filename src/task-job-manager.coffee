_            = require 'lodash'
TokenManager = require 'meshblu-core-manager-token'

class TaskJobManager
  constructor: ({@jobManager, cache, datastore, pepper, @ignoreResponse, uuidAliasResolver}) ->
    @tokenManager = new TokenManager {datastore, cache, pepper,uuidAliasResolver}
    @expireSeconds = @jobManager.timeoutSeconds
    @ignoreResponse ?= true
    #why use inheritance, when you can make something kinda like it yourself?
    _.defaults @, @jobManager

  createRequest: (requestQueue, request, callback) =>
    # This is important. Figure out why!
    # Hint: message could be big, and we don't want to mutate auth.
    request                 = _.clone request
    metadata                = _.cloneDeep request.metadata
    metadata.ignoreResponse = @ignoreResponse
    request.metadata        = metadata
    {auth}                  = metadata
    {uuid}                  = auth

    @tokenManager.generateAndStoreTokenInCache { uuid, @expireSeconds }, (error, token) =>
      return callback error if error?
      auth.token = token
      @jobManager.createRequest requestQueue, request, callback

module.exports = TaskJobManager
