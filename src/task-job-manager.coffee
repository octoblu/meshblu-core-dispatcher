_            = require 'lodash'
TokenManager = require 'meshblu-core-manager-token'

class TaskJobManager
  constructor: ({@jobManager, cache, pepper, @ignoreResponse, uuidAliasResolver}) ->
    @tokenManager = new TokenManager {cache,pepper,uuidAliasResolver}
    @expireSeconds = @jobManager.timeoutSeconds
    @ignoreResponse ?= true
    #why use inheritance, when you can make something kinda like it yourself?
    _.defaults @, @jobManager

  createRequest: (requestQueue, request, callback) =>
    # This is important. Figure out why!
    # Hint: message could be big, and we don't want to mutate auth.
    request                = _.clone request
    request.ignoreResponse = @ignoreResponse
    metadata               = _.cloneDeep request.metadata
    request.metadata       = metadata
    {auth}                 = metadata

    @tokenManager.generateAndStoreTokenInCache {uuid: auth.uuid, expireSeconds: @expireSeconds}, (error, token) =>
      return callback error if error?
      auth.token = token
      @jobManager.createRequest requestQueue, request, callback

module.exports = TaskJobManager
