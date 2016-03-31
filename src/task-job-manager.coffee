_            = require 'lodash'
TokenManager = require 'meshblu-core-manager-token'

class TaskJobManager
  constructor: ({@jobManager, cache, pepper, uuidAliasResolver}) ->
    @tokenManager = new TokenManager {cache,pepper,uuidAliasResolver}
    @expireSeconds = @jobManager.timeoutSeconds
    #why use inheritance, when you can make something kinda like it yourself?
    _.defaults @, @jobManager

  createRequest: (requestQueue, options, callback) =>
    # This is important. Figure out why!
    # Hint: message could be big, and we don't want to mutate auth.
    options          = _.clone options
    metadata         = _.cloneDeep options.metadata
    options.metadata = metadata
    {auth}           = metadata

    @tokenManager.generateAndStoreTokenInCache {uuid: auth.uuid, expireSeconds: @expireSeconds}, (error, token) =>
      return callback error if error?
      auth.token = token
      @jobManager.createRequest(requestQueue, options, callback)

module.exports = TaskJobManager
