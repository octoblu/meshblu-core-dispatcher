debug = require('debug')('meshblu-core-dispatcher:job-handler')

class JobHandler
  constructor: (options) ->
    {
      @jobs
      @jobRegistry
      @pepper
      aliasServerUri
      @meshbluConfig
      @forwardEventDevices
      @workerName
      @privateKey
      @publicKey
      @datastoreFactory
      @cacheFactory
      @taskLogger
      @ignoreResponse
      @jobManager
      @externalJobManager
      @jobType
    } = options

  handle: (request, callback) =>
    {metadata, rawData} = request

    request =
      metadata: metadata
      rawData: rawData

    config = @jobRegistry[@jobType]

    new TaskRunner({
      config
      request
      @datastoreFactory
      @cacheFactory
      @uuidAliasResolver
      @pepper
      @meshbluConfig
      @forwardEventDevices
      @workerName
      @privateKey
      @publicKey
      @taskLogger
      @ignoreResponse
      jobManager: @externalJobManager
      @uuidAliasResolver
    }).run (error, response) =>
      return callback error if error?
      debug @jobType, response.metadata.code
      callback null, response

module.exports = JobHandler
