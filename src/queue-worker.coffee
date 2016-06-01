_                 = require 'lodash'
async             = require 'async'
UuidAliasResolver = require 'meshblu-uuid-alias-resolver'
debug             = require('debug')('meshblu-core-dispatcher:queue-worker')
TaskRunner        = require './task-runner'

class QueueWorker
  constructor: (options={}) ->
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
    } = options
    @uuidAliasResolver = new UuidAliasResolver
      cache: @cacheFactory.build 'uuid-alias'
      aliasServerUri: aliasServerUri

  run: (callback=->) =>
    @jobManager.getRequest @jobs, (error, job) =>
      debug 'got job', error: error, job: JSON.stringify(job, null, 2)
      return callback error if error?
      return callback null unless job?
      @runJob job, callback

  runJob: (request={}, callback=->) =>
    return callback new Error("Missing metadata") unless request.metadata?
    {jobType,responseId,metrics,jobLogs} = request.metadata

    config = @jobRegistry[jobType]
    debug 'jobRegistry keys', _.keys @jobRegistry
    return callback new Error "jobType '#{jobType}' not found" unless config?

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
    }).run (error, response) =>
      return callback error if error?
      @sendResponse jobType, responseId, response, callback

  sendResponse: (jobType, responseId, response, callback) =>
    debug 'sendResponse', jobType, response
    {metadata,rawData,data} = response

    newResponse =
      metadata:   metadata
      rawData:    rawData
      data:       data

    newResponse.metadata.responseId = responseId

    @jobManager.createResponse jobType, newResponse, callback

module.exports = QueueWorker
