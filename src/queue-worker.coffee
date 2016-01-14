_          = require 'lodash'
async      = require 'async'
UuidAliasResolver = require './uuid-alias-resolver'
JobManager = require 'meshblu-core-job-manager'
debug      = require('debug')('meshblu-core-dispatcher:queue-worker')
TaskRunner = require './task-runner'

class QueueWorker
  constructor: (options={}) ->
    {client,@timeout,@jobs,@jobRegistry,@pepper,aliasServerUri,@meshbluConfig,@forwardEventDevices} = options
    {@externalClient,@logJobs} = options
    {@datastoreFactory,@cacheFactory} = options
    @client = _.bindAll client
    @timeout ?= 30
    @jobManager = new JobManager timeoutSeconds: @timeout, client: @client
    @uuidAliasResolver = new UuidAliasResolver
      cache: @cacheFactory.build 'uuid-alias'
      aliasServerUri: aliasServerUri

  run: (callback=->) =>
    debug 'running...'

    @jobManager.getRequest @jobs, (error, job) =>
      debug 'got job', error: error, job: job
      return callback error if error?
      return callback null unless job?
      @runJob job, callback

  runJob: (request={}, callback=->) =>
    return callback new Error("Missing metadata") unless request.metadata?
    {jobType,responseId} = request.metadata

    config = @jobRegistry[jobType]
    return callback new Error "jobType '#{jobType}' not found" unless config?

    startTime = Date.now()
    new TaskRunner({
      config
      request
      @datastoreFactory
      @cacheFactory
      @uuidAliasResolver
      @pepper
      @meshbluConfig
      @forwardEventDevices
      jobManager: new JobManager timeoutSeconds: @timeout, client: @externalClient
    }).run (error, response) =>
      return callback error if error?
      @logTask {startTime, request, response}, =>
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

  logTask: (options, callback) =>
    options.type = 'task'
    @_log options, callback

  _log: ({startTime, request, response, type}, callback) =>
    return callback() unless @logJobs
    requestMetadata = _.cloneDeep request.metadata
    delete requestMetadata.auth?.token

    job =
      index: "#{@indexName}-#{@todaySuffix}"
      type: type
      body:
        elapsedTime: Date.now() - startTime
        date: Date.now()
        request:
          metadata: requestMetadata
        response: _.pick(response, 'metadata')

    debug '_log', job

    @client.lpush 'job-log', JSON.stringify(job), (error, result) =>
      console.error 'Dispatcher.log', {error} if error?
      callback error

module.exports = QueueWorker
