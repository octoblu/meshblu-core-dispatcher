_          = require 'lodash'
async      = require 'async'
UuidAliasResolver = require './uuid-alias-resolver'
JobManager = require 'meshblu-core-job-manager'
debug      = require('debug')('meshblu-core-dispatcher:queue-worker')
TaskRunner = require './task-runner'

class QueueWorker
  constructor: (options={}) ->
    {client,@timeout,@jobs,@jobRegistry,@pepper,aliasServerUri} = options
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

  runJob: (job={}, callback=->) =>
    return callback new Error("Missing metadata") unless job.metadata?
    {jobType,responseId} = job.metadata

    jobDef = @jobRegistry[jobType]
    return callback new Error "jobType '#{jobType}' not found" unless jobDef?

    taskRunner = new TaskRunner
      config: jobDef
      request: job
      datastoreFactory: @datastoreFactory
      cacheFactory: @cacheFactory
      uuidAliasResolver: @uuidAliasResolver
      pepper: @pepper

    taskRunner.run (error, finishedJob) =>
      return callback error if error?
      @sendResponse jobType, responseId, finishedJob, callback

  sendResponse: (jobType, responseId, response, callback) =>
    debug 'sendResponse', jobType, response
    {metadata,rawData} = response

    newResponse =
      metadata:   metadata
      rawData:    rawData

    newResponse.metadata.responseId = responseId

    @jobManager.createResponse jobType, newResponse, callback

module.exports = QueueWorker
