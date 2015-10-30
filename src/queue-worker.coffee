_          = require 'lodash'
async      = require 'async'
JobManager = require 'meshblu-core-job-manager'
debug      = require('debug')('meshblu-core-dispatcher:queue-worker')
TaskRunner = require './task-runner'

class QueueWorker
  constructor: (options={}) ->
    {client,@namespace,@timeout,@jobs,@jobRegistry,@pepper} = options
    {@datastoreFactory,@cacheFactory} = options
    @client = _.bindAll client
    @timeout ?= 30
    @namespace ?= 'meshblu:internal'

  run: (callback=->) =>
    debug 'running...'
    async.each @jobs, @handleJob, callback

  getJobManager: (jobType) =>
    new JobManager
      timeoutSeconds: @timeout
      client: @client
      namespace: @namespace
      requestQueue: jobType
      responseQueue: jobType

  handleJob: (jobType, callback) =>
    debug 'running for jobType', jobType

    jobManager = @getJobManager jobType
    jobManager.getRequest (error, job) =>
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
      pepper: @pepper

    taskRunner.run (error, finishedJob) =>
      return callback error if error?
      @sendResponse jobType, finishedJob, callback

  sendResponse: (jobType, response, callback) =>
    jobManager = @getJobManager jobType

    {metadata,rawData} = response
    {responseId} = metadata

    newResponse =
      metadata:   metadata
      responseId: responseId
      rawData:    rawData

    jobManager.createResponse newResponse, callback

module.exports = QueueWorker
