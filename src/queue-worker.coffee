_          = require 'lodash'
async      = require 'async'
configJobs = require './config/jobs'
JobManager = require 'meshblu-core-job-manager'
debug      = require('debug')('meshblu-core-dispatcher:queue-worker')

class QueueWorker
  constructor: (options={}) ->
    {client,@namespace,@timeout,@tasks,@jobs} = options
    @client = _.bindAll client
    @timeout ?= 30
    @namespace ?= 'meshblu:internal'
    @tasks ?= {}
    @tasks['meshblu-task-authenticate'] ?= require('meshblu-task-authenticate')
    debug 'timeout', @timeout, @namespace

    @jobManager = new JobManager
      timeoutSeconds: @timeout
      client: @client
      namespace: @namespace
      requestQueue: 'authenticate'
      responseQueue: 'authenticate'

  run: (callback=->) =>
    debug 'running'
    async.each @jobs, @handleJob, callback

  handleJob: (jobType, callback) =>
    debug 'running for jobType', jobType
    @jobManager.getRequest (error, job) =>
      debug 'got job', error: error, job: job
      return callback error if error?
      return callback null unless job?
      @runJob job, callback

  runJob: (job={}, callback=->) =>
    return callback new Error("Missing metadata") unless job.metadata?
    {jobType,responseId} = job.metadata

    task = @tasks['meshblu-task-authenticate']
    task job, (error, finishedJob) =>
      debug 'finished job'
      return callback error if error?
      @sendResponse finishedJob, callback

  runTask: (task, originalJob) =>
    return (job, callback=->) =>
      if _.isFunction job
        callback = job
        job = originalJob
      taskFunction = @tasks[task]

      return callback new Error("missing task") unless taskFunction?
      taskFunction job, callback

  sendResponse: (job, callback) =>
    response =
      metadata:   job.metadata
      responseId: job.metadata.responseId
      rawData:    job.rawData

    @jobManager.createResponse response, (error) =>
      return callback error if error?
      debug 'created response'
      callback()

module.exports = QueueWorker
