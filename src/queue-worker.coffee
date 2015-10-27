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
    debug 'timeout', @timeout, @namespace

    @jobManager = new JobManager
      timeoutSeconds: @timeout
      client: @client
      namespace: @namespace
      requestQueue: 'authenticate'
      responseQueue: 'authenticate'

  run: (callback=->) =>
    debug 'running'
    handleJob = (jobType, done) =>
      debug 'running for jobType', jobType
      @jobManager.getRequest (error, job) =>
        debug 'got job', error: error, job: job
        return callback error if error?
        return callback null unless job?
        @runJob job, done
    async.each @jobs, handleJob, callback

  runJob: (job={}, callback=->) =>
    return callback new Error("Missing metadata") unless job.metadata?
    {jobType,responseId} = job.metadata
    debug 'running job', job.metadata
    tasks = configJobs[jobType]
    asyncTasks = _.map tasks, (task) =>
      return @runTask task, job

    async.waterfall asyncTasks, (error, finishedJob) =>
      debug 'finished job'
      return callback error if error?
      response =
        metadata: finishedJob.metadata
        responseId: responseId
        rawData: finishedJob.rawData
      @jobManager.createResponse response, (error) =>
        return callback error if error?
        debug 'created response'
        callback()

  runTask: (task, originalJob) =>
    return (job, callback=->) =>
      if _.isFunction job
        callback = job
        job = originalJob
      taskFunction = @tasks[task]
      try taskFunction = require task unless taskFunction?
      return callback new Error("missing task") unless taskFunction?
      debug 'running task function'
      taskFunction job, callback

module.exports = QueueWorker
