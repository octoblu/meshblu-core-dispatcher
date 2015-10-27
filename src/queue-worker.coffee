_          = require 'lodash'
async      = require 'async'
configJobs = require './config/jobs'
JobManager = require 'meshblu-core-job-manager'
debug      = require('debug')('meshblu-core-dispatcher:queue-worker')

class QueueWorker
  constructor: (options={}) ->
    {localClient,remoteClient,@namespace,@timeout,@tasks} = options
    @localClient  = _.bindAll localClient
    @remoteClient = _.bindAll remoteClient

    {@localHandlers,@remoteHandlers} = options
    @timeout ?= 30
    @namespace ?= 'meshblu:internal'
    debug 'timeout', @timeout, @namespace

  getJobManager: (jobType) =>
    if jobType in @localHandlers
      debug 'using local job queue'
      return new JobManager
        timeoutSeconds: @timeout
        client: @localClient
        namespace: @namespace
        requestQueue: jobType
        responseQueue: jobType
    else
      debug 'using remote job queue'
      return new JobManager
        timeoutSeconds: @timeout
        client: @remoteClient
        namespace: @namespace
        requestQueue: jobType
        responseQueue: jobType

  run: (callback=->) =>
    debug 'running'
    handledJobs = _.union @localHandlers, @remoteHandlers
    handleJob = (jobType, done) =>
      debug 'running for jobType', jobType
      jobManager = @getJobManager jobType
      jobManager.getRequest (error, job) =>
        debug 'got job', error: error, job: job
        return callback error if error?
        return callback null unless job?
        @runJob job, done
    async.each handledJobs, handleJob, callback

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
      jobManager = @getJobManager jobType
      response =
        metadata: finishedJob.metadata
        responseId: responseId
        rawData: finishedJob.rawData
      jobManager.createResponse response, (error) =>
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
