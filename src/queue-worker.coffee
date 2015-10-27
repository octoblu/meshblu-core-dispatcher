_          = require 'lodash'
async      = require 'async'
configJobs = require './config/jobs'
JobManager = require 'meshblu-core-job-manager'


class QueueWorker
  constructor: (options={}) ->
    {localClient,remoteClient,@namespace,@timeout,@jobs,@tasks} = options
    @localClient  = _.bindAll localClient
    @remoteClient = _.bindAll remoteClient

    {@localHandlers,@remoteHandlers} = options
    @timeout ?= 30
    @namespace ?= 'meshblu:internal'

  getJobManager: (jobType) =>
    if jobType in @localHandlers
      return new JobManager
        timeoutSeconds: @timeout
        client: @localClient
        namespace: @namespace
        requestQueue: jobType
        responseQueue: jobType
    else
      return new JobManager
        timeoutSeconds: @timeout
        client: @remoteClient
        namespace: @namespace
        requestQueue: jobType
        responseQueue: jobType

  run: =>
    _.each @jobs, (jobType) =>
      jobManager = @getJobManager jobType
      jobManager.getResponse "queue", (error, job) =>
        return console.error error if error?
        @runJob job

  runJob: (job, callback=->) =>
    {jobType,responseId} = job.metadata
    tasks = configJobs[jobType]
    asyncTasks = _.map tasks, (task) =>
      return @runTask task

    async.waterfall asyncTasks, (error, finishedJob) =>
      return console.error error if error?
      jobManager = @getJobManager jobType
      response =
        metadata: finishedJob.metadata
        responseId: responseId
        rawData: finishedJob.rawData
      jobManager.createResponse response, callback

  runTask: (task) =>
    return (job, callback=->) =>
      callback = job if _.isFunction job
      return callback new Error("missing task") unless @tasks[task]?
      @tasks[task] job, callback

module.exports = QueueWorker
