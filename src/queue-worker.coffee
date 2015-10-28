_          = require 'lodash'
async      = require 'async'
cson       = require 'cson'
JobManager = require 'meshblu-core-job-manager'
debug      = require('debug')('meshblu-core-dispatcher:queue-worker')
path       = require 'path'
jobRegistry = cson.parseFile path.join(__dirname, '../job-registry.cson')
TaskRunner = require './task-runner'

# lowercase all job names
jobRegistry = _.mapKeys jobRegistry, (value, key) =>
  key.toLocaleLowerCase()

class QueueWorker
  constructor: (options={}) ->
    {client,@namespace,@timeout,@tasks,@jobs,@jobRegistry} = options
    @client = _.bindAll client
    @timeout ?= 30
    @namespace ?= 'meshblu:internal'
    @jobRegistry = jobRegistry

    @jobManager = new JobManager
      timeoutSeconds: @timeout
      client: @client
      namespace: @namespace
      requestQueue: 'authenticate'
      responseQueue: 'authenticate'

  run: (callback=->) =>
    debug 'running...'
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

    jobTypeLower = jobType.toLocaleLowerCase()
    jobDef = jobRegistry[jobTypeLower]
    return callback new Error "jobType '#{jobType}' not found" unless jobDef?

    taskRunner = new TaskRunner
      config: jobDef
      tasks: @tasks
      data: job
      
    taskRunner.run (error, finishedJob) =>
      return callback error if error?
      @sendResponse finishedJob, callback

  sendResponse: (job, callback) =>
    {metadata,rawData} = job
    {responseId} = metadata
    response =
      metadata:   metadata
      responseId: responseId
      rawData:    rawData

    @jobManager.createResponse response, (error) =>
      return callback error if error?
      debug 'created response'
      callback()

module.exports = QueueWorker
