_                      = require 'lodash'
debug                  = require('debug')('meshblu-core-dispatcher:task-runner')
moment                 = require 'moment'
SimpleBenchmark        = require 'simple-benchmark'
{Tasks}                = require './task-loader'

class TaskRunner
  @TASKS = Tasks

  constructor: (options={}) ->
    {
      @config
      @request
      @datastoreFactory
      @pepper
      @cacheFactory
      @uuidAliasResolver
      @workerName
      @privateKey
      @publicKey
      @taskLogger
      @taskJobManager
      @firehoseClient
    } = options
    @todaySuffix = moment.utc().format('YYYY-MM-DD')

  run: (callback) =>
    @_doTask @config.start, callback

  _doTask: (name, callback) =>
    benchmark = new SimpleBenchmark label: 'task-runner'
    taskConfig = @config.tasks[name]
    return callback new Error "Task Definition '#{name}' not found" unless taskConfig?

    taskName = taskConfig.task
    Task = TaskRunner.TASKS[taskName]
    return callback new Error "Task Definition '#{name}' missing task class" unless Task?

    datastore = @datastoreFactory.build taskConfig.datastoreCollection if taskConfig.datastoreCollection?
    cache  = @cacheFactory.build taskConfig.cacheNamespace if taskConfig.cacheNamespace?

    task = new Task {
      @uuidAliasResolver
      datastore
      cache
      @pepper
      @privateKey
      @publicKey
      @firehoseClient
      jobManager: @taskJobManager
    }

    task.do @request, (error, response) =>
      return callback error if error?
      debug taskName, response.metadata.code
      {metadata} = response

      codeStr = metadata?.code?.toString()
      nextTask = taskConfig.on?[codeStr]
      @logTask {benchmark, @request, response, taskName}, =>
        return callback null, response unless nextTask?
        @_doTask nextTask, callback

  logTask: ({benchmark, request, response, taskName}, callback) =>
    request.metadata = _.cloneDeep request.metadata
    request.metadata.workerName = @workerName
    request.metadata.taskName = taskName
    response.metadata.jobLogs = request.metadata.jobLogs if request.metadata.jobLogs?
    response.metadata.metrics = request.metadata.metrics if request.metadata.metrics?
    @taskLogger.log {request, response, elapsedTime: benchmark.elapsed()}, callback

module.exports = TaskRunner
