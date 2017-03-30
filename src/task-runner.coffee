_                      = require 'lodash'
debug                  = require('debug')('meshblu-core-dispatcher:task-runner')
debugBenchmark         = require('debug')('meshblu-core-dispatcher:task-runner:benchmark')
moment                 = require 'moment'
SimpleBenchmark        = require 'simple-benchmark'
{Tasks}                = require './task-loader'
RedisNS                = require '@octoblu/redis-ns'

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
    unless taskConfig?
      error = new Error "Task Definition '#{name}' not found"
      error.code = 501
      return callback error

    taskName = taskConfig.task
    Task = TaskRunner.TASKS[taskName]
    unless Task?
      error = new Error "Task Definition '#{name}' missing task class"
      error.code = 501
      return callback error

    datastore = @datastoreFactory.build taskConfig.datastoreCollection if taskConfig.datastoreCollection?
    if taskConfig.cacheNamespace?
      cache  = @cacheFactory.build taskConfig.cacheNamespace
      firehoseClient = new RedisNS taskConfig.cacheNamespace, @firehoseClient

    if taskConfig.redisNamspace?
      redisClient = @cacheFactory.build taskConfig.redisNamspace

    task = new Task {
      @uuidAliasResolver
      datastore
      cache
      redisClient
      @pepper
      @privateKey
      @publicKey
      firehoseClient
      jobManager: @taskJobManager
    }

    task.do @request, (error, response) =>
      if error?
        code = parseInt(error.code)
        delete error.code unless _.inRange code, 99, 600
      return callback error if error?
      {metadata} = response
      debug taskName, metadata?.code

      codeStr = metadata?.code?.toString()
      nextTask = taskConfig.on?[codeStr]
      @logTask {benchmark, @request, response, taskName}, =>
        return callback null, response unless nextTask?
        debugBenchmark("#{taskName}[#{codeStr}] #{benchmark.elapsed()}ms")
        @_doTask nextTask, callback

  logTask: ({benchmark, request, response, taskName}, callback) =>
    request.metadata = _.cloneDeep request.metadata
    request.metadata.workerName = @workerName
    request.metadata.taskName = taskName
    response.metadata.jobLogs = request.metadata.jobLogs if request.metadata.jobLogs?
    response.metadata.metrics = request.metadata.metrics if request.metadata.metrics?
    @taskLogger.log {request, response, elapsedTime: benchmark.elapsed()}, callback

module.exports = TaskRunner
