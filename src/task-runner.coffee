debug      = require('debug')('meshblu-core-dispatcher:task-runner')

class TaskRunner
  constructor: (options={}, dependencies={}) ->
    {@config,@tasks,@data} = options
    @tasks ?= {}
    @tasks['meshblu-core-task-authenticate'] ?= require('meshblu-core-task-authenticate')
    @tasks['meshblu-core-task-get-subscriptions'] ?= require('meshblu-core-task-get-subscriptions')

  run: (callback) =>
    @_doTask @config.start, callback

  _doTask: (name, callback) =>
    taskConfig = @config.tasks[name]
    return callback new Error "Task Definition '#{name}' not found" unless taskConfig?
    taskName = taskConfig.task
    debug '_doTask', taskName
    taskFunc = @tasks[taskName]
    return callback new Error "Task Definition '#{name}' missing task" unless taskFunc?
    taskFunc @data, (error, response) =>
      return callback error if error?
      debug taskName, response
      {metadata,rawData} = response
      codeStr = metadata?.code?.toString()
      nextTask = taskConfig.on?[codeStr]
      return callback null, response unless nextTask?
      @_doTask nextTask, callback

module.exports = TaskRunner
