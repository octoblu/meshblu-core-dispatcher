cson = require 'cson'
path = require 'path'
_    = require 'lodash'

class JobRegistry
  constructor: ({@jobs, @filters}={}) ->
    @jobs    ?= cson.parseFile path.join(__dirname, '../job-registry.cson')
    @filters ?= cson.parseFile path.join(__dirname, '../filter-registry.cson')

  toJSON: =>
    _.each @jobs, (job) =>
      tasks = job.tasks
      tasksWithFilters = _.pick tasks, (task) => task.filter?

      _.each tasksWithFilters, (task,taskName) =>
        filter = @filters[task.filter]

        _.each filter.tasks, (filterTask, filterTaskName) =>
          filterTask.on = _.defaults {}, filterTask.on, task.on
          job.tasks[filterTaskName] = filterTask

        newStartTask =
          task: 'meshblu-core-task-no-content'
          on:
            204: filter.start
        job.tasks[taskName] = newStartTask


    @jobs


module.exports = JobRegistry
