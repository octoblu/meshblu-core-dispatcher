cson = require 'cson'
path = require 'path'
_    = require 'lodash'

class JobRegistry
  constructor: ({@jobs, @filters}={}) ->
    @jobs    ?= cson.parseFile path.join(__dirname, '../job-registry.cson')
    @filters ?= cson.parseFile path.join(__dirname, '../filter-registry.cson')

  toJSON: =>
    _.mapValues @jobs, @buildJob

  buildJob: (job) =>
    job = _.cloneDeep job

    tasksWithFilters = _.pick job.tasks, (task) => task.filter?

    _.each tasksWithFilters, (task) =>
      _.extend job.tasks, @tasksFromFilter(task)

    _.each tasksWithFilters, (task, taskName) =>
      job.tasks[taskName] = @newStartTask task

    job

  tasksFromFilter: (task) =>
    filter = _.cloneDeep @filters[task.filter]
    _.mapValues filter.tasks, (filterTask, filterTaskName) =>
      filterTask.on = _.defaults {}, filterTask.on, task.on
      filterTask

  newStartTask: (task) =>
    filterStart = @filters[task.filter].start

    return {
      task: "meshblu-core-task-no-content"
      on:
        204: filterStart
    }

module.exports = JobRegistry
