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

    # needs better recursion
    @mapFilters job.tasks

    job

  mapFilters: (tasks) =>
    _.each tasks, (task) =>
      return unless task.filter?
      _.extend tasks, @tasksFromFilter task

    _.each tasks, (task, taskName) =>
      return unless task.filter?
      tasks[taskName] = @newStartTask task

  tasksFromFilter: (task) =>
    filter = _.cloneDeep @filters[task.filter]
    @mapFilters filter.tasks
    _.mapValues filter.tasks, (filterTask) =>
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
