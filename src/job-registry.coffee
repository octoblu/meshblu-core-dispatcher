_ = require 'lodash'

class JobRegistry
  constructor: ({@jobs, @filters}) ->
  toJSON: =>
    _.each @jobs, (job) =>
      tasks = job.tasks
      tasksWithFilters = _.pick tasks, (task) => task.filter?

      _.each tasksWithFilters, (task,taskName) =>
        filter = @filters[task.filter]

        _.each filter.tasks, (filterTask, filterTaskName) =>
          filterTask.on = task.on
          job.tasks[filterTaskName] = filterTask

        newStartTask =
          task: 'meshblu-core-task-no-content'
          on:
            204: filter.start
        job.tasks[taskName] = newStartTask


    @jobs


module.exports = JobRegistry
