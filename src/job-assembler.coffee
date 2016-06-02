async           = require 'async'
_               = require 'lodash'
{EventEmitter2} = require 'eventemitter2'
JobHandler      = require './job-handler'

class JobAssembler extends EventEmitter2
  constructor: (options={}) ->
    {@localJobManager,@remoteJobManager} = options
    {@localHandlers,@remoteHandlers} = options

  assemble: =>
    jobs = {}

    _.each _.union(@localHandlers, @remoteHandlers), (jobType) =>
      jobHandler = new JobHandler jobType, @getJobManager(jobType)
      jobs[jobType] = jobHandler.handle

    jobs

  getJobManager: (jobType) =>
    if jobType in @localHandlers
      @localJobManager
    else
      @remoteJobManager

module.exports = JobAssembler
