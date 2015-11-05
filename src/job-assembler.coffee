async = require 'async'
_ = require 'lodash'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'
JobHandler = require './job-handler'

class JobAssembler extends EventEmitter2
  constructor: (options={}) ->
    {localClient,remoteClient,@timeout} = options
    @localClient  = _.bindAll localClient
    @remoteClient = _.bindAll remoteClient

    {@localHandlers,@remoteHandlers} = options
    @timeout ?= 30

    @localJobManager = new JobManager
      timeoutSeconds: @timeout
      client: @localClient

    @remoteJobManager = new JobManager
      timeoutSeconds: @timeout
      client: @remoteClient

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
