async = require 'async'
_ = require 'lodash'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'

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
    Authenticate: (request, callback) =>
      {metadata,rawData} = request
      {responseId}       = metadata

      jobManager = @getJobManager 'Authenticate'

      options =
        responseId: responseId
        metadata: metadata
        rawData: rawData

      jobManager.createRequest 'Authenticate', options, (error) =>
        return callback error if error?
        @waitForResponse 'Authenticate', responseId, callback

  getJobManager: (jobType) =>
    if jobType in @localHandlers
      @localJobManager
    else
      @remoteJobManager

  waitForResponse: (jobType, responseId, callback) =>
    jobManager = @getJobManager jobType
    jobManager.getResponse 'Authenticate', responseId, (error, response) =>
      return callback error if error?
      return callback new Error('Timed out waiting for response') unless response?
      @emit 'response', response
      callback null, response

module.exports = JobAssembler
