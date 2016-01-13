_ = require 'lodash'
http = require 'http'
debug = require('debug')('meshblu-core-dispatcher:dispatcher')
async = require 'async'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@timeout,@logJobs} = options
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 30

    @jobManager = new JobManager
      client: @client
      timeoutSeconds: @timeout

  dispatch: (callback) =>
    startDispatchTime = Date.now()
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?
      debug 'dispatch: got a job'

      @logRequest {startTime: startDispatchTime, request, index: 'meshblu_dispatcher'}, =>
        startTime = Date.now()

        @doJob request, (error, response) =>
          return @sendError {startTime, request, error}, callback if error?
          @sendResponse {startTime, request, response}, callback

  sendResponse: ({startTime, request, response}, callback) =>
    {metadata,rawData} = response

    response =
      metadata: metadata
      rawData: rawData

    @logRequest {startTime, request, response}, =>
      @jobManager.createResponse 'response', response, (error, something) =>
        callback error

  sendError: ({startTime, request, error}, callback) =>
    response =
      metadata:
        code: 504
        responseId: request.metadata.responseId
        status: error.message

    @logRequest {startTime, request, response}, =>
      @jobManager.createResponse 'response', response, callback

  doJob: (request, callback) =>
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

  logRequest: ({startTime, index='meshblu_job', request, response}, callback) =>
    requestMetadata = _.cloneDeep request.metadata
    delete requestMetadata.auth?.token

    body =
      date: startTime
      elapsedTime: Date.now() - startTime
      request:
        metadata: requestMetadata

    body.response = _.pick response, 'metadata' if response?
    @_log {index, type: 'dispatcher', body}, callback

  _log: ({index, type, body}, callback) =>
    return callback() unless @logJobs
    job = {index, type, body}

    @client.lpush 'job-log', JSON.stringify(job), (error, result) =>
      console.error 'Dispatcher.log', {error} if error?
      callback error

module.exports = Dispatcher
