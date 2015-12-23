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
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?
      debug 'dispatch: got a job'

      startTime = Date.now()

      @doJob request, (error, response) =>
        return @sendError {startTime, request, error}, callback if error?
        @sendResponse {startTime, request, response}, callback

  sendResponse: ({startTime, request, response}, callback) =>
    {metadata,rawData} = response

    response =
      metadata: metadata
      rawData: rawData

    @log {startTime, request, response}, =>
      @jobManager.createResponse 'response', response, (error, something) =>
        callback error

  sendError: ({startTime, request, error}, callback) =>
    response =
      metadata:
        code: 504
        responseId: request.metadata.responseId
        status: error.message

    @log {startTime, request, response}, =>
      @jobManager.createResponse 'response', response, callback

  doJob: (request, callback) =>
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

  log: ({startTime, request, response}, callback) =>
    return callback() unless @logJobs
    requestMetadata = _.cloneDeep request.metadata
    delete requestMetadata.auth?.token

    job =
      index: 'meshblu_job'
      type: 'dispatcher'
      body:
        elapsedTime: Date.now() - startTime
        request:
          metadata: requestMetadata
        response: _.pick(response, 'metadata')

    @client.lpush 'job-log', JSON.stringify(job), (error, result) =>
      console.error 'Dispatcher.log', {error} if error?
      callback error

module.exports = Dispatcher
