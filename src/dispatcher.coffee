_ = require 'lodash'
http = require 'http'
debug = require('debug')('meshblu-core-dispatcher:dispatcher')
async = require 'async'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@timeout,@elasticsearch} = options
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
    return callback() unless @elasticsearch?

    requestMetadata = _.cloneDeep request.metadata
    delete requestMetadata.auth?.token

    event =
      index: 'meshblu_job'
      type: 'dispatcher'
      body:
        elapsedTime: Date.now() - startTime
        request:
          metadata: requestMetadata
        response: _.pick(response, 'metadata')

    @elasticsearch.create event, callback

module.exports = Dispatcher
