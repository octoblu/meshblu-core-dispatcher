_               = require 'lodash'
http            = require 'http'
debug           = require('debug')('meshblu-core-dispatcher:dispatcher')
async           = require 'async'
moment          = require 'moment'
{EventEmitter2} = require 'eventemitter2'
JobManager      = require 'meshblu-core-job-manager'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@timeout,@logJobs,@workerName,@jobLogger,@dispatchLogger} = options
    @startDispatchTime = Date.now()
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 30

    throw new Error('Missing @jobLogger') unless @jobLogger?
    throw new Error('Missing @dispatchLogger') unless @dispatchLogger?

    @todaySuffix = moment.utc().format('YYYY-MM-DD')

    @jobManager = new JobManager
      client: @client
      timeoutSeconds: @timeout

  dispatch: (callback) =>
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?
      debug 'dispatch: got a job'

      @logDispatcher {startTime: @startDispatchTime, request}, =>
        startTime = Date.now()

        @doJob request, (error, response) =>
          return @sendError {startTime, request, error}, callback if error?
          @sendResponse {startTime, request, response}, callback

  sendResponse: ({startTime, request, response}, callback) =>
    {metadata,rawData} = response

    response =
      metadata: metadata
      rawData: rawData

    @jobManager.createResponse 'response', response, (error) =>
      @logJob {startTime, request, response}, =>
        callback error

  sendError: ({startTime, request, error}, callback) =>
    response =
      metadata:
        code: 504
        responseId: request.metadata.responseId
        status: error.message

    @logJob {startTime, request, response}, =>
      @jobManager.createResponse 'response', response, callback

  doJob: (request, callback) =>
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

  logDispatcher: ({startTime, request, response}, callback) =>
    elapsedTime = Date.now() - startTime
    @dispatchLogger.log {request, response, elapsedTime}, callback

  logJob: ({startTime, request, response}, callback) =>
    elapsedTime = Date.now() - startTime
    @jobLogger.log {request, response, elapsedTime}, callback

module.exports = Dispatcher
