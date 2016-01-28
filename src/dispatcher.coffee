_ = require 'lodash'
http = require 'http'
debug = require('debug')('meshblu-core-dispatcher:dispatcher')
async = require 'async'
moment = require 'moment'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@timeout,@logJobs,@indexName,@workerName} = options
    @startDispatchTime = Date.now()
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 30

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

  logDispatcher: (options, callback) =>
    options.type = 'dispatcher'
    @_log options, callback

  logJob: (options, callback) =>
    options.type = 'job'
    @_log options, callback

  _log: ({startTime, request, response, type}, callback) =>
    return callback() unless @logJobs
    requestMetadata = _.cloneDeep request.metadata
    delete requestMetadata.auth?.token
    requestMetadata.workerName = @workerName
    responseMetadata = _.cloneDeep(response?.metadata ? {})
    responseMetadata.success = (responseMetadata.code < 500)

    job =
      index: "#{@indexName}-#{@todaySuffix}"
      type: type
      body:
        elapsedTime: Date.now() - startTime
        date: Date.now()
        request:
          metadata: requestMetadata
        response:
          metadata: responseMetadata

    @client.lpush 'job-log', JSON.stringify(job), (error, result) =>
      console.error 'Dispatcher.log', {error} if error?
      callback error

module.exports = Dispatcher
