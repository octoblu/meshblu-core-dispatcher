_               = require 'lodash'
async           = require 'async'
moment          = require 'moment'
{EventEmitter2} = require 'eventemitter2'
Benchmark       = require 'simple-benchmark'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {
      @workerName
      @jobLogger
      @dispatchLogger
      @jobManager
      @jobHandlers
    } = options
    @dispatchBenchmark = new Benchmark label: 'Dispatcher'

    throw new Error('Missing @jobLogger') unless @jobLogger?
    throw new Error('Missing @dispatchLogger') unless @dispatchLogger?

    @todaySuffix = moment.utc().format('YYYY-MM-DD')

  dispatch: (callback) =>
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?
      requestBenchmark = new Benchmark label: 'Job'
      requestBenchmark.startTime = request.createdAt if request.createdAt?

      response = metadata:
        code: 200
        jobLogs: request.metadata?.jobLogs

      @dispatchLogger.log {request, response, elapsedTime: @dispatchBenchmark.elapsed()}, =>
        @doJob request, (error, response) =>
          return @sendError {requestBenchmark, request, error}, callback if error?
          @sendResponse {requestBenchmark, request, response}, callback

  sendResponse: ({requestBenchmark, request, response}, callback) =>
    {metadata,rawData} = response

    response =
      metadata: metadata
      rawData: rawData

    logResponse = _.clone response
    logResponse.metadata = _.cloneDeep metadata
    response.metadata.metrics = request.metadata.metrics
    response.metadata.jobLogs = request.metadata.jobLogs
    logResponse.metadata.jobLogs = request.metadata.jobLogs

    @jobManager.createResponse 'response', response, (error) =>
      @jobLogger.log {request, response: logResponse}, callback

  sendError: ({requestBenchmark, request, error}, callback) =>
    response =
      metadata:
        code: 504
        responseId: request.metadata.responseId
        status: error.message

    response.metadata.metrics = request.metadata.metrics
    response.metadata.jobLogs = request.metadata.jobLogs

    @jobManager.createResponse 'response', response, (createResponseError) =>
      @jobLogger.log {request, response}, =>
        callback createResponseError

  doJob: (request, callback) =>
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

module.exports = Dispatcher
