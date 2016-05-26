_               = require 'lodash'
async           = require 'async'
moment          = require 'moment'
{EventEmitter2} = require 'eventemitter2'
JobManager      = require 'meshblu-core-job-manager'
Benchmark       = require 'simple-benchmark'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {
      client
      @timeout
      @logJobs
      @workerName
      @jobLogger
      @memoryLogger
      @dispatchLogger
    } = options
    @dispatchBenchmark = new Benchmark label: 'Dispatcher'
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 30

    throw new Error('Missing @jobLogger') unless @jobLogger?
    throw new Error('Missing @memoryLogger') unless @memoryLogger?
    throw new Error('Missing @dispatchLogger') unless @dispatchLogger?

    @todaySuffix = moment.utc().format('YYYY-MM-DD')

    @jobManager = new JobManager
      client: @client
      timeoutSeconds: @timeout

  dispatch: (callback) =>
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?
      requestBenchmark = new Benchmark label: 'Job'
      requestBenchmark.startTime = request.createdAt if request.createdAt?

      @dispatchLogger.log {request, elapsedTime: @dispatchBenchmark.elapsed()}, =>
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

    @jobManager.createResponse 'response', response, (error) =>
      async.parallel [
        async.apply @jobLogger.log, {request, response: logResponse}
        async.apply @memoryLogger.log, {request, response: logResponse, elapsedTime: process.memoryUsage().rss, date: Date.now()}
      ], =>
        callback error

  sendError: ({requestBenchmark, request, error}, callback) =>
    response =
      metadata:
        code: 504
        responseId: request.metadata.responseId
        status: error.message

    response.metadata.metrics = request.metadata.metrics

    @jobManager.createResponse 'response', response, (createResponseError) =>
      @jobLogger.log {request, response}, =>
        callback createResponseError

  doJob: (request, callback) =>
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

module.exports = Dispatcher
