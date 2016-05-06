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
      @createRespondLogger
      @createPopLogger
    } = options
    @dispatchBenchmark = new Benchmark label: 'Dispatcher'
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 30

    throw new Error('Missing @jobLogger') unless @jobLogger?
    throw new Error('Missing @memoryLogger') unless @memoryLogger?
    throw new Error('Missing @dispatchLogger') unless @dispatchLogger?
    throw new Error('Missing @createPopLogger') unless @createPopLogger?
    throw new Error('Missing @createRespondLogger') unless @createRespondLogger?

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

      async.parallel [
        async.apply @createPopLogger.log, {request, elapsedTime: requestBenchmark.elapsed()}
        async.apply @dispatchLogger.log, {request, elapsedTime: @dispatchBenchmark.elapsed()}
      ], =>
        benchmark = new Benchmark label: 'do-job'

        @doJob request, (error, response) =>
          return @sendError {requestBenchmark, benchmark, request, error}, callback if error?
          @sendResponse {requestBenchmark, benchmark, request, response}, callback

  sendResponse: ({requestBenchmark, benchmark, request, response}, callback) =>
    {metadata,rawData} = response

    response =
      metadata: metadata
      rawData: rawData

    @jobManager.createResponse 'response', response, (error) =>
      async.parallel [
        async.apply @createRespondLogger.log, {request, response, elapsedTime: requestBenchmark.elapsed()}
        async.apply @jobLogger.log, {request, response, elapsedTime: benchmark.elapsed()}
        async.apply @memoryLogger.log, {request, response, elapsedTime: process.memoryUsage().rss, date: Date.now()}
      ], =>
        callback error

  sendError: ({requestBenchmark, benchmark, request, error}, callback) =>
    response =
      metadata:
        code: 504
        responseId: request.metadata.responseId
        status: error.message

    async.parallel [
      async.apply @createRespondLogger.log, {request, response, elapsedTime: requestBenchmark.elapsed()}
      async.apply @jobLogger.log, {request, response, elapsedTime: benchmark.elapsed()}
    ], =>
      @jobManager.createResponse 'response', response, callback

  doJob: (request, callback) =>
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

module.exports = Dispatcher
