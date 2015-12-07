_ = require 'lodash'
http = require 'http'
debug = require('debug')('meshblu-core-dispatcher:dispatcher')
async = require 'async'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@timeout} = options
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 1

    @jobManager = new JobManager
      client: @client
      timeoutSeconds: @timeout

  dispatch: (callback) =>
    @jobManager.getRequest ['request'], (error, request) =>
      return callback error if error?
      return callback() unless request?

      @doJob request, (error, response) =>
        return @sendError request.metadata, error, callback if error?
        @sendResponse response, callback

  sendResponse: (response, callback) =>
    {metadata,rawData} = response

    options =
      metadata: metadata
      rawData: rawData

    @jobManager.createResponse 'response', options, (error, something) =>
      callback error

  sendError: (metadata, upstreamError, callback) =>
    errorMetadata =
      code: 504
      status: upstreamError.message

    options =
      metadata: errorMetadata

    @jobManager.createResponse 'response', options, callback

  doJob: (request, callback) =>
    debug 'doJob', request
    {metadata} = request

    type = metadata.jobType
    return @jobHandlers[type] request, callback if @jobHandlers[type]?

    callback new Error "jobType Not Found: #{type}"

module.exports = Dispatcher
