_ = require 'lodash'
http = require 'http'
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
    {responseId}   = metadata

    options =
      responseId: responseId
      metadata: metadata
      rawData: rawData

    @jobManager.createResponse 'response', options, callback

  sendError: (metadata, upstreamError, callback) =>
    {responseId} = metadata

    errorMetadata =
      responseId: responseId
      code: 504
      status: upstreamError.message

    options =
      responseId: responseId
      metadata: errorMetadata

    @jobManager.createResponse 'response', options, callback

  doJob: (request, callback) =>
    {metadata} = request
    @emit 'job', request

    type = metadata.jobType
    @jobHandlers[type] request, callback

module.exports = Dispatcher
