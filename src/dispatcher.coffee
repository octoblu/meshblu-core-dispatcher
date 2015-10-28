_ = require 'lodash'
http = require 'http'
async = require 'async'
{EventEmitter2} = require 'eventemitter2'
JobManager = require 'meshblu-core-job-manager'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@namespace,@timeout} = options
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 1
    @namespace ?= 'meshblu'

    @jobManager = new JobManager
      client: @client
      timeoutSeconds: @timeout
      namespace: @namespace
      requestQueue: 'request'
      responseQueue: 'response'

  dispatch: (callback) =>
    @jobManager.getRequest (error, request) =>
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

    @jobManager.createResponse options, callback

  sendError: (metadata, upstreamError, callback) =>
    {responseId} = metadata

    errorMetadata =
      responseId: responseId
      code: 504
      status: upstreamError.message

    options =
      responseId: responseId
      metadata: errorMetadata

    @jobManager.createResponse options, callback

  doJob: (request, callback) =>
    {metadata} = request
    @emit 'job', request

    type = metadata.jobType
    @jobHandlers[type] request, callback

module.exports = Dispatcher
