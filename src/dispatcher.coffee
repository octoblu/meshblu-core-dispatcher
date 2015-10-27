_ = require 'lodash'
http = require 'http'
async = require 'async'
{EventEmitter2} = require 'eventemitter2'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {client,@namespace,@timeout} = options
    @client = _.bindAll client
    {@jobHandlers} = options
    @timeout ?= 1
    @namespace ?= 'meshblu'

  dispatch: (callback) =>
    @client.brpop "#{@namespace}:request:queue", @timeout, (error, result) =>
      return callback error if error?
      return callback() unless result?
      [channel,responseKey] = result

      @doJob responseKey, (error, response) =>

        @sendResponse response, callback

  sendResponse: (response, callback) =>
    {metadata,rawData} = response
    {responseId}   = metadata

    metadataStr = JSON.stringify metadata

    async.series [
      async.apply @client.hset, "#{@namespace}:#{responseId}", "response:metadata", metadataStr
      async.apply @client.hset, "#{@namespace}:#{responseId}", "response:data", rawData
      async.apply @client.lpush, "#{@namespace}:response:#{responseId}", "#{@namespace}:#{responseId}"
    ], callback

  doJob: (responseKey, callback) =>
    async.parallel
      metadata: async.apply @client.hget, responseKey, 'request:metadata'
      data: async.apply @client.hget, responseKey, 'request:data'
    , (error, result) =>
      return callback error if error?

      metadata = JSON.parse result.metadata

      request = {
        metadata: metadata
        rawData: result.data ? ""
      }

      @emit 'job', request

      type = metadata.jobType
      @jobHandlers[type] request, callback

module.exports = Dispatcher
