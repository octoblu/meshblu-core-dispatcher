async = require 'async'
_ = require 'lodash'
{EventEmitter2} = require 'eventemitter2'

class JobAssembler extends EventEmitter2
  constructor: (options={}) ->
    {@namespace,localClient,remoteClient,@timeout} = options
    @localClient  = _.bindAll localClient
    @remoteClient = _.bindAll remoteClient

    {@localHandlers,@remoteHandlers} = options
    @namespace ?= 'meshblu:internal'
    @timeout ?= 30

  assemble: =>
    authenticate: (request, callback) =>
      {metadata,rawData} = request
      {responseId}       = metadata

      client = @getClient 'authenticate'

      metadataStr = JSON.stringify metadata
      async.series [
        async.apply client.hset, "#{@namespace}:#{responseId}", "request:metadata", metadataStr
        async.apply client.hset, "#{@namespace}:#{responseId}", "request:data", rawData
        async.apply client.lpush, "#{@namespace}:authenticate:queue", "#{@namespace}:#{responseId}"
      ], (error) =>
        return callback error if error?
        @waitForResponse 'authenticate', responseId, callback

  getClient: (jobType) =>
    if jobType in @localHandlers
      @localClient
    else
      @remoteClient

  waitForResponse: (jobType, responseId, callback) =>
    client = @getClient jobType
    client.brpop "#{@namespace}:#{jobType}:#{responseId}", @timeout, (error, result) =>
      return callback error if error?
      return callback new Error('Timed out waiting for response') unless result?
      [channel,responseKey] = result
      @emit 'response', responseKey
      @getResponse jobType, responseKey, callback

  getResponse: (jobType, responseKey, callback) =>
    client = @getClient jobType
    async.parallel
      metadata: async.apply client.hget, responseKey, 'response:metadata'
      data:     async.apply client.hget, responseKey, 'response:data'
    , (error, result) =>
      response =
        metadata: JSON.parse result.metadata
        rawData: result.data

      return callback null, response

module.exports = JobAssembler
