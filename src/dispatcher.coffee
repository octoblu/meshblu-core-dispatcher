redis = require 'redis'
http = require 'http'
{EventEmitter2} = require 'eventemitter2'

class Dispatcher extends EventEmitter2
  constructor: (options={}) ->
    {@namespace,@timeout} = options
    {@jobHandlers} = options
    @timeout ?= 1
    @namespace ?= 'meshblu'
    @redis = redis.createClient options.redisUri

  dispatch: (callback) =>
    @redis.brpop "#{@namespace}:request:queue", @timeout, (error, result) =>
      return callback error if error?
      return callback() unless result?

      [channel,requestStr] = result
      request = JSON.parse requestStr

      @emit 'job', request
      @doJob request, (error, response) =>
        @sendResponse response, callback

  sendResponse: (response, callback) =>
    [metadata]     = response
    {responseId}   = metadata

    responseStr = JSON.stringify(response)
    @redis.lpush "#{@namespace}:response:#{responseId}", responseStr, callback

  doJob: (request, callback) =>
    [metadata] = request
    type = metadata.jobType
    @jobHandlers[type] request, callback

module.exports = Dispatcher
