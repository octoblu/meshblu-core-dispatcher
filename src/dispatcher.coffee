redis = require 'redis'
http = require 'http'

class Dispatcher
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

      @doJob request, (error, response) =>
        @sendResponse response, callback

  sendResponse: (response, callback) =>
    [metadata]     = response
    {responseId} = metadata

    responseStr = JSON.stringify(response)
    @redis.lpush "#{@namespace}:response:#{responseId}", responseStr, callback

  doJob: (request, callback) =>
    [metadata] = request
    type = metadata.jobType
    @jobHandlers[type] request, callback

module.exports = Dispatcher
