redis = require 'redis'
http = require 'http'

class Dispatcher
  constructor: (options={}) ->
    {@namespace} = options
    {@jobHandlers} = options
    @namespace ?= 'meshblu'
    @redis = redis.createClient process.env.REDIS_URI

  dispatch: (callback) =>
    @redis.brpop "#{@namespace}:request:queue", 1, (error, result) =>
      return callback error if error?
      [channel,requestStr] = result
      request = JSON.parse requestStr

      @doJob request, (error, response) =>
        @sendResponse response, callback

  sendResponse: (response, callback) =>
    [metadata]     = response
    {responseUuid} = metadata

    responseStr = JSON.stringify(response)
    @redis.lpush "#{@namespace}:response:#{responseUuid}", responseStr, callback

  doJob: (request, callback) =>
    [metadata] = request
    type = metadata.jobType
    @jobHandlers[type] request, callback

module.exports = Dispatcher
