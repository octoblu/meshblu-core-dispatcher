redis = require 'redis'
http = require 'http'

doAuthenticateJob = require './jobs/do-authenticate-job'

class Dispatcher
  @JOBS:
    'authenticate': doAuthenticateJob

  constructor: (options={}) ->
    {@namespace} = options
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
    Dispatcher.JOBS[type] request, callback

module.exports = Dispatcher
