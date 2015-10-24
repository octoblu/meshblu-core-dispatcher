redis = require 'redis'
http = require 'http'

doAuthenticateJob = require '../jobs/do-authenticate-job'

class Dispatcher
  @JOBS:
    'authenticate': doAuthenticateJob

  constructor: () ->
    @redis = redis.createClient process.env.REDIS_URI

  dispatch: (callback) =>
    @redis.brpop 'test:request:queue', 1, (error, result) =>
      return callback error if error?
      [queueName,request] = result

      @doJob request.jobType, request.options, (metadata, data) =>
        @sendResponse request.responseUuid, metadata, data, callback

  sendResponse: (responseUuid, metadata, data) =>
    @redis.lpush "test:response:#{responseUuid}", JSON.stringify [metadata, data]

  doJob: (type, options, callback) =>
    return callback code: 501, status: http.STATUS_CODES[501]
    Dispatcher.JOBS[type] options, callback

module.exports = Worker
