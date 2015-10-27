_ = require 'lodash'
async = require 'async'
configJobs = require './config/jobs'

class QueueWorker
  constructor: (options={}) ->
    {localClient,remoteClient,@namespace,@timeout,@jobs,@tasks} = options
    @localClient  = _.bindAll localClient
    @remoteClient = _.bindAll remoteClient
    {@localHandlers,@remoteHandlers} = options
    @timeout ?= 1
    @namespace ?= 'meshblu'

  getClient: (jobType) =>
    return @localClient if jobType in @localHandlers
    return @remoteClient

  run: =>
    _.each @jobs, (jobType) =>
      client = @getClient jobType
      client.brpop "#{@namespace}:#{jobType}:queue", @timeout, (error, result) =>
        return console.error error if error?
        [channel, responseKey] = result
        @getResponse jobType, responseKey, (error, job) =>
          return console.error error if error?
          @runJob job

  runJob: (job) =>
    {metadata} = job
    client = @getClient metadata.jobType
    tasks = configJobs[metadata.jobType]
    async.each tasks, @runTask, (error) =>
      console.log 'done with tasks'

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

module.exports = QueueWorker
