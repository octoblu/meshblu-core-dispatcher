class JobAssembler
  constructor: (options={}) ->
    {@namespace,@localClient,@remoteClient,@timeout} = options
    {@localHandlers,@remoteHandlers} = options
    @namespace ?= 'meshblu'
    @timeout ?= 30

  assemble: =>
    authenticate: (request, callback) =>
      requestStr = JSON.stringify(request)

      client = @getClient 'authenticate'
      client.lpush "#{@namespace}:authenticate:queue", requestStr, (error) =>
        return callback error if error?

        [metadata] = request
        @waitForResponse 'authenticate', metadata.responseId, callback

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
      [channel,responseStr] = result
      response = JSON.parse responseStr
      return callback null, response


module.exports = JobAssembler
