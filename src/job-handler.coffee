class JobHandler
  constructor: (@jobType, @jobManager) ->

  handle: (request, callback) =>
    {metadata,rawData}   = request
    {responseId} = metadata

    options =
      responseId: responseId
      metadata: metadata
      rawData: rawData

    @jobManager.createRequest @jobType, options, (error) =>
      return callback error if error?
      @waitForResponse responseId, callback

  waitForResponse: (responseId, callback) =>
    @jobManager.getResponse @jobType, responseId, (error, response) =>
      return callback error if error?
      return callback new Error('Timed out waiting for response') unless response?
      callback null, response

module.exports = JobHandler
