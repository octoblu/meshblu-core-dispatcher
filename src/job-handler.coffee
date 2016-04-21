debug = require('debug')('meshblu-core-dispatcher:job-handler')

class JobHandler
  constructor: (@jobType, @jobManager) ->

  handle: (request, callback) =>
    {metadata,rawData} = request

    options =
      metadata: metadata
      rawData: rawData

    @jobManager.do @jobType, @jobType, options, (error, response) =>
      return callback error if error?
      return callback new Error('Timed out waiting for response') unless response?
      debug @jobType, response.metadata.code
      callback null, response

module.exports = JobHandler
