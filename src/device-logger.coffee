JobLogger = require 'job-logger'
debug     = require('debug')('meshblu-core-dispatcher:device-logger')

class DeviceLogger
  constructor: ({client, indexPrefix, type, jobLogQueue, sampleRate, @filterUuid}) ->
    return unless @filterUuid?
    debug('new DeviceLogger', {indexPrefix, type, jobLogQueue, sampleRate})
    @jobLogger = new JobLogger {client, indexPrefix, type, jobLogQueue, sampleRate}

  log: ({request, response, elapsedTime}, callback) =>
    return callback() unless @shouldLog(request)
    debug('logging request')
    @jobLogger.log {request, response, elapsedTime}, callback

  shouldLog: (request) =>
    return false unless @filterUuid?
    return true if request?.metadata?.auth?.uuid == @filterUuid
    return true if request?.metadata?.fromUuid == @filterUuid
    return true if request?.metadata?.toUuid == @filterUuid
    return false

module.exports = DeviceLogger
