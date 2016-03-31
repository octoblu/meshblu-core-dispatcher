_     = require 'lodash'
async = require 'async'

class ArrayLogger
  constructor: ({loggers}) ->
    @logFunctions = _.pluck loggers, 'log'

  log: (options, callback) =>
    async.applyEachSeries @logFunctions, options, callback

module.exports = ArrayLogger
