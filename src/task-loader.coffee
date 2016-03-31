_ = require 'lodash'
packageJSON = require('../package.json')

loadTasks = =>
  taskNames = _.filter _.keys(packageJSON.dependencies), (name) =>
    _.startsWith name, 'meshblu-core-task'
  taskRequires = _.map taskNames, require
  _.zipObject taskNames, taskRequires

module.exports =
  Tasks: loadTasks()
