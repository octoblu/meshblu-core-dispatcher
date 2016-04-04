commander    = require 'commander'
debug        = require('debug')('meshblu-core-dispatcher:command')
packageJSON  = require './package.json'
JobRegistry  = require './src/job-registry'

class CommandJobs
  parseOptions: =>
    commander
      .version packageJSON.version
      .parse process.argv

  run: =>
    @parseOptions()
    jobs = (new JobRegistry).toJSON()
    console.log JSON.stringify(jobs, null, 2)

  panic: (error) =>
    console.error error.stack
    process.exit 1

commandWork = new CommandJobs()
commandWork.run()
