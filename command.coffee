commander    = require 'commander'
packageJSON  = require './package.json'

class Command
  run: =>
    commander
      .version packageJSON.version
      .parse process.argv
      .command 'dispatch', 'run the dispatch worker'
      .command 'jobs',     'list compiled jobs from the registry'
      .command 'work',     'run the job worker'
      .parse process.argv

command = new Command()
command.run()
