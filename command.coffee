commander    = require 'commander'
packageJSON  = require './package.json'

class Command
  run: =>
    commander
      .version packageJSON.version
      .parse process.argv
      .command 'dispatch', 'run the dispatch worker'
      .command 'work',     'run the job worker'
      .parse process.argv

command = new Command()
command.run()
