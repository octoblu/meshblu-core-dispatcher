commander = require 'commander'
async = require 'async'
packageJSON = require './package.json'
Dispatcher = require './src/dispatcher'

class Command
  parseOptions: =>
    commander
      .version packageJSON.version
      .option '-n, --namespace <meshblu>', 'request/response queue namespace.', 'meshblu'
      .option '-s, --single-run', 'perform only one job.'
      .option '-t, --timeout <n>', 'seconds to wait for a next job.', parseInt, 30
      .parse process.argv

    {@namespace,@singleRun,@timeout} = commander

  run: =>
    @parseOptions()

    dispatcher = new Dispatcher namespace: @namespace, timeout: @timeout

    return dispatcher.work(@panic) if @singleRun
    async.forever dispatcher.dispatch, @panic

  panic: (error) =>
    console.error error.stack
    process.exit 1

command = new Command()
command.run()
