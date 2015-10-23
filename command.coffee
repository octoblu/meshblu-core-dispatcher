commander = require 'commander'
async = require 'async'
packageJSON = require './package.json'
Worker = require './src/worker'

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

    worker = new Worker namespace: @namespace, timeout: @timeout

    return worker.work(@panic) if @singleRun
    async.forever worker.work, @panic

  panic: (error) =>
    console.error error.stack
    process.exit 1

command = new Command()
command.run()
