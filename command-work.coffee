commander    = require 'commander'
async        = require 'async'
redis        = require 'redis'
debug        = require('debug')('meshblu-core-dispatcher:command')
packageJSON  = require './package.json'
CacheFactory     = require './src/cache-factory'
DatastoreFactory = require './src/datastore-factory'
JobAssembler     = require './src/job-assembler'
JobRegistry      = require './src/job-registry'
QueueWorker      = require './src/queue-worker'

class CommandWork
  parseList: (val) =>
    val.split ','

  parseOptions: =>
    commander
      .version packageJSON.version
      .option '-i, --internal-namespace <meshblu:internal>', 'job handler queue namespace.', 'meshblu:internal'
      .option '-j, --jobs <job1,job2>', 'jobs this worker is willing to do', @parseList
      .option '-s, --single-run', 'perform only one job.'
      .option '-t, --timeout <n>', 'seconds to wait for a next job.', parseInt, 30
      .parse process.argv

    {@internalNamespace,@singleRun,@timeout,@jobs} = commander
    @client = redis.createClient process.env.REDIS_URI

    @redisUri   = process.env.REDIS_URI
    @mongoDBUri = process.env.MONGODB_URI
    @pepper     = process.env.TOKEN

  run: =>
    @parseOptions()

    queueWorker = new QueueWorker
      pepper:    @pepper
      timeout:   @timeout
      namespace: @internalNamespace
      jobs:      @jobs
      client:       @client
      jobRegistry:  (new JobRegistry).toJSON()
      cacheFactory:     new CacheFactory client: redis.createClient(@redisUri)
      datastoreFactory: new DatastoreFactory database: @mongoDBUri

    if @singleRun
      queueWorker.run(@panic)
      return

    async.forever queueWorker.run, @panic

  panic: (error) =>
    console.error error.stack
    process.exit 1

commandWork = new CommandWork()
commandWork.run()
