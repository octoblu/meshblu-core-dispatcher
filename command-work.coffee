commander   = require 'commander'
async       = require 'async'
redis       = require 'redis'
mongojs     = require 'mongojs'
RedisNS     = require '@octoblu/redis-ns'
debug       = require('debug')('meshblu-core-dispatcher:command')
packageJSON      = require './package.json'
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
    @client = new RedisNS @internalNamespace, redis.createClient(process.env.REDIS_URI)

    @redisUri   = process.env.REDIS_URI
    @mongoDBUri = process.env.MONGODB_URI
    @pepper     = process.env.TOKEN

  run: =>
    @parseOptions()
    
    process.on 'SIGTERM', => @terminate = true

    cacheClient = new RedisNS @internalNamespace, redis.createClient(@redisUri)

    queueWorker = new QueueWorker
      pepper:    @pepper
      timeout:   @timeout
      jobs:      @jobs
      client:    @client
      jobRegistry:  (new JobRegistry).toJSON()
      cacheFactory:     new CacheFactory client: cacheClient
      datastoreFactory: new DatastoreFactory database: mongojs(@mongoDBUri)

    return queueWorker.run @tentativePanic if @singleRun

    async.until @terminated, queueWorker.run, @tentativePanic

  panic: (error) =>
    console.error error.stack
    process.exit 1

  tentativePanic: (error) =>
    return process.exit(0) unless error?
    console.error error.stack
    process.exit 1

  terminated: => @terminate

commandWork = new CommandWork()
commandWork.run()
