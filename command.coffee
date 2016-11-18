dashdash         = require 'dashdash'
DispatcherWorker = require './src/dispatcher-worker'
packageJSON      = require './package.json'
SigtermHandler   = require 'sigterm-handler'

options = [
  {
    name: 'version'
    type: 'bool'
    help: 'Print tool version and exit.'
  }
  {
    names: ['help', 'h']
    type: 'bool'
    help: 'Print this help and exit.'
  }
  {
    names: ['namespace', 'n']
    type: 'string'
    help: 'request/response queue namespace'
    default: 'meshblu'
    env: 'NAMESPACE'
  }
  {
    names: ['single-run', 's']
    type: 'bool'
    help: 'perform only one job, then exit'
  }
  {
    names: ['timeout', 't']
    type: 'positiveInteger'
    help: 'seconds to wait for the next job'
    default: 15
  }
  {
    name: 'redis-uri'
    type: 'string'
    help: 'URI for Redis'
    env: 'REDIS_URI'
  }
  {
    name: 'cache-redis-uri'
    type: 'string'
    help: 'URI for Datastore/Cache Factory'
    env:  'CACHE_REDIS_URI'
  }
  {
    name: 'firehose-redis-uri'
    type: 'string'
    help: 'URI for Firehose redis'
    env: 'FIREHOSE_REDIS_URI'
  }
  {
    name: 'mongodb-uri'
    type: 'string'
    help: 'URI for MongoDB'
    env: 'MONGODB_URI'
  }
  {
    name: 'pepper'
    type: 'string'
    help: 'Pepper for encryption'
    env: 'TOKEN'
  }
  {
    name: 'alias-server-uri'
    type: 'string'
    help: 'URI for alias server'
    env: 'ALIAS_SERVER_URI'
  }
  {
    name: 'worker-name'
    type: 'string'
    help: 'name of this worker'
    env: 'WORKER_NAME'
  }
  {
    name: 'job-log-redis-uri'
    type: 'string'
    help: 'URI for job log Redis'
    env: 'JOB_LOG_REDIS_URI'
  }
  {
    name: 'job-log-queue'
    type: 'string'
    help: 'Job log queue name'
    env: 'JOB_LOG_QUEUE'
  }
  {
    name: 'job-log-sample-rate'
    type: 'number'
    help: 'Job log sample rate (0.00 to 1.00)'
    env: 'JOB_LOG_SAMPLE_RATE'
  }
  {
    name: 'private-key-base64'
    type: 'string'
    help: 'Base64-encoded private key'
    env: 'PRIVATE_KEY_BASE64'
  }
  {
    name: 'public-key-base64'
    type: 'string'
    help: 'Base64-encoded public key'
    env: 'PUBLIC_KEY_BASE64'
  }
  {
    name: 'request-queue-name'
    type: 'string'
    help: 'request queue name'
    env: 'REQUEST_QUEUE_NAME'
  }
]

parser = dashdash.createParser(options: options)
try
  opts = parser.parse(process.argv)
catch e
  console.error 'meshblu-core-dispatcher: error: %s', e.message
  process.exit 1

if opts.version
  console.log "meshblu-core-dispatcher v#{packageJSON.version}"
  process.exit 0

if opts.help
  help = parser.help({includeEnv: true, includeDefaults: true}).trimRight()
  console.log 'usage: node command.js [OPTIONS]\n' + 'options:\n' + help
  process.exit 0

if opts.private_key_base64?
  privateKey = new Buffer(opts.private_key_base64, 'base64').toString('utf8')

if opts.public_key_base64?
  publicKey = new Buffer(opts.public_key_base64, 'base64').toString('utf8')

options = {
  namespace:           opts.namespace
  timeoutSeconds:      opts.timeout
  redisUri:            opts.redis_uri
  cacheRedisUri:       opts.cache_redis_uri
  firehoseRedisUri:    opts.firehose_redis_uri
  mongoDBUri:          opts.mongodb_uri
  pepper:              opts.pepper
  workerName:          opts.worker_name
  aliasServerUri:      opts.alias_server_uri
  jobLogRedisUri:      opts.job_log_redis_uri
  jobLogQueue:         opts.job_log_queue
  jobLogSampleRate:    opts.job_log_sample_rate
  privateKey:          privateKey
  publicKey:           publicKey
  singleRun:           opts.single_run
  requestQueueName:    opts.request_queue_name
}

dispatcherWorker = new DispatcherWorker options

sigtermHandler = new SigtermHandler({ events: ['SIGTERM', 'SIGINT'] })
sigtermHandler.register dispatcherWorker.stop

dispatcherWorker.catchErrors()

dispatcherWorker.prepare (error) =>
  if error
    dispatcherWorker.reportError error
    console.error error.stack
    process.exit 1

  dispatcherWorker.run (error) =>
    if error
      dispatcherWorker.reportError error
      console.error error.stack
      process.exit 1
    process.exit 0
