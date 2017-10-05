dashdash         = require 'dashdash'
_                = require 'lodash'
SigtermHandler   = require 'sigterm-handler'

DispatcherWorker = require './src/dispatcher-worker'
packageJSON      = require './package.json'

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
    env: 'NAMESPACE'
    default: 'meshblu'
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
    name: 'datastore-cache-ttl'
    type: 'number'
    help: 'TTL of cache optimized datastore actions'
    env: 'DATASTORE_CACHE_TTL'
    default: 216000
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
    default: 'sample-rate:1.00'
  }
  {
    name: 'job-log-namespace'
    type: 'string'
    help: 'Job log index name metric:meshblu-core-dispatcher[-task]:namespace'
    env: 'JOB_LOG_NAMESPACE'
  }
  {
    name: 'job-log-sample-rate'
    type: 'number'
    help: 'Job log sample rate (0.00 to 1.00)'
    env: 'JOB_LOG_SAMPLE_RATE'
    default: 0
  }
  {
    name: 'job-log-sample-rate-override-uuids'
    type: 'string'
    help: 'UUIDs that bypass the sample-rate, comma seperated (no spaces please)'
    env:  'JOB_LOG_SAMPLE_RATE_OVERRIDE_UUIDS'
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
    default: 'v2:request:queue'
  }
  {
    name: 'concurrency'
    type: 'positiveInteger'
    help: 'number of concurrent jobs to process'
    env: 'CONCURRENCY'
    default: 10
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
  cacheRedisUri:       opts.cache_redis_uri ? opts.redis_uri
  firehoseRedisUri:    opts.firehose_redis_uri ? opts.redis_uri
  mongoDBUri:          opts.mongodb_uri
  pepper:              opts.pepper
  workerName:          opts.worker_name
  aliasServerUri:      opts.alias_server_uri
  jobLogNamespace:     opts.job_log_namespace
  jobLogRedisUri:      opts.job_log_redis_uri ? opts.redis_uri
  jobLogQueue:         opts.job_log_queue
  jobLogSampleRate:    opts.job_log_sample_rate
  jobLogSampleRateOverrideUuids: _.split(opts.job_log_sample_rate_override_uuids, ',')
  privateKey:          privateKey
  publicKey:           publicKey
  singleRun:           opts.single_run
  requestQueueName:    opts.request_queue_name
  datastoreCacheTTL:   opts.datastore_cache_ttl
  concurrency:         opts.concurrency
}

dispatcherWorker = new DispatcherWorker options

sigtermHandler = new SigtermHandler({ events: ['SIGTERM', 'SIGINT'] })
sigtermHandler.register =>
  dispatcherWorker.stop (error) =>
    if error
      dispatcherWorker.reportError error
      console.error error.stack
      process.exit 1
    process.exit 0

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
    console.log "dispatcher running."
