DispatcherWorker = require '../src/dispatcher-worker'

describe 'DispatcherWorker', ->
  beforeEach ->
    @sut = new DispatcherWorker
      namespace:           'meshblu-test'
      timeoutSeconds:      1
      redisUri:            'redis://localhost:6379'
      mongoDBUri:          'mongodb://localhost'
      pepper:              'im-a-pepper'
      workerName:          'test-worker'
      aliasServerUri:      null
      jobLogRedisUri:      'redis://localhost:6379'
      jobLogQueue:         'sample-rate:1.00'
      jobLogSampleRate:    1
      intervalBetweenJobs: 1
      privateKey:          'private'
      publicKey:           'public'
      singleRun:           true

  it 'should run', ->
    expect(@sut.run).to.exist
