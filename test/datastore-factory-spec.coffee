DatastoreFactory = require '../src/datastore-factory'
Datastore = require 'meshblu-core-datastore'

describe 'DatastoreFactory', ->
  beforeEach (done) ->
    @sut = new DatastoreFactory database: 'helicopter'
    @datastore = new Datastore database: 'helicopter', collection: 'ToTheChopper'
    @datastore.remove done

  beforeEach (done) ->
    datastore = @sut.build 'ToTheChopper'
    datastore.insert uuid: 'shoelace', token: 'maybe-just-stick-with-velcro', done

  it 'should use my datastore', (done) ->
    @datastore.findOne uuid: 'shoelace', (error, record) =>
      return done error if error?
      expect(record).to.deep.equal uuid: 'shoelace', token: 'maybe-just-stick-with-velcro'
      done()
