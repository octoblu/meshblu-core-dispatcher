JobRegistry = require '../src/job-registry'

describe 'JobRegistry', ->
  context "one", ->
    beforeEach ->
      @sut = new JobRegistry
        jobs:
          TiedUp:
            start: 'our-safe-word-will-be-dont-stop'
            tasks:
              'our-safe-word-will-be-dont-stop':
                filter: 'SpiderBite'
                on:
                  418: 'exhausted'
              'exhausted':
                task: 'meshblu-core-task-used-up'

        filters:
          SpiderBite:
            start: "im-starting-to-suspect-that-comics-lied-to-you"
            tasks:
              'im-starting-to-suspect-that-comics-lied-to-you':
                task: 'lie-to-you'

    describe '->toJSON', ->
      beforeEach ->
        @result = @sut.toJSON()

      it 'should return a json-ified version of the registry', ->
        expect(@result).to.deep.equal
          TiedUp:
            start: 'our-safe-word-will-be-dont-stop'
            tasks:
              'our-safe-word-will-be-dont-stop':
                task: 'meshblu-core-task-no-content'
                on:
                  204: 'im-starting-to-suspect-that-comics-lied-to-you'
              'im-starting-to-suspect-that-comics-lied-to-you':
                task: 'lie-to-you'
                on:
                  418: 'exhausted'
              'exhausted':
                task: 'meshblu-core-task-used-up'

  context "another one", ->
    beforeEach ->
      @sut = new JobRegistry
        jobs:
          TiedUp:
            start: 'our-safe-word-will-be-dont-stop'
            tasks:
              'our-safe-word-will-be-dont-stop':
                filter: 'SpiderBite'
                on:
                  418: 'exhausted'
              'exhausted':
                task: 'meshblu-core-task-used-up'

        filters:
          SpiderBite:
            start: "im-starting-to-suspect-that-comics-lied-to-you"
            tasks:
              'im-starting-to-suspect-that-comics-lied-to-you':
                task: 'lie-to-you'
                on:
                  418: 'broken'
              'broken':
                task: 'bro-ken'

    describe '->toJSON', ->
      beforeEach ->
        @result = @sut.toJSON()

      it 'should return a json-ified version of the registry', ->
        expect(@result).to.deep.equal
          TiedUp:
            start: 'our-safe-word-will-be-dont-stop'
            tasks:
              'our-safe-word-will-be-dont-stop':
                task: 'meshblu-core-task-no-content'
                on:
                  204: 'im-starting-to-suspect-that-comics-lied-to-you'
              'im-starting-to-suspect-that-comics-lied-to-you':
                task: 'lie-to-you'
                on:
                  418: 'broken'
              'broken':
                task: 'bro-ken'
                on:
                  418: 'exhausted'
              'exhausted':
                task: 'meshblu-core-task-used-up'

  context "a filter with a filter", ->
    beforeEach ->
      @sut = new JobRegistry
        jobs:
          TiedUp:
            start: 'our-safe-word-will-be-dont-stop'
            tasks:
              'our-safe-word-will-be-dont-stop':
                filter: 'SpiderBite'
                on:
                  418: 'exhausted'
              'exhausted':
                task: 'meshblu-core-task-used-up'

        filters:
          SpiderBite:
            start: 'really-check-for-ant-bites'
            tasks:
              'really-check-for-ant-bites':
                filter: 'AntBite'

          AntBite:
            start: 'im-starting-to-suspect-that-comics-lied-to-you'
            tasks:
              'im-starting-to-suspect-that-comics-lied-to-you':
                task: 'lie-to-you'
                on:
                  418: 'broken'
              'broken':
                task: 'bro-ken'

    describe '->toJSON', ->
      beforeEach ->
        @result = @sut.toJSON()

      it 'should return a json-ified version of the registry', ->
        expect(@result).to.deep.equal
          TiedUp:
            start: 'our-safe-word-will-be-dont-stop'
            tasks:
              'our-safe-word-will-be-dont-stop':
                task: 'meshblu-core-task-no-content'
                on:
                  204: 'really-check-for-ant-bites'
              'really-check-for-ant-bites':
                task: 'meshblu-core-task-no-content'
                on:
                  204: 'im-starting-to-suspect-that-comics-lied-to-you'
                  418: 'exhausted'
              'im-starting-to-suspect-that-comics-lied-to-you':
                task: 'lie-to-you'
                on:
                  418: 'broken'
              'broken':
                task: 'bro-ken'
                on:
                  418: 'exhausted'
              'exhausted':
                task: 'meshblu-core-task-used-up'
