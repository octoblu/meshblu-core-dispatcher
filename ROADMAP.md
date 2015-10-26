# Roadmap for Meshblu Core Dispatcher

1. Request and response should have the same format `[metadata, data]`
  e.g.
  ```
[{
    jobType: "sendMessage",
    requestId: "425e451f-bcc0-4762-b0a1-f6077b820459",
    options: {
      devices: ["*"],
      topic: "blah"
    }
},
  "IyBSb2FkbWFwIGZvciBNZXNoYmx1IENvcmUgRGlzcGF0Y2hlixvYWQ6IHt9CiAgfV0KYGBgCg=="
]
```
1. Network/local abstraction layer for tasks
