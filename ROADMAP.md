# Roadmap for Meshblu Core Dispatcher

### Endpoints that need to be replaced
- [x] Messaging
  - [x] GET /subscribe/:uuid
  - [x] GET /subscribe/:uuid/broadcast
  - [x] GET /subscribe/:uuid/received
  - [x] GET /subscribe/:uuid/sent
  - [x] GET /subscribe
- [x] GET /status
- [x] GET /publickey
- [ ] POST /devices (register)
- [ ] DELETE /devices/:uuid (unregister)
- [ ] GET /devices (search)
- [ ] GET /v2/devices (search)
- [ ] POST /v2/devices/:subscriberUuid/subscriptions/:emitterUuid/:type (create subscription)
- [ ] DELETE /v2/devices/:subscriberUuid/subscriptions/:emitterUuid/:type (delete subscription)
- [x] GET /devices/:uuid/publickey
- [ ] POST /claimdevice/:uuid
- [ ] POST /devices/:uuid/token (create token)
- [ ] POST /devices/:uuid/tokens (create tokens)
- [ ] DELETE /devices/:uuid/tokens/:token
- [ ] GET /mydevices
- [ ] GET /authenticate/:uuid

### Protocol Adapters that need to be built
 - [ ] MQTT
 - [ ] CoAP
 - [ ] XMPP
 - [ ] SNMP
 - [ ] SMTP
