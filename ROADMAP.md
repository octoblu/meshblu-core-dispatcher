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
- [x] POST /devices (register)
- [x] DELETE /devices/:uuid (unregister)
- [x] GET /devices (search)
- [x] GET /v2/devices (search)
- [x] POST /v2/devices/:subscriberUuid/subscriptions/:emitterUuid/:type (create subscription)
- [x] DELETE /v2/devices/:subscriberUuid/subscriptions/:emitterUuid/:type (delete subscription)
- [x] GET /devices/:uuid/publickey (device publicKey)
- [x] POST /claimdevice/:uuid (claim device)
- [ ] POST /devices/:uuid/token (reset root token)
- [ ] POST /devices/:uuid/tokens (create session tokens)
- [ ] DELETE /devices/:uuid/tokens/:token (revoke session token)
- [x] GET /mydevices (devices with owner)
- [x] GET /authenticate/:uuid (authenticate uuid)

### Protocol Adapters that need to be built
 - [ ] MQTT
 - [ ] CoAP
 - [ ] XMPP
 - [ ] SNMP
 - [ ] SMTP
