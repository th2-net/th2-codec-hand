# th2 codec hand (0.1.0)

## Release notes

### 0.1.1

+ fix Configuration class reference
+ add mainClassName property

### 0.1.0

+ reads dictionaries from the /var/th2/config/dictionary folder
+ uses mq_router, grpc_router, cradle_manager optional JSON configs from the /var/th2/config folder
+ tries to load log4j.properties files from sources in order: '/var/th2/config', '/home/etc', configured path via cmd, default configuration
+ update Cradle version. Introduce async API for storing events
+ removed gRPC event loop handling
+ fixed dictionary reading