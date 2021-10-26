# th2 codec hand (0.2.0)

This microservice can decode messages received from component th2-hand.
Example of th2-hand output:

```json
{
  "ScriptOutputCode": "SUCCESS",
  "ErrorText": "",
  "ActionResults": [
    {
      "id": "th2_hand_action_id1",
      "data": "8=FIXT.1.1\u00019=259\u000135=D\u0001..."
    }
  ],
  "RhSessionId": "th2_hand"
}
```
th2-hand codec will decode this kind of messages to display separated fields
in the report-viewer and some fields will be passed as raw and can be accessible by other codecs in codec-pipeline.

## Configuration

Main configuration is done by setting the following properties in custom configuration block:

1. `contentKey` (default value: `ActionResults`)
2. `resultKey` (default value `data`)

both option should refer to fields that should be passed as raw messages.

```
{
  "ScriptOutputCode": "SUCCESS",
  "ErrorText": "",
  "ActionResults": [                                     <------ this is contentKey
    {
      "id": "th2_hand_action_id1",
      "data": "8=FIXT.1.1\u00019=259\u000135=D\u0001..." <------ this is resultKey
    },
    {
      "id": "th2_hand_action_id2",
      "data": "8=FIXT.1.1\u00019=259\u000135=D\u0001..."
    },
  ],
  "RhSessionId": "th2_hand"
}
```

These options are already configured to actual `th2-hand` version by default and user doesn't have to change or specify them. 


## Deployment via `infra-mgr`

Here's an example of CRD file config that is required to deploy this service

```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2Box
metadata:
  name: codec-hand
spec:
  image-name: ghcr.io/th2-net/th2-codec-hand
  image-version: 0.1.4
  type: th2-codec
  pins:
    # decoder
    - name: in_codec_decode
      connection-type: mq
      attributes:
        - decoder_in
        - raw
        - subscribe
    - name: out_codec_decode
      connection-type: mq
      attributes:
        - decoder_out
        - parsed
        - publish
        - store
    # decoder general (technical)
    - name: in_codec_general_decode
      connection-type: mq
      attributes:
        - general_decoder_in
        - raw
        - subscribe
    - name: out_codec_general_decode
      connection-type: mq
      attributes:
        - general_decoder_out
        - parsed
        - publish
  extended-settings:
    service:
      enabled: false
```

## Release notes

### 0.2.0

+ Added the ability to process `FIX` messages

### 0.1.4

+ fixed issues with incorrect processing of message groups

### 0.1.3

+ fixed reading of raw messages
+ updated github workflow

### 0.1.2

+ updated format of action results

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