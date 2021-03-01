/*
 Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.exactpro.th2.codec.hand.processor;

import com.exactpro.th2.common.grpc.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
    Following class does RawMessage processing
    and Message generation
 */

@Slf4j
public class HandProcessor {
    private final HandProcessorConfiguration configuration;

    public HandProcessor(HandProcessorConfiguration configuration) {
        this.configuration = configuration;

    }

    public List<GeneratedMessageV3> process (RawMessage rawMessage, Integer subsequenceNumber) throws Exception {
        List<GeneratedMessageV3> messages = new ArrayList<>();

        ObjectMapper objectMapper;

        objectMapper = new ObjectMapper();
        String body = new String(rawMessage.getBody().toByteArray());



        HashMap<String, String> jsonMap = objectMapper.readValue(body, HashMap.class);

        for (var entry : jsonMap.entrySet()) {
            GeneratedMessageV3 msg;
            if (entry.getKey().equals(configuration.getContentKey())) {
                msg  = RawMessage.newBuilder()
                        .setMetadata(RawMessageMetadata.newBuilder()
                                .setId(MessageID.newBuilder()
                                        .setConnectionId(rawMessage.getMetadata().getId().getConnectionId())
                                        .setDirection(rawMessage.getMetadata().getId().getDirection())
                                        .setSequence(rawMessage.getMetadata().getId().getSequence())
                                        .addSubsequence(subsequenceNumber)
                                        .build())
                                .setTimestamp(rawMessage.getMetadata().getTimestamp())
                                .putAllProperties(rawMessage.getMetadata().getPropertiesMap())
                                .setProtocol(rawMessage.getMetadata().getProtocol())
                                .build())
                        .setBody(ByteString.copyFrom(entry.getValue().getBytes()))
                        .build();
            } else {
                msg = Message.newBuilder()
                        .setMetadata(MessageMetadata.newBuilder()
                                .setId(MessageID.newBuilder()
                                        .setConnectionId(rawMessage.getMetadata().getId().getConnectionId())
                                        .setDirection(rawMessage.getMetadata().getId().getDirection())
                                        .setSequence(rawMessage.getMetadata().getId().getSequence())
                                        .addSubsequence(subsequenceNumber)
                                        .build())
                                .setTimestamp(rawMessage.getMetadata().getTimestamp())
                                .putAllProperties(rawMessage.getMetadata().getPropertiesMap())
                                .setProtocol(rawMessage.getMetadata().getProtocol())
                                .build())
                        .putFields(entry.getKey(), Value.newBuilder().setSimpleValue(entry.getValue()).build())
                        .build();
            }
            messages.add(msg);

            subsequenceNumber ++;
        }

        return messages;
    }
}


