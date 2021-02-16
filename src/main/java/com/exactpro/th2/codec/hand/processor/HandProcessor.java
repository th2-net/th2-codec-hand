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

import com.exactpro.sf.common.util.Pair;
import com.exactpro.th2.common.grpc.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
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

    public Pair <List<Message>, RawMessage> process (RawMessage rawMessage) throws Exception {
        List <Message> messages = new ArrayList<>();
        RawMessage outputRawMessage = null;

        ObjectMapper objectMapper;

        objectMapper = new ObjectMapper();
        String body = new String(rawMessage.getBody().toByteArray());



        HashMap<String, String> jsonMap = objectMapper.readValue(body, HashMap.class);

        int subsequenceNumber = 1;
        for (var entry : jsonMap.entrySet()) {
            if (entry.getKey().equals(configuration.getContentKey())) {
                outputRawMessage = RawMessage.newBuilder()
                        .setMetadata(RawMessageMetadata.newBuilder()
                                .setId(rawMessage.getMetadata().getId())
                                .setTimestamp(rawMessage.getMetadata().getTimestamp())
                                .putAllProperties(rawMessage.getMetadata().getPropertiesMap())
                                .setProtocol(rawMessage.getMetadata().getProtocol())
                                .build())
                        .setBody(ByteString.copyFrom(entry.getValue().getBytes()))
                        .build();

                continue;
            }

            var msg = Message.newBuilder()
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

            messages.add(msg);

            subsequenceNumber ++;
        }

        return new Pair<>(messages, outputRawMessage);
    }
}


