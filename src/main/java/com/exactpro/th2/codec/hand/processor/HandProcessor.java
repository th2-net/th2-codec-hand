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
import java.util.Map;

/*
    Following class does RawMessage processing
    and Message generation
 */

@Slf4j
public class HandProcessor {
    public static final String DEFAULT_MESSAGE_TYPE = "th2-hand";
    private final HandProcessorConfiguration configuration;

    public HandProcessor(HandProcessorConfiguration configuration) {
        this.configuration = configuration;

    }
    
    private MessageID.Builder getMessageIdBuilder(RawMessage rawMessage) {
        return rawMessage.getMetadata().getId().toBuilder();
    }

    private MessageMetadata.Builder getMetaDataBuilder(RawMessage rawMessage) {
        RawMessageMetadata metadata = rawMessage.getMetadata();
        return MessageMetadata.newBuilder().setId(metadata.getId()).setTimestamp(metadata.getTimestamp())
                .putAllProperties(metadata.getPropertiesMap()).setProtocol(metadata.getProtocol());
    }

    private RawMessageMetadata.Builder getRawMetaDataBuilder(RawMessage rawMessage) {
        return rawMessage.getMetadata().toBuilder();
    }

    public List<AnyMessage> process (RawMessage rawMessage, Integer subsequenceNumber) throws Exception {
        List<AnyMessage> messages = new ArrayList<>();

        ObjectMapper objectMapper;

        objectMapper = new ObjectMapper();
        String body = new String(rawMessage.getBody().toByteArray());
        Map<?, ?> jsonMap = objectMapper.readValue(body, HashMap.class);

        MessageID.Builder messageIdBuilder = this.getMessageIdBuilder(rawMessage);
        RawMessageMetadata.Builder rawMetaDataBuilder = this.getRawMetaDataBuilder(rawMessage);
        MessageMetadata.Builder metaDataBuilder = this.getMetaDataBuilder(rawMessage);

        for (var entry : jsonMap.entrySet()) {
            AnyMessage.Builder anyMsgBuilder = AnyMessage.newBuilder();
            if (entry.getKey().equals(configuration.getContentKey())) {
                RawMessageMetadata rawMsgMetaData = rawMetaDataBuilder.setId(messageIdBuilder.clearSubsequence()
                        .addSubsequence(subsequenceNumber)).build();

                Object value = entry.getValue();
                if (!(value instanceof String)) {
                    log.error("Expected value for {} is string but received: {}", entry.getKey(),
                            (value == null ? "null" : value.getClass().toString()));
                    continue;
                }

                anyMsgBuilder.setRawMessage(RawMessage.newBuilder().setMetadata(rawMsgMetaData)
                        .setBody(ByteString.copyFrom(((String) value).getBytes())));
            } else {
                MessageMetadata msgmetaData = metaDataBuilder.setId(messageIdBuilder.clearSubsequence()
                        .addSubsequence(subsequenceNumber)).setMessageType(DEFAULT_MESSAGE_TYPE).build();
                
                String key = String.valueOf(entry.getKey());
                Value value = this.convertToValue(entry.getValue());
                
                anyMsgBuilder.setMessage(Message.newBuilder().setMetadata(msgmetaData).putFields(key, value));
            }
            
            messages.add(anyMsgBuilder.build());
            subsequenceNumber ++;
        }

        return messages;
    }
    
    private Value convertToValue (Object value) {
        if (value instanceof List) {
            ListValue.Builder listValueBuilder = ListValue.newBuilder();
            for (var o : ((List<?>) value)) {
                listValueBuilder.addValues(this.convertToValue(o));
            }
            return Value.newBuilder().setListValue(listValueBuilder).build();
        }
        
        if (value instanceof Map) {
            Message.Builder msgBuilder = Message.newBuilder();
            for (var o1 : ((Map<?, ?>) value).entrySet()) {
                msgBuilder.putFields(String.valueOf(o1.getKey()), convertToValue(o1.getValue()));
            }
            return Value.newBuilder().setMessageValue(msgBuilder).build();
        }
        
        return Value.newBuilder().setSimpleValue(String.valueOf(value)).build();
    }
}


