/*
 Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.codec.hand.util.RawMessageConverter;
import com.exactpro.th2.common.event.EventUtils;
import com.exactpro.th2.common.grpc.*;
import com.exactpro.th2.common.message.MessageUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractHandProcessorTest {
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    protected static final RawMessageConverter RAW_MESSAGE_CONVERTER = new RawMessageConverter();


    protected static AnyMessage createAnyMessage(Message message, RawMessage rawMessage) {
        AnyMessage.Builder anyMessage = AnyMessage.newBuilder();
        if (message != null) {
            anyMessage.setMessage(message);
        }
        if (rawMessage != null) {
            anyMessage.setRawMessage(rawMessage);
        }
        return anyMessage.build();
    }

    protected static RawMessage createRawMessage(String parentEventId, RawMessageMetadata rawMessageMetadata, Map<String, Object> body) throws JsonProcessingException {
        EventID eventID = EventID.newBuilder().setId(EventUtils.generateUUID()).build();
        return createRawMessage(eventID, rawMessageMetadata, body);
    }

    protected static RawMessage createRawMessage(EventID parentEventId, RawMessageMetadata rawMessageMetadata, Map<String, Object> body) throws JsonProcessingException {
        return RawMessage.newBuilder()
                .setMetadata(rawMessageMetadata)
                .setParentEventId(parentEventId)
                .setBody(createPayloadBody(body))
                .build();
    }

    protected static RawMessageMetadata createRawMessageMetadata(MessageID messageID,
                                                                 Instant instant,
                                                                 String protocol,
                                                                 Map<String, String> properties) {
        return RawMessageMetadata.newBuilder()
                .setTimestamp(MessageUtils.toTimestamp(instant))
                .setProtocol(protocol)
                .setId(messageID)
                .putAllProperties(properties)
                .build();
    }

    protected static MessageID createMessageID(long sequence, Direction direction, ConnectionID connectionID) {
        return MessageID.newBuilder()
                .setSequence(sequence)
                .setDirection(direction)
                .setConnectionId(connectionID)
                .build();
    }

    protected static ConnectionID createConnectionID(String sessionAlias) {
        return ConnectionID.newBuilder()
                .setSessionAlias(sessionAlias)
                .build();
    }

    protected static ByteString createPayloadBody(Map<String, Object> body) throws JsonProcessingException {
        return ByteString.copyFrom(MAPPER.writeValueAsBytes(body));
    }

    protected static Map<String, Value> createKeyValueMap(Map<String, Object> body) {
        return body.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, k -> convertToValue(k.getValue())));
    }

    protected static Value convertToValue(Object value) {
        if (value instanceof List) {
            ListValue.Builder listValueBuilder = ListValue.newBuilder();
            for (var o : ((List<?>) value)) {
                listValueBuilder.addValues(convertToValue(o));
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
