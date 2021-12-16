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

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.mutable.MutableInt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.exactpro.th2.codec.hand.decoder.HandDecoder.MESSAGE_TYPE;

public class FixMessageHandProcessor extends AbstractHandProcessor<RawMessage> {

    private static final String ACTION_RESULTS = "ActionResults";
    private static final String ACTION_DATA = "data";
    private static final String EXECUTION_ID = "ExecutionId";

    @Override
    public List<AnyMessage> processMessage(Map<?, ?> convertedMessage, RawMessage message, MutableInt subSequenceNumber) {
        Objects.requireNonNull(convertedMessage, "Converted message cannot be null");

        Object rawActionResults = Objects.requireNonNull(
                convertedMessage.get(ACTION_RESULTS),
                "RawMessage cannot be processed because 'ActionResults' key is missing"
        );
        if (!(rawActionResults instanceof List<?>)) {
            throw new IllegalStateException("Action results has invalid type");
        }
        List<?> actionResults = (List<?>) rawActionResults;
        List<AnyMessage> messages = new ArrayList<>(actionResults.size());
        String messageType = getMessageType().getValue();
        MessageID.Builder messageIdBuilder = this.getMessageIdBuilder(message);
        var rawMessageMetadataBuilder = getRawMetaDataBuilder(message)
                .setId(messageIdBuilder
                        .clearSubsequence()
                        .addSubsequence(subSequenceNumber.getAndIncrement())
                )
                .setProtocol(messageType)
                .putProperties(MESSAGE_TYPE, messageType);
        Object executionId = convertedMessage.get(EXECUTION_ID);
        if (executionId instanceof String) {
            rawMessageMetadataBuilder.putProperties(EXECUTION_ID, (String) executionId);
        }
        var rawMessageMetadata = rawMessageMetadataBuilder.build();

        for (Object value : actionResults) {
            if (!(value instanceof Map<?, ?>)) {
                throw new IllegalStateException("Action result has invalid type");
            }
            Map<?, ?> keyValuePair = (Map<?, ?>) value;
            Object actionValue = Objects.requireNonNull(keyValuePair.get(ACTION_DATA), "Action value is missing");
            if (!(actionValue instanceof String)) {
                throw new IllegalStateException("Action value has invalid type");
            }
            RawMessage newRawMessage = message.toBuilder()
                    .setMetadata(rawMessageMetadata)
                    .setBody(ByteString.copyFrom((String) actionValue, StandardCharsets.UTF_8))
                    .build();

            var anyMessage = AnyMessage.newBuilder()
                    .setRawMessage(newRawMessage)
                    .build();

            messages.add(anyMessage);
        }

        return messages;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.FIX;
    }
}
