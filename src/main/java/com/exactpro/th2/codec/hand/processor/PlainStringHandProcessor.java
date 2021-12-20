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

import com.exactpro.th2.common.grpc.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class PlainStringHandProcessor extends AbstractHandProcessor<RawMessage> {
    public static final String DEFAULT_MESSAGE_TYPE = "th2-hand";

    @Override
    public void processMessage(Map<?, ?> convertedMessage, RawMessage message, MutableInt subSequenceNumber, Consumer<AnyMessage> messageConsumer) {
        Objects.requireNonNull(convertedMessage, "Converted message cannot be null");

        MessageID.Builder messageIdBuilder = this.getMessageIdBuilder(message);
        MessageMetadata.Builder metaDataBuilder = this.getMetaDataBuilder(message)
                .setId(messageIdBuilder
                        .clearSubsequence()
                        .addSubsequence(subSequenceNumber.getAndIncrement())
                )
                .setMessageType(DEFAULT_MESSAGE_TYPE);

        AnyMessage.Builder anyMsgBuilder = AnyMessage.newBuilder();

        var messageBuilder = Message.newBuilder().setMetadata(metaDataBuilder);

        for (var node : convertedMessage.entrySet()) {
            String key = String.valueOf(node.getKey());
            Value value = convertToValue(node.getValue());

            messageBuilder.putFields(key, value);
        }

        messageConsumer.accept(anyMsgBuilder.setMessage(messageBuilder).build());
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.PLAIN_STRING;
    }
}
