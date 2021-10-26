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
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FixMessageHandProcessor extends AbstractHandProcessor<RawMessage> {
    @Override
    public List<AnyMessage> processMessage(Map<?, ?> convertedMessage, RawMessage message, MutableInt subSequenceNumber) {
        RawMessageMetadata rawMessageMetadata = getRawMetaDataBuilder(message)
                .setProtocol(getMessageType().getValue())
                .build();

        RawMessage newMessage = message.toBuilder()
                .setMetadata(rawMessageMetadata)
                .build();

        AnyMessage anyMessage = AnyMessage.newBuilder()
                .setRawMessage(newMessage)
                .build();

        return Collections.singletonList(anyMessage);
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.FIX;
    }
}
