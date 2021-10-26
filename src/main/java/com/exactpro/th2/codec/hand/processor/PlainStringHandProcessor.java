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
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.Value;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PlainStringHandProcessor extends AbstractHandProcessor<RawMessage> {
    public static final String DEFAULT_MESSAGE_TYPE = "th2-hand";

    @Override
    public List<AnyMessage> processMessage(Map<?, ?> convertedMessage, RawMessage message, MutableInt subSequenceNumber) {
        MessageID.Builder messageIdBuilder = this.getMessageIdBuilder(message);
        MessageMetadata.Builder metaDataBuilder = this.getMetaDataBuilder(message);

        List<AnyMessage> messages = new ArrayList<>(convertedMessage.size());
        for (var node : convertedMessage.entrySet()) {
            AnyMessage.Builder anyMsgBuilder = AnyMessage.newBuilder();

            MessageMetadata msgMetaData = metaDataBuilder.setId(messageIdBuilder.clearSubsequence()
                    .addSubsequence(subSequenceNumber.getAndIncrement())).setMessageType(DEFAULT_MESSAGE_TYPE).build();

            String key = String.valueOf(node.getKey());
            Value value = convertToValue(node.getValue());

            anyMsgBuilder.setMessage(Message.newBuilder().setMetadata(msgMetaData).putFields(key, value));
            messages.add(anyMsgBuilder.build());
        }
        return messages;
    }

    @Override
    public MessageType getMessageType() {
        return MessageType.PLAIN_STRING;
    }
}
