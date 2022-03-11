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

import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.Value;
import com.google.protobuf.AbstractMessage;

import java.util.List;
import java.util.Map;

public abstract class AbstractHandProcessor<T extends AbstractMessage> implements HandProcessor<T> {

    protected MessageID.Builder getMessageIdBuilder(RawMessage rawMessage) {
        return rawMessage.getMetadata().getId().toBuilder();
    }

    protected MessageMetadata.Builder getMetaDataBuilder(RawMessage rawMessage) {
        RawMessageMetadata metadata = rawMessage.getMetadata();
        return MessageMetadata.newBuilder().setId(metadata.getId()).setTimestamp(metadata.getTimestamp())
                .putAllProperties(metadata.getPropertiesMap()).setProtocol(metadata.getProtocol());
    }

    protected RawMessageMetadata.Builder getRawMetaDataBuilder(RawMessage rawMessage) {
        return rawMessage.getMetadata().toBuilder();
    }

    protected Value convertToValue(Object value) {
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
