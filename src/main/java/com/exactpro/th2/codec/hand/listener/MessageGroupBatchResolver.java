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

package com.exactpro.th2.codec.hand.listener;

import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;

import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageGroupBatchResolver {

    private final Map<AnyMessage.KindCase, MessageGroupBatch.Builder> messageGroupBatchByType = new EnumMap<>(AnyMessage.KindCase.class);
    
    public void resolveGroups(Map<AnyMessage.KindCase, MessageGroup> messageGroupByType) {
        messageGroupByType.forEach(this::addMessageByType);
    }

    public Collection<MessageGroupBatch> releaseBatches() {
        return messageGroupBatchByType.values().stream()
                .map(MessageGroupBatch.Builder::build)
                .collect(Collectors.toList());
    }


    private void addMessageByType(AnyMessage.KindCase type, MessageGroup messageGroup) {
        MessageGroupBatch.Builder MessageGroupBatchBuilder = messageGroupBatchByType.computeIfAbsent(type, kindCase -> MessageGroupBatch.newBuilder());
        MessageGroupBatchBuilder.addGroups(messageGroup);
    }
    
}
