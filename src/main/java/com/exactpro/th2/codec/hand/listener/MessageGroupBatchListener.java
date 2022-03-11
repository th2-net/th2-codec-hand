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

package com.exactpro.th2.codec.hand.listener;

import com.exactpro.th2.codec.hand.decoder.HandDecoder;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.MessageGroupBatch;
import com.exactpro.th2.common.schema.message.MessageListener;
import com.exactpro.th2.common.schema.message.MessageRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
    Listener receives RawMessages, uses FixDecoder and
    sends generated MessageBatch via MessageRouter
 */

public class MessageGroupBatchListener implements MessageListener<MessageGroupBatch> {

    private static final Logger log = LoggerFactory.getLogger(MessageGroupBatchListener.class);

    private MessageRouter<MessageGroupBatch> batchGroupRouter;
    private HandDecoder handDecoder;

    public MessageGroupBatchListener(MessageRouter<MessageGroupBatch> batchGroupRouter, HandDecoder handDecoder) {
        this.batchGroupRouter = batchGroupRouter;
        this.handDecoder = handDecoder;
    }

    @Override
    public void handler(String consumerTag, MessageGroupBatch message) {

        MessageGroupBatch.Builder outputBatchBuilder = MessageGroupBatch.newBuilder();
        
        try {
            
            int groupNumber = 0;
            for (MessageGroup messageGroup : message.getGroupsList()) {
                ++groupNumber;
                MessageGroup decodedGroup = handDecoder.decode(messageGroup);

                if (decodedGroup == null) {
                    log.info("Exception happened during decoding group {}, router won't send anything", groupNumber);
                    continue;
                }

                if (decodedGroup.getMessagesCount() == 0) {
                    log.info("Messages weren't found in this group {}, router won't send anything", groupNumber);
                    continue;
                }
                outputBatchBuilder.addGroups(decodedGroup);
            }
            
            batchGroupRouter.sendAll(outputBatchBuilder.build());
            
        } catch (Exception e) {
            log.error("Exception sending message(s)", e);
        }
    }

    @Override
    public void onClose() {

    }
}
