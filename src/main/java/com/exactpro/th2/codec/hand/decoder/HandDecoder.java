package com.exactpro.th2.codec.hand.decoder;

import com.exactpro.th2.codec.hand.processor.HandProcessor;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.RawMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HandDecoder {

    private final HandProcessor handProcessor;

    public HandDecoder(HandProcessor handProcessor) {
        this.handProcessor = handProcessor;
    }

    public MessageGroup decode (MessageGroup group) {
        MessageGroup.Builder messageGroupBuilder = MessageGroup.newBuilder();

        Integer subsequenceNumber = 1;
        for (var msg : group.getMessagesList()) {
            try {
                if (msg.hasMessage()) {
                    messageGroupBuilder.addMessages(AnyMessage.newBuilder().setMessage(msg.getMessage()).build());
                    continue;
                }
                var output = handProcessor.process(msg.getRawMessage(), subsequenceNumber);

                for (var el : output) {
                    if (el instanceof Message) {
                        messageGroupBuilder.addMessages(AnyMessage.newBuilder().setMessage((Message) el).build());
                    } else {
                        messageGroupBuilder.addMessages(AnyMessage.newBuilder().setRawMessage((RawMessage) el).build());
                    }
                }
            } catch (Exception e) {
                log.error("Exception decoding message", e);

                return null;
            }
        }

        log.info("Finished decoding RawMessages");
        return messageGroupBuilder.build();
    }
}
