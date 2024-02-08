package com.exactpro.th2.codec.hand.decoder;

import com.exactpro.th2.codec.hand.processor.HandProcessor;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HandDecoder {
    private static final Logger log = LoggerFactory.getLogger(HandDecoder.class);

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
                
                List<AnyMessage> output = handProcessor.process(msg.getRawMessage(), subsequenceNumber);
                output.forEach(messageGroupBuilder::addMessages);
            } catch (Exception e) {
                log.error("Exception decoding message", e);
                return null;
            }
        }

        log.info("Finished decoding RawMessages");
        return messageGroupBuilder.build();
    }
}
