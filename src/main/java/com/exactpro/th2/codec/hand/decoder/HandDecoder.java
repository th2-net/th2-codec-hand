package com.exactpro.th2.codec.hand.decoder;

import com.exactpro.sf.common.util.Pair;
import com.exactpro.th2.codec.hand.processor.HandProcessor;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.RawMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class HandDecoder {

    private final HandProcessor handProcessor;

    public HandDecoder(HandProcessor handProcessor) {
        this.handProcessor = handProcessor;
    }

    public MessageGroup decode (MessageGroup group) {
        MessageGroup.Builder messageGroupBuilder = MessageGroup.newBuilder();

        for (var msg : group.getMessagesList()) {
            try {
                if (msg.hasMessage()) {
                    messageGroupBuilder.addMessages(AnyMessage.newBuilder().setMessage(msg.getMessage()).build());
                }
                Pair<List<Message>, RawMessage> output = handProcessor.process(msg.getRawMessage());

                for (var el : output.getFirst()) {
                    messageGroupBuilder.addMessages(AnyMessage.newBuilder().setMessage(el).build());
                }

                messageGroupBuilder.addMessages(AnyMessage.newBuilder().setRawMessage(output.getSecond()).build());
            } catch (Exception e) {
                log.error("Exception decoding message", e);

                return null;
            }
        }

        log.info("Finished decoding RawMessages");
        return messageGroupBuilder.build();
    }
}
