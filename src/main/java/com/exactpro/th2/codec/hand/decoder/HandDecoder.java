package com.exactpro.th2.codec.hand.decoder;

import com.exactpro.th2.codec.hand.processor.HandProcessor;
import com.exactpro.th2.codec.hand.processor.MessageType;
import com.exactpro.th2.codec.hand.util.RawMessageConverter;
import com.exactpro.th2.common.grpc.AnyMessage;
import com.exactpro.th2.common.grpc.MessageGroup;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.message.MessageUtils;
import com.google.protobuf.AbstractMessage;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class HandDecoder {

    private static final Logger log = LoggerFactory.getLogger(HandDecoder.class);

    public static final String MESSAGE_TYPE = "MessageType";

    private final RawMessageConverter rawMessageConverter;
    private final Map<MessageType, HandProcessor<AbstractMessage>> handProcessors;


    public HandDecoder(RawMessageConverter rawMessageConverter, Map<MessageType, HandProcessor<AbstractMessage>> handProcessors) {
        this.rawMessageConverter = rawMessageConverter;
        this.handProcessors = handProcessors;
    }

    public MessageGroup decode (MessageGroup group) {
        MessageGroup.Builder messageGroupBuilder = MessageGroup.newBuilder();

        MutableInt subSequenceNumber = new MutableInt(1);
        for (var anyMessage : group.getMessagesList()) {
            try {
                if (log.isInfoEnabled()) {
                    log.info("Start decoding message: {}", MessageUtils.toJson(anyMessage));
                }
                if (anyMessage.hasMessage()) {
                    messageGroupBuilder.addMessages(AnyMessage.newBuilder().setMessage(anyMessage.getMessage()).build());
                    continue;
                }

                RawMessage rawMessage = anyMessage.getRawMessage();
                Map<?, ?> convertedMessage = rawMessageConverter.convert(rawMessage);

                Object messageType = convertedMessage.get(MESSAGE_TYPE);
                if (messageType == null) {
                    log.error("Message {} do not contain a mandatory field '{}' and can not be decoded", convertedMessage, MESSAGE_TYPE);
                    continue;
                }

                List<AnyMessage> messages;

                if (messageType.equals(MessageType.FIX.getValue())) {
                    messages = handProcessors.get(MessageType.FIX).processMessage(convertedMessage, rawMessage, subSequenceNumber);
                } else {
                    //default
                    messages = handProcessors.get(MessageType.PLAIN_STRING).processMessage(convertedMessage, rawMessage, subSequenceNumber);
                }

                messages.forEach(messageGroupBuilder::addMessages);
                log.info("Message successfully decoded");
                if (log.isDebugEnabled())
                    log.debug("Decoded messages: {}", messages);
            } catch (Exception e) {
                log.error("Exception decoding message", e);
                return null;
            }
        }

        return messageGroupBuilder.build();
    }
}
