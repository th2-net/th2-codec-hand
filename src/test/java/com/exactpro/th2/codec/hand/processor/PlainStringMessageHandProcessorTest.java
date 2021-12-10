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
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PlainStringMessageHandProcessorTest extends AbstractHandProcessorTest {

    private static final PlainStringHandProcessor PLAIN_STRING_MESSAGE_HAND_PROCESSOR = new PlainStringHandProcessor();


    @Test
    public void checkThatMessageHasCorrectMessageType() throws JsonProcessingException {
        MessageID messageID = createMessageID(1, Direction.FIRST, createConnectionID("test_session_alias"));
        RawMessageMetadata rawMessageMetadata = createRawMessageMetadata(messageID, Instant.now(), StringUtils.EMPTY, Map.of(
                "A", "1",
                "B", "2"
        ));
        RawMessage rawMessage = createRawMessage("root", rawMessageMetadata, Map.of("MessageType", "PLAIN_STRING"));

        Map<?, ?> convertedMessage = RAW_MESSAGE_CONVERTER.convert(rawMessage);
        List<AnyMessage> messages = PLAIN_STRING_MESSAGE_HAND_PROCESSOR.processMessage(convertedMessage, rawMessage, new MutableInt(0));

        Assertions.assertEquals(1, messages.size());
        AnyMessage anyMessage = messages.get(0);
        Assertions.assertTrue(anyMessage.hasMessage());
        RawMessageMetadata expectedMetadata = rawMessage.getMetadata();
        MessageMetadata actualMetadata = anyMessage.getMessage().getMetadata();
        Assertions.assertEquals(expectedMetadata.getPropertiesMap(), actualMetadata.getPropertiesMap());
        Assertions.assertEquals("th2-hand", actualMetadata.getMessageType());
    }

    @Test
    public void checkThatMessageHasCorrectFields() throws JsonProcessingException {
        MessageID messageID = createMessageID(1, Direction.FIRST, createConnectionID("test_session_alias"));
        RawMessageMetadata rawMessageMetadata = createRawMessageMetadata(messageID, Instant.now(), StringUtils.EMPTY, Map.of(
                "A", "1",
                "B", "2"
        ));
        Map<String, Object> expectedFields = Map.of(
                "MessageType", "PLAIN_STRING",
                "ExecutionId", "1234567890",
                "RhSessionId", "session_id"
        );
        RawMessage rawMessage = createRawMessage("root", rawMessageMetadata, expectedFields);

        Map<?, ?> convertedMessage = RAW_MESSAGE_CONVERTER.convert(rawMessage);
        List<AnyMessage> messages = PLAIN_STRING_MESSAGE_HAND_PROCESSOR.processMessage(convertedMessage, rawMessage, new MutableInt(0));

        Assertions.assertEquals(1, messages.size());
        Assertions.assertTrue(messages.stream().allMatch(AnyMessage::hasMessage));
        Assertions.assertTrue(
                messages.stream()
                        .allMatch(m ->
                                m.getMessage().getMetadata().getPropertiesMap().equals(rawMessage.getMetadata().getPropertiesMap())
                        )
        );
        List<Map<String, Value>> actualFieldsList = messages.stream().map(m -> m.getMessage().getFieldsMap()).collect(Collectors.toList());
        Assertions.assertEquals(1, actualFieldsList.size());
        Assertions.assertEquals(expectedFields.size(), actualFieldsList.get(0).size());

        List<Map<String, Value>> expectedFieldsList = Collections.singletonList(createKeyValueMap(expectedFields));
        
        Assertions.assertTrue(expectedFieldsList.containsAll(actualFieldsList));
    }
}
