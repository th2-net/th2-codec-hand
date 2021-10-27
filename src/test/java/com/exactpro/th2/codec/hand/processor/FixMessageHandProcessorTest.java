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
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class FixMessageHandProcessorTest extends AbstractHandProcessorTest {

    private static final FixMessageHandProcessor FIX_MESSAGE_HAND_PROCESSOR = new FixMessageHandProcessor();

    @Test
    public void fixHandProcessorChangesProtocol() throws JsonProcessingException {
        MessageID messageID = createMessageID(1, Direction.FIRST, createConnectionID("test_session_alias"));
        RawMessageMetadata rawMessageMetadata = createRawMessageMetadata(messageID, Instant.now(), "th2_hand", Map.of(
                "A", "1",
                "B", "2"
        ));
        RawMessage rawMessage = createRawMessage("root", rawMessageMetadata, Map.of("MessageType", "FIX"));

        List<AnyMessage> messages = FIX_MESSAGE_HAND_PROCESSOR.processMessage(null, rawMessage, new MutableInt(0));

        Assertions.assertEquals(1, messages.size());
        AnyMessage anyMessage = messages.get(0);
        Assertions.assertFalse(anyMessage.hasMessage());
        RawMessage receivedRawMessage = anyMessage.getRawMessage();
        RawMessageMetadata expectedMetadata = rawMessage.getMetadata();
        RawMessageMetadata actualMetadata = receivedRawMessage.getMetadata();
        Assertions.assertNotEquals(expectedMetadata.getProtocol(), actualMetadata.getProtocol());
        Assertions.assertEquals(expectedMetadata.getPropertiesMap(), actualMetadata.getPropertiesMap());
        Assertions.assertEquals(rawMessage.getBody(), receivedRawMessage.getBody());
    }
}
