/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.hand

import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.hand.processor.HandProcessor.Companion.DEFAULT_MESSAGE_TYPE
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.common.value.toListValue
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.ByteString
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoMoreInteractions
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.elementAt
import strikt.assertions.hasSize
import strikt.assertions.isA
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isNotBlank
import strikt.assertions.isSameInstanceAs
import strikt.assertions.isTrue
import java.time.Instant
import com.exactpro.th2.common.grpc.MessageGroup as ProtobufMessageGroup
import com.exactpro.th2.common.grpc.RawMessage as ProtobufRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage as TransportRawMessage

class CodecHandTest {
    private val config = CodecHandSettings()
    private val codec = CodecHand(config)

    private val context: IReportingContext = mock {  }

    private val testContent = """
                        {
                          "${config.contentKey}": [
                            {
                              "${config.resultKey}": "test-result"
                            }
                          ],
                          "test-simple-filed": "test-value-0", 
                          "test-list-filed": ["test-value-1"],
                          "test-map-list-filed": [{"test-field": "test-value-2"}],
                          "test-map-filed": {"test-field": "test-value-3"}
                        }
                    """.trimIndent()

    @AfterEach
    fun afterEach() {
        verifyNoMoreInteractions(context)
    }

    interface CodecTest {
        @Test
        fun decode()

        @ParameterizedTest
        @ValueSource(strings = ["[]"])
        fun `skip incorrect type of content`(content: String)

        @ParameterizedTest
        @ValueSource(strings = ["\"\"", "[\"\"]"])
        fun `error incorrect type of content`(content: String)

        @ParameterizedTest
        @ValueSource(strings = ["\"\""])
        fun `skip incorrect type of result`(result: String)

        @ParameterizedTest
        @ValueSource(strings = ["[]", "{}"])
        fun `error incorrect type of result`(result: String)
    }

    @Nested
    inner class ProtobufTest: CodecTest {
        @Test
        override fun decode() {
            val messageId = MessageID.newBuilder().generate(2, 2).build()
            val message = ProtobufRawMessage.newBuilder().apply {
                metadataBuilder.apply {
                    id = messageId
                    putAllProperties(PROPERTIES)
                }
                parentEventId = PROTOBUF_EVENT_ID
                body = ByteString.copyFrom(testContent, Charsets.UTF_8)
            }.build()

            val group = codec.decode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder().setRawMessage(message)
                }.build(),
                context
            )

            expectThat(group) {
                get { messagesList }.and {
                    hasSize(5)
                    elementAt(0).and {
                        get { hasRawMessage() }.isTrue()
                        get { getRawMessage() }.and {
                            verifyRawMetadata(messageId, PROPERTIES, 1)
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { body }.isEqualTo(ByteString.copyFrom("test-result".toByteArray()))
                        }
                    }
                    elementAt(1).and {
                        get { hasMessage() }.isTrue()
                        get { getMessage() }.and {
                            verifyMetadata(messageId, PROPERTIES, 2)
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { fieldsMap }.and {
                                hasSize(1)
                                get { get("test-simple-filed") }.isEqualTo("test-value-0".toValue())
                            }
                        }
                    }
                    elementAt(2).and {
                        get { hasMessage() }.isTrue()
                        get { getMessage() }.and {
                            verifyMetadata(messageId, PROPERTIES, 3)
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { fieldsMap }.and {
                                hasSize(1)
                                get { get("test-list-filed") }.isEqualTo(listOf("test-value-1").toListValue().toValue())
                            }
                        }
                    }
                    elementAt(3).and {
                        get { hasMessage() }.isTrue()
                        get { getMessage() }.and {
                            verifyMetadata(messageId, PROPERTIES, 4)
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { fieldsMap }.and {
                                hasSize(1)
                                get { get("test-map-list-filed") }.isEqualTo(
                                    listOf(mapOf("test-field" to "test-value-2")).toListValue().toValue()
                                )
                            }
                        }
                    }
                    elementAt(4).and {
                        get { hasMessage() }.isTrue()
                        get { getMessage() }.and {
                            verifyMetadata(messageId, PROPERTIES, 5)
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { fieldsMap }.and {
                                hasSize(1)
                                get { get("test-map-filed") }.isEqualTo(mapOf("test-field" to "test-value-3").toValue())
                            }
                        }
                    }
                }
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["[]"])
        override fun `skip incorrect type of content`(content: String) {
            val group = codec.decode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder().rawMessageBuilder.apply {
                        metadataBuilder.apply {
                            idBuilder.generate()
                        }
                        body = ByteString.copyFrom(generateContent(content), Charsets.UTF_8)
                    }
                }.build(),
                context
            )

            expectThat(group) {
                get { messagesList }.isEmpty()
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["\"\"", "[\"\"]"])
        override fun `error incorrect type of content`(content: String) {
            val messageId = MessageID.newBuilder().generate().build()
            val message = ProtobufRawMessage.newBuilder().apply {
                metadataBuilder.apply {
                    id = messageId
                    putAllProperties(PROPERTIES)
                }
                parentEventId = PROTOBUF_EVENT_ID
                body = ByteString.copyFrom(generateContent(content), Charsets.UTF_8)
            }.build()
            val group = codec.decode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder().setRawMessage(message)
                }.build(),
                context
            )

            expectThat(group) {
                get { messagesList }.and {
                    hasSize(1)
                    elementAt(0).and {
                        get { hasMessage() }.isTrue()
                        get { getMessage() }.and {
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { fieldsMap }.and {
                                hasSize(1)
                                get { get(ERROR_CONTENT_FIELD) }.isA<Value>().and {
                                    get { hasSimpleValue() }.isTrue()
                                    get { simpleValue }.isNotBlank()
                                }
                            }
                        }
                    }
                }
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["\"\""])
        override fun `skip incorrect type of result`(result: String) {
            val group = codec.decode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder().rawMessageBuilder.apply {
                        metadataBuilder.apply {
                            idBuilder.generate()
                        }
                        body = ByteString.copyFrom(generateContentWithResult(result), Charsets.UTF_8)
                    }
                }.build(),
                context
            )

            expectThat(group) {
                get { messagesList }.isEmpty()
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["[]", "{}"])
        override fun `error incorrect type of result`(result: String) {
            val messageId = MessageID.newBuilder().generate().build()
            val message = ProtobufRawMessage.newBuilder().apply {
                metadataBuilder.apply {
                    id = messageId
                    putAllProperties(PROPERTIES)
                }
                parentEventId = PROTOBUF_EVENT_ID
                body = ByteString.copyFrom(generateContentWithResult(result), Charsets.UTF_8)
            }.build()
            val group = codec.decode(
                ProtobufMessageGroup.newBuilder().apply {
                    addMessagesBuilder().setRawMessage(message)
                }.build(),
                context
            )

            expectThat(group) {
                get { messagesList }.and {
                    hasSize(1)
                    elementAt(0).and {
                        get { hasMessage() }.isTrue()
                        get { getMessage() }.and {
                            get { parentEventId }.isSameInstanceAs(message.parentEventId)
                            get { fieldsMap }.and {
                                hasSize(1)
                                get { get(ERROR_CONTENT_FIELD) }.isA<Value>().and {
                                    get { hasSimpleValue() }.isTrue()
                                    get { simpleValue }.isNotBlank()
                                }
                            }
                        }
                    }
                }
            }
        }

        private fun Assertion.Builder<ProtobufRawMessage>.verifyRawMetadata(
            messageId: MessageID,
            properties: Map<String, String>,
            subSeq: Int
        ) {
            get { metadata }.and {
                get { propertiesMap }.isEqualTo(properties)
                get { id }.and {
                    get { bookName }.isSameInstanceAs(messageId.bookName)
                    get { connectionId }.isSameInstanceAs(messageId.connectionId)
                    get { sequence }.isSameInstanceAs(messageId.sequence)
                    get { subsequenceList }.isEqualTo(listOf(subSeq))
                    get { timestamp }.isSameInstanceAs(messageId.timestamp)
                }
            }
        }

        private fun Assertion.Builder<Message>.verifyMetadata(
            messageId: MessageID,
            properties: Map<String, String>,
            subSeq: Int
        ) {
            get { metadata }.and {
                get { messageType }.isSameInstanceAs(DEFAULT_MESSAGE_TYPE)
                get { propertiesMap }.isEqualTo(properties)
                get { id }.and {
                    get { bookName }.isSameInstanceAs(messageId.bookName)
                    get { connectionId }.isSameInstanceAs(messageId.connectionId)
                    get { sequence }.isSameInstanceAs(messageId.sequence)
                    get { subsequenceList }.isEqualTo(listOf(subSeq))
                    get { timestamp }.isSameInstanceAs(messageId.timestamp)
                }
            }
        }

        private fun MessageID.Builder.generate(seq: Long = 1, subSeq: Int = 1): MessageID.Builder = apply {
            bookName = BOOK
            connectionIdBuilder.setSessionGroup(SESSION_GROUP)
                .setSessionAlias(SESSION_ALIAS)
            direction = Direction.SECOND
            sequence = seq
            addSubsequence(subSeq)
            timestamp = Instant.now().toTimestamp()
        }
    }

    @Nested
    inner class TransportTest: CodecTest {
        @Test
        override fun decode() {
            val messageId = MessageId.builder().generate(2, 2).build()
            val message = TransportRawMessage.builder()
                .setId(messageId)
                .setMetadata(PROPERTIES)
                .setEventId(TRANSPORT_EVENT_ID)
                .setBody(testContent.toByteArray())
                .build()

            val group = codec.decode(
                message.toGroup(),
                context
            )

            expectThat(group) {
                get { messages }.and {
                    hasSize(5)
                    elementAt(0).isA<TransportRawMessage>().and {
                        verifyRawMetadata(messageId, PROPERTIES, 1)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body.toString(Charsets.UTF_8) }.isEqualTo("test-result")
                    }
                    elementAt(1).isA<ParsedMessage>().and {
                        verifyMetadata(messageId, DEFAULT_MESSAGE_TYPE, PROPERTIES, 2)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body }.and {
                            hasSize(1)
                            get { get("test-simple-filed") }.isEqualTo("test-value-0")
                        }
                    }
                    elementAt(2).isA<ParsedMessage>().and {
                        verifyMetadata(messageId, DEFAULT_MESSAGE_TYPE, PROPERTIES, 3)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body }.and {
                            hasSize(1)
                            get { get("test-list-filed") }.isEqualTo(listOf("test-value-1"))
                        }
                    }
                    elementAt(3).isA<ParsedMessage>().and {
                        verifyMetadata(messageId, DEFAULT_MESSAGE_TYPE, PROPERTIES, 4)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body }.and {
                            hasSize(1)
                            get { get("test-map-list-filed") }.isEqualTo(listOf(mapOf("test-field" to "test-value-2")))
                        }
                    }
                    elementAt(4).isA<ParsedMessage>().and {
                        verifyMetadata(messageId, DEFAULT_MESSAGE_TYPE, PROPERTIES, 5)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body }.and {
                            hasSize(1)
                            get { get("test-map-filed") }.isEqualTo(mapOf("test-field" to "test-value-3"))
                        }
                    }
                }
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["[]"])
        override fun `skip incorrect type of content`(content: String) {
            val group = codec.decode(
                TransportRawMessage.builder().apply {
                    idBuilder().generate()
                    setEventId(TRANSPORT_EVENT_ID)
                    setBody(generateContent(content).toByteArray())
                }.build().toGroup(),
                context
            )

            expectThat(group) {
                get { messages }.isEmpty()
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["\"\"", "[\"\"]"])
        override fun `error incorrect type of content`(content: String) {
            val messageId = MessageId.builder().generate().build()
            val message = TransportRawMessage.builder()
                .setId(messageId)
                .setMetadata(PROPERTIES)
                .setEventId(TRANSPORT_EVENT_ID)
                .setBody(generateContent(content).toByteArray())
                .build()
            val group = codec.decode(
                message.toGroup(),
                context
            )

            expectThat(group) {
                get { messages }.and {
                    hasSize(1)
                    elementAt(0).isA<ParsedMessage>().and {
                        verifyMetadata(messageId, ERROR_TYPE_MESSAGE, PROPERTIES, 1)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body }.and {
                            hasSize(1)
                            get { get(ERROR_CONTENT_FIELD) }.isA<String>().isNotBlank()
                        }
                    }
                }
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["\"\""])
        override fun `skip incorrect type of result`(result: String) {
            val group = codec.decode(
                TransportRawMessage.builder().apply {
                    idBuilder().generate()
                    setEventId(TRANSPORT_EVENT_ID)
                    setBody(generateContentWithResult(result).toByteArray())
                }.build().toGroup(),
                context
            )

            expectThat(group) {
                get { messages }.isEmpty()
            }
        }

        @ParameterizedTest
        @ValueSource(strings = ["[]", "{}"])
        override fun `error incorrect type of result`(result: String) {
            val messageId = MessageId.builder().generate().build()
            val message = TransportRawMessage.builder()
                .setId(messageId)
                .setMetadata(PROPERTIES)
                .setEventId(TRANSPORT_EVENT_ID)
                .setBody(generateContentWithResult(result).toByteArray())
                .build()
            val group = codec.decode(
                message.toGroup(),
                context
            )

            expectThat(group) {
                get { messages }.and {
                    hasSize(1)
                    elementAt(0).isA<ParsedMessage>().and {
                        verifyMetadata(messageId, ERROR_TYPE_MESSAGE, PROPERTIES, 1)
                        get { eventId }.isSameInstanceAs(message.eventId)
                        get { body }.and {
                            hasSize(1)
                            get { get(ERROR_CONTENT_FIELD) }.isA<String>().isNotBlank()
                        }
                    }
                }
            }
        }

        private fun Assertion.Builder<TransportRawMessage>.verifyRawMetadata(
            messageId: MessageId,
            properties: Map<String, String>,
            subSeq: Int
        ) {
            get { metadata }.isEqualTo(properties)
            get { id }.and {
                get { sessionAlias }.isSameInstanceAs(messageId.sessionAlias)
                get { sequence }.isSameInstanceAs(messageId.sequence)
                get { subsequence }.isEqualTo(listOf(subSeq))
                get { timestamp }.isSameInstanceAs(messageId.timestamp)
            }
        }

        private fun Assertion.Builder<ParsedMessage>.verifyMetadata(
            messageId: MessageId,
            messageType: String,
            properties: Map<String, String>,
            subSeq: Int
        ) {
            get { type }.isSameInstanceAs(messageType)
            get { metadata }.isEqualTo(properties)
            get { id }.and {
                get { sessionAlias }.isSameInstanceAs(messageId.sessionAlias)
                get { sequence }.isSameInstanceAs(messageId.sequence)
                get { subsequence }.isEqualTo(listOf(subSeq))
                get { timestamp }.isSameInstanceAs(messageId.timestamp)
            }
        }

        private fun MessageId.Builder.generate(seq: Long = 1, subSeq: Int = 1): MessageId.Builder = this
            .setSessionAlias(SESSION_ALIAS)
            .setDirection(OUTGOING)
            .setSequence(seq)
            .addSubsequence(subSeq)
            .setTimestamp(Instant.now())
    }

    private fun generateContentWithResult(result: String) = """
                            {
                              "${config.contentKey}": [
                                {
                                  "${config.resultKey}": $result
                                }
                              ]
                            }
                        """.trimIndent()

    private fun generateContent(content: String) = """
                            {
                              "${config.contentKey}": $content
                            }
                        """.trimIndent()

    companion object {
        private const val BOOK = "test-book"
        private const val SCOPE = "test-scope"
        private const val SESSION_ALIAS = "test-session-alias"
        private const val SESSION_GROUP = "test-session-group"

        private val PROTOBUF_EVENT_ID = EventID.newBuilder()
            .setBookName(BOOK)
            .setScope(SCOPE)
            .setStartTimestamp(Instant.now().toTimestamp())
            .setId("test-id")
            .build()

        private val TRANSPORT_EVENT_ID = EventId.builder()
            .setBook(BOOK)
            .setScope(SCOPE)
            .setTimestamp(Instant.now())
            .setId("test-id")
            .build()

        private val PROPERTIES = mapOf("test-property" to "test-property-value")
    }
}