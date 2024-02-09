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
package com.exactpro.th2.codec.hand.decoder

import com.exactpro.th2.codec.hand.processor.HandProcessor
import com.exactpro.th2.codec.hand.processor.HandProcessorConfiguration
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.value.toListValue
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.ByteString
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.elementAt
import strikt.assertions.hasSize
import strikt.assertions.isEmpty
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isSameInstanceAs
import strikt.assertions.isTrue
import java.time.Instant

internal class HandDecoderTest {

    private val configuration = HandProcessorConfiguration()
    private val decoder = HandDecoder(HandProcessor(configuration))

    @Test
    fun decode() {
        val messageId = MessageID.newBuilder().generate(2, 2).build()
        val msgWithContent = RawMessage.newBuilder().apply {
            metadataBuilder.apply {
                id = messageId
            }
            parentEventId = EVENT_ID
            body = ByteString.copyFrom(
                """
                        {
                          "${configuration.contentKey}": [
                            {
                              "${configuration.resultKey}": "test-result"
                            }
                          ],
                          "test-simple-filed": "test-value-0", 
                          "test-list-filed": ["test-value-1"],
                          "test-map-list-filed": [{"test-field": "test-value-2"}],
                          "test-map-filed": {"test-field": "test-value-3"}
                        }
                    """.trimIndent(), Charsets.UTF_8
            )
        }.build()

        val group = decoder.decode(
            MessageGroup.newBuilder().apply {
                addMessagesBuilder().setRawMessage(msgWithContent)
            }.build()
        )

        expectThat(group) {
            get { messagesList }.and {
                hasSize(5)
                elementAt(0).and {
                    get { hasRawMessage() }.isTrue()
                    get { getRawMessage() }.and {
                        verifyRawMetadata(messageId, 1)
                        // FIXME: bug
                        get { hasParentEventId() }.isFalse()
                        get { body }.isEqualTo(ByteString.copyFrom("test-result".toByteArray()))
                    }
                }
                elementAt(1).and {
                    get { hasMessage() }.isTrue()
                    get { getMessage() }.and {
                        verifyMetadata(messageId, 2)
                        // FIXME: bug
                        get { hasParentEventId() }.isFalse()
                        get { fieldsMap }.and {
                            hasSize(1)
                            get { get("test-simple-filed") }.isEqualTo("test-value-0".toValue())
                        }
                    }
                }
                elementAt(2).and {
                    get { hasMessage() }.isTrue()
                    get { getMessage() }.and {
                        verifyMetadata(messageId, 3)
                        // FIXME: bug
                        get { hasParentEventId() }.isFalse()
                        get { fieldsMap }.and {
                            hasSize(1)
                            get { get("test-list-filed") }.isEqualTo(listOf("test-value-1").toListValue().toValue())
                        }
                    }
                }
                elementAt(3).and {
                    get { hasMessage() }.isTrue()
                    get { getMessage() }.and {
                        verifyMetadata(messageId, 4)
                        // FIXME: bug
                        get { hasParentEventId() }.isFalse()
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
                        verifyMetadata(messageId, 5)
                        // FIXME: bug
                        get { hasParentEventId() }.isFalse()
                        get { fieldsMap }.and {
                            hasSize(1)
                            get { get("test-map-filed") }.isEqualTo(mapOf("test-field" to "test-value-3").toValue())
                        }
                    }
                }
            }
        }
    }

    private fun Assertion.Builder<RawMessage>.verifyRawMetadata(messageId: MessageID, subSeq: Int) {
        get { metadata }.and {
            get { id }.and {
                get { bookName }.isSameInstanceAs(messageId.bookName)
                get { connectionId }.isSameInstanceAs(messageId.connectionId)
                get { sequence }.isSameInstanceAs(messageId.sequence)
                get { subsequenceList }.isEqualTo(listOf(subSeq))
                get { timestamp }.isSameInstanceAs(messageId.timestamp)
            }
        }
    }

    private fun Assertion.Builder<Message>.verifyMetadata(messageId: MessageID, subSeq: Int) {
        get { metadata }.and {
            get { id }.and {
                get { bookName }.isSameInstanceAs(messageId.bookName)
                get { connectionId }.isSameInstanceAs(messageId.connectionId)
                get { sequence }.isSameInstanceAs(messageId.sequence)
                get { subsequenceList }.isEqualTo(listOf(subSeq))
                get { timestamp }.isSameInstanceAs(messageId.timestamp)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["\"\"", "[]", "[\"\"]"])
    fun `incorrect type of content`(content: String) {
        val group = decoder.decode(
            MessageGroup.newBuilder().apply {
                addMessagesBuilder().rawMessageBuilder.apply {
                    metadataBuilder.apply {
                        idBuilder.generate()
                    }
                    body = ByteString.copyFrom("""
                        {
                          "${configuration.contentKey}": $content
                        }
                    """.trimIndent(), Charsets.UTF_8)
                }
            }.build()
        )

        expectThat(group) {
            get { messagesList }.isEmpty()
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["\"\"", "[]", "{}"])
    fun `incorrect type of result`(result: String) {
        val group = decoder.decode(
            MessageGroup.newBuilder().apply {
                addMessagesBuilder().rawMessageBuilder.apply {
                    metadataBuilder.apply {
                        idBuilder.generate()
                    }
                    body = ByteString.copyFrom("""
                        {
                          "${configuration.contentKey}": [
                            {
                              "${configuration.resultKey}": $result
                            }
                          ]
                        }
                    """.trimIndent(), Charsets.UTF_8)
                }
            }.build()
        )

        expectThat(group) {
            get { messagesList }.isEmpty()
        }
    }

    companion object {
        private const val BOOK = "test-book"
        private const val SCOPE = "test-scope"
        private const val SESSION_ALIAS = "test-session-alias"
        private const val SESSION_GROUP = "test-session-group"

        private val EVENT_ID = EventID.newBuilder().setBookName(BOOK)
            .setScope(SCOPE)
            .setStartTimestamp(Instant.now().toTimestamp())
            .setId("test-id")
            .build()

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
}