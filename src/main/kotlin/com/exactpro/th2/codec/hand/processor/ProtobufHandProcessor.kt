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
package com.exactpro.th2.codec.hand.processor

import com.exactpro.th2.codec.hand.CodecHandSettings
import com.exactpro.th2.codec.hand.processor.ProtobufHandProcessor.Companion.Context
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.grpc.RawMessageMetadata
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.ByteString

class ProtobufHandProcessor(
    settings: CodecHandSettings
): HandProcessor<RawMessage, Context, AnyMessage>(
    settings
) {
    override val RawMessage.bodyAsString: String
        get() = body.toStringUtf8()

    override fun RawMessage.createContext(): Context = Context(this)

    override fun createRaw(context: Context, subSequence: Int, rawData: String): AnyMessage =
        AnyMessage.newBuilder().apply {
            val rawMsgMetaData = context.rawMetaDataBuilder
                .setId(
                    context.messageIdBuilder.clearSubsequence()
                        .addSubsequence(subSequence)
                )
                .build()
            val builderForValue = RawMessage.newBuilder().apply {
                setMetadata(rawMsgMetaData)
                context.parentEventId?.let(::setParentEventId)
                setBody(ByteString.copyFrom(rawData.toByteArray()))
            }
            setRawMessage(builderForValue)
        }.build()

    override fun createParsed(context: Context, subSequence: Int, key: String, value: Any): AnyMessage =
        AnyMessage.newBuilder().apply {
            val msgMetaData = context.metaDataBuilder.setId(
                context.messageIdBuilder.clearSubsequence().addSubsequence(subSequence)
            ).setMessageType(DEFAULT_MESSAGE_TYPE).build()

            setMessage(
                Message.newBuilder().apply {
                    setMetadata(msgMetaData)
                    context.parentEventId?.let(::setParentEventId)
                    putFields(key, convertToValue(value))
                }
            )
        }.build()

    private fun convertToValue(value: Any): Value = value.toValue()

    companion object {
        class Context(message: RawMessage) {
            val messageIdBuilder: MessageID.Builder = getMessageIdBuilder(message)
            val rawMetaDataBuilder: RawMessageMetadata.Builder = getRawMetaDataBuilder(message)
            val metaDataBuilder: MessageMetadata.Builder = getMetaDataBuilder(message)
            val parentEventId: EventID? = if (message.hasParentEventId()) message.parentEventId else null

            private fun getMessageIdBuilder(rawMessage: RawMessage): MessageID.Builder {
                return rawMessage.metadata.id.toBuilder()
            }

            private fun getMetaDataBuilder(rawMessage: RawMessage): MessageMetadata.Builder {
                val metadata = rawMessage.metadata
                return MessageMetadata.newBuilder()
                    .setId(metadata.id.toBuilder().setTimestamp(metadata.id.timestamp).build())
                    .putAllProperties(metadata.propertiesMap).setProtocol(metadata.protocol)
            }

            private fun getRawMetaDataBuilder(rawMessage: RawMessage): RawMessageMetadata.Builder {
                return rawMessage.metadata.toBuilder()
            }
        }
    }
}


