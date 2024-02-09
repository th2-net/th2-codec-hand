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
import com.exactpro.th2.codec.hand.processor.TransportHandProcessor.Companion.Context
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage

class TransportHandProcessor(
    settings: CodecHandSettings
): HandProcessor<RawMessage, Context, Message<*>>(
    settings
) {
    override val RawMessage.bodyAsString: String
        get() = body.toString(Charsets.UTF_8)

    override fun RawMessage.createContext(): Context = Context(this)

    override fun createRaw(context: Context, subSequence: Int, rawData: String): RawMessage =
        RawMessage.builder().apply {
            fill(context, subSequence)
            setBody(rawData.toByteArray())
        }.build()

    override fun createParsed(context: Context, subSequence: Int, key: String, value: Any): ParsedMessage =
        createParsed(context, DEFAULT_MESSAGE_TYPE, subSequence, key, value)

    override fun createError(context: Context, subSequence: Int, text: String): ParsedMessage =
        createParsed(context, ERROR_TYPE_MESSAGE, subSequence, ERROR_CONTENT_FIELD, text)

    private fun createParsed(context: Context, type: String, subSequence: Int, key: String, value: Any): ParsedMessage =
        ParsedMessage.builder().apply {
            fill(context, subSequence)
            setType(type)
            addField(key, value)
        }.build()

    private fun <T: Message.Builder<T>> Message.Builder<T>.fill(context: Context, subSequence: Int) {
        setId(context.id.copy(subsequence = listOf(subSequence)))
        setMetadata(context.metadata)
        context.eventId?.let(::setEventId)
        setProtocol(context.protocol)
    }

    companion object {
        class Context(message: RawMessage) {
            val id: MessageId = message.id
            val metadata: Map<String, String> = message.metadata
            val protocol: String = message.protocol
            val eventId: EventId? = message.eventId
        }
    }
}


