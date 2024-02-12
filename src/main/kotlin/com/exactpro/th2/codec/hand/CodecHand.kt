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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.hand.processor.ProtobufHandProcessor
import com.exactpro.th2.codec.hand.processor.TransportHandProcessor
import com.exactpro.th2.common.grpc.MessageGroup as ProtobufMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup as TransportMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage as TransportRawMessage

class CodecHand(settings: CodecHandSettings) : IPipelineCodec {
    private val protobufProcessor = ProtobufHandProcessor(settings)
    private val transportProcessor = TransportHandProcessor(settings)

    override fun decode(messageGroup: TransportMessageGroup, context: IReportingContext): TransportMessageGroup =
        TransportMessageGroup.builder().apply {
            messageGroup.messages.forEach { message ->
                if(message !is TransportRawMessage) {
                    addMessage(message)
                    return@forEach
                }
                transportProcessor.process(message, ::addMessage)
            }
        }.build()

    override fun decode(messageGroup: ProtobufMessageGroup, context: IReportingContext): ProtobufMessageGroup =
        ProtobufMessageGroup.newBuilder().apply {
            messageGroup.messagesList.forEach { anyMessage ->
                if (!anyMessage.hasRawMessage()) {
                    addMessages(anyMessage)
                    return@forEach
                }
                protobufProcessor.process(anyMessage.rawMessage, ::addMessages)
            }
        }.build()
}
