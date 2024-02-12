/*
 * Copyright 2020-2024 Exactpro (Exactpro Systems Limited)
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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import mu.KotlinLogging

/*
    Following class does RawMessage processing
    and Message generation
 */
abstract class HandProcessor<M, C, R>(
    private val settings: CodecHandSettings
) {
    @Throws(Exception::class)
    fun process(rawMessage: M, onMessage: (R) -> Unit) {
        var subSequence = 1
        val jsonMap: Map<String, Any> = MAPPER.readValue(rawMessage.bodyAsString)

        val context = rawMessage.createContext()

        for ((key, value) in jsonMap) {
            if (key == settings.contentKey) {
                if (value !is List<*>) {
                    val text = "Expected value for '$key' is list but received: '${value.javaClass}'"
                    LOGGER.error(text)
                    onMessage(createError(context, subSequence, text))
                    continue
                }

                if (value.isEmpty()) continue

                for (iterableValue in value) {
                    if (iterableValue !is Map<*, *>) {
                        val text = "Expected type of $iterableValue is map but received: ${iterableValue?.javaClass}"
                        LOGGER.error(text)
                        onMessage(createError(context, subSequence, text))
                        continue
                    }

                    val rawData: Any? = iterableValue[settings.resultKey]
                    if (rawData !is String) {
                        val text = "Expected type of $rawData is string but received: ${rawData?.javaClass}"
                        LOGGER.error(text)
                        onMessage(createError(context, subSequence, text))
                        continue
                    }
                    if (rawData.isEmpty()) continue

                    onMessage(createRaw(context, subSequence, rawData))
                    subSequence += 1
                }
            } else {
                onMessage(createParsed(context, subSequence, key, value))
                subSequence += 1
            }
        }
    }

    protected abstract val M.bodyAsString: String

    protected abstract fun M.createContext(): C

    abstract fun createRaw(context: C, subSequence: Int, rawData: String): R

    abstract fun createParsed(context: C, subSequence: Int, key: String, value: Any): R

    abstract fun createError(context: C, subSequence: Int, text: String): R

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        internal const val DEFAULT_MESSAGE_TYPE: String = "th2-hand"

        internal val MAPPER = ObjectMapper()
    }
}


