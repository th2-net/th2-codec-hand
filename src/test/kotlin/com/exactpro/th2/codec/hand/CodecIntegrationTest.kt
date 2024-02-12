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

import com.exactpro.th2.codec.Application
import com.exactpro.th2.codec.configuration.Configuration
import com.exactpro.th2.codec.configuration.TransportLine
import com.exactpro.th2.codec.configuration.TransportType
import com.exactpro.th2.codec.grpc.GrpcCodecService
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.test.annotations.Th2AppFactory
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.extension.CleanupExtension
import com.exactpro.th2.test.spec.CradleSpec
import com.exactpro.th2.test.spec.CustomConfigSpec
import com.exactpro.th2.test.spec.GrpcSpec
import com.exactpro.th2.test.spec.RabbitMqSpec
import com.exactpro.th2.test.spec.client
import com.exactpro.th2.test.spec.pin
import com.exactpro.th2.test.spec.pins
import com.exactpro.th2.test.spec.publishers
import com.exactpro.th2.test.spec.subscribers
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
@Th2IntegrationTest
class CodecIntegrationTest {
    @JvmField
    internal val customConfig = CustomConfigSpec.fromObject(
        Configuration().apply {
            codecSettings = CodecHandSettings()
            transportLines = mapOf(
                "protobuf" to TransportLine(
                    type = TransportType.PROTOBUF,
                    useParentEventId = true,
                ),
                "transport" to TransportLine(
                    type = TransportType.TH2_TRANSPORT,
                    useParentEventId = true,
                ),
            )
        },
    )

    @JvmField
    internal val queue = RabbitMqSpec.create()
        .pins {
            subscribers {
                pin("transport_in_decode") {
                    attributes("transport_decoder_in", "transport-group")
                }
                pin("transport_in_encode") {
                    attributes("transport_encoder_in", "transport-group")
                }
                pin("protobuf_in_decode") {
                    attributes("protobuf_decoder_in", "group")
                }
                pin("protobuf_in_encode") {
                    attributes("protobuf_encoder_in", "group")
                }
            }
            publishers {
                pin("transport_out_decode") {
                    attributes("transport_decoder_out", "transport-group")
                }
                pin("transport_out_encode") {
                    attributes("transport_encoder_out", "transport-group")
                }
                pin("protobuf_out_decode") {
                    attributes("protobuf_decoder_out", "group")
                }
                pin("protobuf_out_encode") {
                    attributes("protobuf_encoder_out", "group")
                }
            }
        }

    @JvmField
    internal val cradle = CradleSpec.create()
        .reuseKeyspace()
        .disableAutoPages()

    @JvmField
    internal val grpc = GrpcSpec.create()
        .client<GrpcCodecService>()

    @Test
    fun `app starts`(
        @Th2AppFactory factory: CommonFactory,
        resource: CleanupExtension.Registry,
    ) {
        val application = Application(factory)
        resource.add("app", application)
    }
}
