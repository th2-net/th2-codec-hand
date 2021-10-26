/*
 Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.codec.hand;

import com.exactpro.th2.codec.hand.decoder.HandDecoder;
import com.exactpro.th2.codec.hand.listener.MessageGroupBatchListener;
import com.exactpro.th2.codec.hand.processor.HandProcessor;
import com.exactpro.th2.codec.hand.processor.MessageType;
import com.exactpro.th2.codec.hand.util.RawMessageConverter;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.google.protobuf.AbstractMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.exactpro.th2.common.metrics.CommonMetrics.setLiveness;
import static com.exactpro.th2.common.metrics.CommonMetrics.setReadiness;

@Slf4j
public class Controller {

    private static volatile List<AutoCloseable> resources;

    public static void main(String[] args) {

        setLiveness(true);
        resources = new ArrayList<>();

        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        try {
            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);

            var parsedBatchRouter = factory.getMessageRouterMessageGroupBatch();
            resources.add(parsedBatchRouter);

            var rawBatchRouter = factory.getMessageRouterMessageGroupBatch();
            resources.add(rawBatchRouter);

            configureShutdownHook(lock, condition);
            setReadiness(true);

            HandDecoder handDecoder = createDecoder();
            MessageGroupBatchListener messageGroupBatchListener = new MessageGroupBatchListener(parsedBatchRouter, handDecoder);
            rawBatchRouter.subscribeAll(messageGroupBatchListener);

            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            log.info("Wait shutdown");
            condition.await();
            log.info("App shutdown");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook (ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread("Shutdown hook") {
            @Override
            public void run() {
                log.info("Shutdown start");
                setReadiness(false);

                try {
                    lock.lock();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }

                for (var resource : resources) {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        log.error("Exception closing resource {}", resource.getClass().getSimpleName(), e);
                    }
                }

                log.info("Shutdown end");
                setLiveness(false);
            }
        });
    }

    private static HandDecoder createDecoder() {
        return new HandDecoder(new RawMessageConverter(), getHandProcessors());
    }

    @SuppressWarnings("unchecked")
    private static Map<MessageType, HandProcessor<AbstractMessage>> getHandProcessors() {
        Map<MessageType, HandProcessor<AbstractMessage>> processors = new EnumMap<>(MessageType.class);
        for (var handProcessor : ServiceLoader.load(HandProcessor.class)) {
            processors.put(handProcessor.getMessageType(), handProcessor);
        }
        return processors;
    }
}
