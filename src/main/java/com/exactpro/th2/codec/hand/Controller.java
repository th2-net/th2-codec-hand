/*
 Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.hand.processor.HandProcessorConfiguration;
import com.exactpro.th2.codec.hand.processor.HandProcessor;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.exactpro.th2.common.metrics.CommonMetrics.setLiveness;
import static com.exactpro.th2.common.metrics.CommonMetrics.setReadiness;

public class Controller {
    private static final Logger log = LoggerFactory.getLogger(Controller.class);
    private static volatile List<AutoCloseable> resources;

    public static void main(String[] args) {

        setLiveness(true);
        resources = new ArrayList<>();

        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();
        try {
            CommonFactory factory = CommonFactory.createFromArguments();
            resources.add(factory);

            var parsedBatchRouter = factory.getMessageRouterMessageGroupBatch();
            resources.add(parsedBatchRouter);

            var rawBatchRouter = factory.getMessageRouterMessageGroupBatch();
            resources.add(rawBatchRouter);

            configureShutdownHook(lock, condition);
            setReadiness(true);

            HandProcessorConfiguration handProcessorConfiguration = factory.getCustomConfiguration(HandProcessorConfiguration.class);
            HandProcessor handProcessor = new HandProcessor(handProcessorConfiguration);

            HandDecoder handDecoder = new HandDecoder(handProcessor);
            MessageGroupBatchListener messageGroupBatchListener = new MessageGroupBatchListener(parsedBatchRouter, handDecoder);
            rawBatchRouter.subscribeAll(messageGroupBatchListener);

            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
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
}
