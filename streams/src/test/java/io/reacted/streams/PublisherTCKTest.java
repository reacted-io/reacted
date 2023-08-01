/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.streams;

import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.Try;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Test
public class PublisherTCKTest extends FlowPublisherVerification<ReActedMessage.LongMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublisherTCKTest.class);
    private final AtomicLong counter = new AtomicLong(0);
    private final ReActorSystem localReActorSystem;
    private final BlockingQueue<ReactedSubmissionPublisher<ReActedMessage.LongMessage>> generatedFlows = new LinkedBlockingDeque<>();
    private volatile ExecutorService submitterThread;

    public PublisherTCKTest() {
        super(new TestEnvironment(250, 250, false));
        var rasCfg = ReActorSystemConfig.newBuilder()
                .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                .setRecordExecution(false)
                .setMsgFanOutPoolSize(1)
                .setReactorSystemName("TckValidationRAS")
                .build();
        this.localReActorSystem = new ReActorSystem(rasCfg);
    }

    @Override
    public Flow.Publisher<ReActedMessage.LongMessage> createFlowPublisher(long l) {
        var publisher = new ReactedSubmissionPublisher<ReActedMessage.LongMessage>(localReActorSystem, 10_000,
                                                             "TckFeed-" + counter.getAndIncrement());
        generatedFlows.add(publisher);
        asyncPublishMessages(publisher, l);
        return publisher;
    }

    @Override
    public Flow.Publisher<ReActedMessage.LongMessage> createFailedFlowPublisher() {
        var publisher = new ReactedSubmissionPublisher<ReActedMessage.LongMessage>(localReActorSystem, 10_000,
                                                             "FailedTckFeed-" + counter.getAndIncrement());
        publisher.close();
        Try.ofRunnable(() -> TimeUnit.MILLISECONDS.sleep(20))
           .ifError(error -> Thread.currentThread().interrupt());
        generatedFlows.add(publisher);
        return publisher;
    }

    @BeforeMethod
    public void setup() {
        this.submitterThread = Executors.newSingleThreadExecutor();
    }

    @AfterMethod
    private void cleanupTest() {
        if (submitterThread != null) {
            submitterThread.shutdownNow();
        }
        while(!generatedFlows.isEmpty()) {
            var publisher = generatedFlows.poll();
            publisher.close();
        }
    }

    @BeforeSuite
    public void init() {
        localReActorSystem.initReActorSystem();
        this.submitterThread = Executors.newSingleThreadExecutor();
    }

    @AfterSuite
    public void cleanup() {
        submitterThread.shutdownNow();
        localReActorSystem.shutDown();
    }

    private void asyncPublishMessages(ReactedSubmissionPublisher<ReActedMessage.LongMessage> publisher,
                                      long messagesNum) {
        submitterThread.submit(() -> {
            //The subscription handshake needs some time to complete
            Try.ofRunnable(() -> TimeUnit.MILLISECONDS.sleep(50));
            for(long cycle = 0; cycle < messagesNum && !Thread.currentThread().isInterrupted(); cycle++) {
                ReActedMessage.LongMessage payload = new ReActedMessage.LongMessage();
                payload.setPayload(cycle);
                if (publisher.submit(payload).isNotSent()) {
                    LOGGER.error("Critic! Message {} not sent!", cycle);
                }
            }
            Try.ofRunnable(() -> TimeUnit.MILLISECONDS.sleep(50));
            publisher.close();
        });
    }
}
