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
import io.reacted.patterns.Try;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Test
public class PublisherTCKTest extends FlowPublisherVerification<Long> {
    private final AtomicLong counter = new AtomicLong(0);
    private final ReActorSystem localReActorSystem;
    private final BlockingQueue<ReactedSubmissionPublisher<Long>> generatedFlows = new LinkedBlockingDeque<>();
    private volatile ExecutorService submitterThread;

    public PublisherTCKTest() {
        super(new TestEnvironment(250, 250, false));
        var rasCfg = ReActorSystemConfig.newBuilder()
                .setLocalDriver(SystemLocalDrivers.DIRECT_COMMUNICATION)
                .setRecordExecution(false)
                .setMsgFanOutPoolSize(1)
                .setReactorSystemName("TckValidationRAS")
                .setAskTimeoutsCleanupInterval(Duration.ofSeconds(10))
                .build();
        this.localReActorSystem = new ReActorSystem(rasCfg);
    }

    @Override
    public Flow.Publisher<Long> createFlowPublisher(long l) {
        var publisher = new ReactedSubmissionPublisher<Long>(this.localReActorSystem,
                                                             "TckFeed-" + counter.getAndIncrement());
        this.generatedFlows.add(publisher);
        asyncPublishMessages(publisher, l);
        return publisher;
    }

    @Override
    public Flow.Publisher<Long> createFailedFlowPublisher() {
        var publisher = new ReactedSubmissionPublisher<Long>(this.localReActorSystem,
                                                             "FailedTckFeed-" + counter.getAndIncrement());
        publisher.close();
        Try.ofRunnable(() -> TimeUnit.MILLISECONDS.sleep(20))
           .ifError(error -> Thread.currentThread().interrupt());
        this.generatedFlows.add(publisher);
        return publisher;
    }

    @BeforeMethod
    public void setup() {
        this.submitterThread = Executors.newSingleThreadExecutor();
    }

    @AfterMethod
    private void cleanupTest() throws InterruptedException {
        if (this.submitterThread != null) {
            this.submitterThread.shutdownNow();
        }
        while(!generatedFlows.isEmpty()) {
            var publisher = generatedFlows.poll();
            publisher.close();
        }
    }

    @BeforeSuite
    public void init() {
        this.localReActorSystem.initReActorSystem();
        this.submitterThread = Executors.newSingleThreadExecutor();
    }

    @AfterSuite
    public void cleanup() {
        this.submitterThread.shutdownNow();
        this.localReActorSystem.shutDown();
    }

    private void asyncPublishMessages(ReactedSubmissionPublisher<Long> publisher,
                                      long messagesNum) {
        this.submitterThread.submit(() -> {
            Try.ofRunnable(() -> TimeUnit.MILLISECONDS.sleep(150));
            for(long cycle = 0; cycle < messagesNum && !Thread.currentThread().isInterrupted(); cycle++) {
                publisher.submit(cycle);
            }
            publisher.close();
        });
    }
}
