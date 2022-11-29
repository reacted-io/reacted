/*
 * Copyright (c) 2020 , <Pierre Falda> [ pierre@reacted.io ]
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

package io.reacted.core.reactors.systemreactors;

import com.sun.management.OperatingSystemMXBean;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.messages.reactors.SystemMonitorReport;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.patterns.ObjectUtils;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@NonNullByDefault
public class SystemMonitor implements ReActiveEntity {
    private final OperatingSystemMXBean systemDataSource;
    private final Duration taskPeriod;
    private final ScheduledExecutorService timerService;
    @Nullable
    private ScheduledFuture<?> timer;

    public SystemMonitor(Duration taskPeriod, ScheduledExecutorService timerService) {
        this.timerService = Objects.requireNonNull(timerService);
        this.systemDataSource = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        this.taskPeriod = ObjectUtils.checkNonNullPositiveTimeInterval(taskPeriod);
    }

    @Nonnull
    @Override
    public ReActions getReActions() {
        return ReActions.newBuilder()
                        .reAct(ReActorInit.class, (raCtx, init) -> onInit(raCtx))
                        .reAct(ReActorStop.class, (raCtx, stop) -> onStop())
                        .reAct(ReActions::noReAction)
                        .build();
    }

    private void onInit(ReActorContext raCtx) {
        this.timer = Try.of(() -> timerService.scheduleAtFixedRate(() -> broadcastStatistics(raCtx),
                                                                   0, taskPeriod.toMillis(),
                                                                   TimeUnit.MILLISECONDS))
                        .orElse(null, error -> initRetry(error, raCtx));
    }

    private void onStop() {
        if (timer != null) {
            timer.cancel(true);
        }
    }

    private void broadcastStatistics(ReActorContext raCtx) {
        Try.ofRunnable(() -> raCtx.getReActorSystem()
                                  .broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF,
                                                               getSystemStatistics(
                                                                   systemDataSource)))
           .ifError(error -> raCtx.logError("Unable to broadcast statistics update", error));
    }

    /*
     * @throws IllegalArgumentException if any meaningless value is returned by the OS interface
     */
    private static SystemMonitorReport getSystemStatistics(OperatingSystemMXBean systemInterface) {
        return SystemMonitorReport.newBuilder()
                                  .setCpuLoad(systemInterface.getSystemLoadAverage())
                                  .setFreeMemorySize(systemInterface.getFreeMemorySize())
                                  .build();
    }

    private static void initRetry(Throwable error, ReActorContext raCtx) {
        raCtx.logError("Unable to init {} reattempting",
                       SystemMonitor.class.getSimpleName(), error);
        raCtx.selfPublish(new ReActorInit());
    }
}
