import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.flow.ReActedGraph;
import io.reacted.flow.operators.map.MapOperatorConfig;
import io.reacted.flow.operators.reduce.ReduceOperatorConfig;
import io.reacted.patterns.UnChecked;
import org.apache.commons.lang3.function.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class StatisticsCollector {
    static final String LATENCIES_COLLECTOR_OPERATOR = "LatenciesCollector";
    static final String REQUESTS_PER_INTERVAL_COLLECTOR_OPERATOR = "RPICollector";
    static final String PRINTER_OPERATOR = "Printer";
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCollector.class);
    private StatisticsCollector() { throw new AssertionError("Never supposed to be called"); }

    static Future<?> initAsyncMessageProducer(Runnable messageGenerator) {
        return initAsyncMessageProducer(messageGenerator, ForkJoinPool.commonPool());
    }

    static Future<?> initAsyncMessageProducer(Runnable messageGenerator, ExecutorService executor) {
        return executor.submit(messageGenerator);
    }
    static Runnable backpressureAwareMessageSender(Instant startTime, long messageNum,
                                                   ReActorRef destination) {
        return backpressureAwareMessageSender(startTime, messageNum, destination, System::nanoTime);
    }
    static Runnable backpressureAwareMessageSender(Instant startTime, long messageNum,
                                                   ReActorRef destination,
                                                   Supplier<? extends Serializable> payloadProducer) {
        Runnable sender = UnChecked.runnable(() -> {
            long baseNanosDelay = 1_000_000;
            long delay = baseNanosDelay;
            for (int msg = 0; msg < messageNum; msg++) {
                DeliveryStatus status = destination.tell(payloadProducer.get());
                if (status.isNotDelivered()) {
                    LOGGER.error("CRITIC! FAILED DELIVERY!?");
                    System.exit(3);
                }
                if (status.isBackpressureRequired()) {
                    TimeUnit.NANOSECONDS.sleep(delay);
                    delay = delay + (delay / 3);
                } else {
                    delay = Math.max((delay / 3) << 1, baseNanosDelay);
                }
            }
            LOGGER.info("Sent in {}", ChronoUnit.SECONDS.between(startTime, Instant.now()));
        });
        return sender;
    }
    static void requestsLatenciesFromWorkers(ReActorSystem reActorSystem) {
        reActorSystem.broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF, new StopCrunching());
    }
    static Map<String, ReActorRef> initStatisticsCollectorProcessor(ReActorSystem reActorSystem,
                                                                    long minDataPointsForProcessing) {
        var statisticsRequest = new DiagnosticRequest();
        reActorSystem.getSystemSchedulingService()
                     .scheduleAtFixedRate(() -> reActorSystem.broadcastToLocalSubscribers(ReActorRef.NO_REACTOR_REF,
                                                                                          statisticsRequest),
                                          1, 1, TimeUnit.SECONDS);
        return initStatisticsCollectorProcessor(createStatisticsCollectorProcessor(Dispatcher.DEFAULT_DISPATCHER_NAME,
                                                                                   minDataPointsForProcessing),
                                                reActorSystem);
    }

    static Map<String, ReActorRef> initStatisticsCollectorProcessor(ReActorSystem reActorSystem,
                                                                    String dispatcherName,
                                                                    long minDataPointsForProcessing) {
        return initStatisticsCollectorProcessor(createStatisticsCollectorProcessor(dispatcherName,
                                                                                   minDataPointsForProcessing),
                                                reActorSystem);
    }

    static Map<String, ReActorRef> initStatisticsCollectorProcessor(ReActedGraph statisticsCollectorGraph,
                                                                    ReActorSystem reActorSystem) {
        return statisticsCollectorGraph.run(reActorSystem).toCompletableFuture().join().orElseSneakyThrow();
    }

    static ReActedGraph
    createStatisticsCollectorProcessor(String dispatcherName, long minDataPointsForProcessing) {
        return ReActedGraph.newBuilder()
                           .setReActorName("Statistics Collector")
                           .setDispatcherName(dispatcherName)
                           .addOperator(ReduceOperatorConfig.newBuilder()
                                                            .setReActorName(LATENCIES_COLLECTOR_OPERATOR)
                                                            .setReductionRules(Map.of(LatenciesSnapshot.class,
                                                                                      minDataPointsForProcessing))
                                                            .setReducer(StatisticsCollector::fromLatenciesSnapshotsToPrintableOutput)
                                                            .setOutputOperators(PRINTER_OPERATOR)
                                                            .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(StatisticsCollector.LatenciesSnapshot.class))
                                                            .build())
                           .addOperator(ReduceOperatorConfig.newBuilder()
                                                            .setReActorName(REQUESTS_PER_INTERVAL_COLLECTOR_OPERATOR)
                                                            .setReductionRules(Map.of(StatisticsCollector.RPISnapshot.class,
                                                                                      minDataPointsForProcessing))
                                                            .setReducer(StatisticsCollector::fromRequestsPerIntervalSnapshotsToPrintableOutput)
                                                            .setTypedSubscriptions(TypedSubscription.TypedSubscriptionPolicy.LOCAL.forType(StatisticsCollector.RPISnapshot.class))
                                                            .setOutputOperators(PRINTER_OPERATOR)
                                                            .build())
                    .addOperator(MapOperatorConfig.newBuilder()
                                                  .setReActorName(PRINTER_OPERATOR)
                                                  .setConsumer(logEntry -> LOGGER.info((String)logEntry))
                                                  .build())
                    .build();
    }

    static List<String>
    fromRequestsPerIntervalSnapshotsToPrintableOutput(Map<Class<? extends Serializable>, List<? extends Serializable>> payloadByType) {
        List<RPISnapshot> requestsPerInterval = (List<RPISnapshot>)payloadByType.get(RPISnapshot.class);
        long totalRequests = requestsPerInterval.stream()
                                                .mapToLong(RPISnapshot::requestsPerInterval)
                                                .sum();
        StringBuilder output = new StringBuilder().append("Processed: ");
        for (RPISnapshot rpi : requestsPerInterval) {
            output.append(String.format("%d ", rpi.requestsPerInterval()));
        }
        output.append(String.format("-> %d%n", totalRequests));
        return List.of(output.toString());
    }
    static List<String>
    fromLatenciesSnapshotsToPrintableOutput(Map<Class<? extends Serializable>, List<? extends Serializable>> payloadByType) {
        List<LatenciesSnapshot> snapshots = (List<LatenciesSnapshot>)payloadByType.get(LatenciesSnapshot.class);
        long[] latencies = snapshots.stream()
                              .map(LatenciesSnapshot::latencies)
                              .flatMapToLong(Arrays::stream)
                              .toArray();
        return computeLatenciesOutput(latencies);
    }
    static List<String> computeLatenciesOutput(long[] latencies) {
        return computeLatencies(latencies).stream()
                .map(percentile -> String.format("Msgs: %d Percentile %f Latency: %s",
                                                 latencies.length, percentile.percentile,
                                                 percentile.latency))
                .collect(Collectors.toUnmodifiableList());
    }

    static List<LatencyForPercentile> computeLatencies(long[] latencies) {
        Arrays.sort(latencies);
        return List.of(70d, 75d, 80d, 85d, 90d, 95d, 99d, 99.9d, 99.99d, 99.9999d, 100d)
                   .stream()
                   .map(percentile -> new LatencyForPercentile(percentile,
                                                               getLatencyForPercentile(latencies, percentile)))
                   .collect(Collectors.toUnmodifiableList());
    }

    static Duration getLatencyForPercentile(long[] sortedLatencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sortedLatencies.length) - 1;
        return Duration.ofNanos(sortedLatencies[index]);
    }

    record LatencyForPercentile(double percentile, Duration latency) implements Serializable { }

    record DiagnosticRequest() implements Serializable { }

    record RPISnapshot(int requestsPerInterval) implements Serializable { }

    record LatenciesSnapshot(long[] latencies) implements Serializable { }

    record StopCrunching() implements Serializable { }
}
