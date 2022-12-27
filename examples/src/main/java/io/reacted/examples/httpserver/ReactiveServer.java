package io.reacted.examples.httpserver;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.exceptions.DeliveryException;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.serialization.ReActedMessage;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.streams.ReactedSubmissionPublisher;
import io.reacted.streams.ReactedSubmissionPublisher.ReActedSubscriptionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@NonNullByDefault
public class ReactiveServer {
    private static final Logger SERVER_LOGGER = LoggerFactory.getLogger(ReactiveServer.class);
    private static final String LOG_PATH = "/tmp/log";
    private static final String RESPONSE_DISPATCHER = "ResponseDispatcher";
    private static final String READER_DISPATCHER = "ReaderDispatcher";
    private static final int READ_CHUNK_SIZE = 65535;
    private static final int BATCH_BUFFER_SIZE = 100;

    public static void main(String[] args) throws IOException {
        var serverReactorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                       /* You can use chronicle driver and record execution property for replay */
                                                                       .setLocalDriver(
                                                                           SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver(LOG_PATH))
                                                                       .setReactorSystemName("ReactiveServer")
                                                                       .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                            .setDispatcherName(RESPONSE_DISPATCHER)
                                                                                                            .setDispatcherThreadsNum(1)
                                                                                                            .setBatchSize(100)
                                                                                                            .build())
                                                                       .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                               .setDispatcherName(READER_DISPATCHER)
                                                                               .setDispatcherThreadsNum(1)
                                                                               .setBatchSize(100)
                                                                               .build())
                                                                       .build()).initReActorSystem();
        ExecutorService serverPool = Executors.newSingleThreadExecutor();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);
        server.createContext("/read", new ReactiveHttpHandler(serverReactorSystem));
        server.setExecutor(serverPool);
        server.start();
    }

    private static class ReactiveHttpHandler implements HttpHandler {
        private final ReActorSystem reactiveServerSystem;
        private final AtomicLong requestCounter;
        private ReactiveHttpHandler(ReActorSystem reactiveServerSystem) {
            this.reactiveServerSystem = Objects.requireNonNull(reactiveServerSystem);
            this.requestCounter = new AtomicLong();
        }

        @Override
        public void handle(HttpExchange exchange) {
            handleResponse(exchange, "GET".equals(exchange.getRequestMethod())
                                     ? handleGetRequest(exchange)
                                     : List.of(), requestCounter.incrementAndGet());
        }

        private void handleResponse(HttpExchange exchange, List<String> filenames, long requestId) {
            if (!filenames.isEmpty()) {
                reactiveServerSystem.spawn(new ReactiveResponse(exchange, filenames, requestId),
                                           ReActorConfig.newBuilder()
                                                        .setReActorName("Request " + requestId)
                                                        .setDispatcherName(RESPONSE_DISPATCHER)
                                                        .build());
            }
        }

        private static List<String> handleGetRequest(HttpExchange httpExchange) {
            return Try.of(() -> httpExchange.getRequestURI()
                                            .toString()
                                            .split("\\?")[1].split("=")[1].split(","))
                      .map(Arrays::asList)
                      .orElse(List.of(), error -> SERVER_LOGGER.error("Invalid request ", error));
        }
    }

    private static class ReactiveResponse implements ReActiveEntity {
        private final HttpExchange httpCtx;
        private final List<String> filePaths;
        private final OutputStream outputStream;
        private final long requestId;
        private final AtomicInteger processed;

        public ReactiveResponse(HttpExchange httpCtx, List<String> filePaths, long reqId) {
            this.httpCtx = Objects.requireNonNull(httpCtx);
            this.filePaths = Objects.requireNonNull(filePaths).stream()
                                    .sorted()
                                    .collect(Collectors.toUnmodifiableList());
            this.outputStream = httpCtx.getResponseBody();
            this.requestId = reqId;
            this.processed = new AtomicInteger(filePaths.size());
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class, this::onInit)
                            .reAct(ReactedSubmissionPublisher.class, this::onDataPublisher)
                            .reAct(InternalError.class, (ctx, error) -> handleError(ctx, error.anyError))
                            .reAct(ReActorStop.class, (ctx, error) -> onStop(ctx))
                            .reAct(ReActions::noReAction)
                            .build();
        }

        private void onInit(ReActorContext ctx, ReActorInit init) {
            ctx.logInfo("Initializing {}", ctx.getSelf().getReActorId().getReActorName());
            Try.ofRunnable(() -> httpCtx.sendResponseHeaders(200, 0))
               .flatMap(noVal -> Try.ofRunnable(() -> sendData("<html><body>")))
               .flatMap(noVal -> spawnPathReaders(filePaths, ctx, requestId))
               .ifError(error -> handleError(ctx, error));
        }

        private void onDataPublisher(ReActorContext ctx, ReactedSubmissionPublisher<String> publisher) {
            publisher.subscribe(ReActedSubscriptionConfig.<String>newBuilder()
                                                         .setSubscriberName("sub_" + ctx.getSender().getReActorId().getReActorName())
                                                         .setBufferSize(ReactiveServer.BATCH_BUFFER_SIZE)
                                                         .build(), getNexDataConsumer(ctx))
                     .thenAccept(noVal -> ctx.reply(new PublishRequest(Duration.ofNanos(1))));
        }

        private Flow.Subscriber<String> getNexDataConsumer(ReActorContext ctx) {
            return new Flow.Subscriber<>() {
                @Nullable
                private Flow.Subscription subscription;
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    subscription.request(1);
                }

                @Override
                public void onNext(String item) {
                    try {
                        sendData(item);
                        Objects.requireNonNull(subscription).request(1);
                    } catch (Exception exc) {
                            onError(exc);
                    }
                }

                @Override
                public void onError(Throwable throwable) { handleError(ctx, throwable); }

                @Override
                public void onComplete() {
                    if (ReactiveResponse.this.processed.decrementAndGet() == 0) {
                        ctx.stop();
                    }
                }
            };
        }

        private void onStop(ReActorContext ctx) {
            logOnStop(ctx);
            Try.ofRunnable(() -> sendData("</body></html>"))
               .ifSuccess(noVal -> { outputStream.flush();
                                     outputStream.close(); });
        }

        private void handleError(ReActorContext ctx, Throwable anyError) {
            ctx.stop();
            ctx.logError("Error detected ", anyError);
        }

        private void sendData(String htmlResponse) throws IOException {
            outputStream.write(htmlResponse.getBytes(StandardCharsets.UTF_8));
        }

        private Try<ReActorRef> spawnPathReaders(List<String> filePaths,
                                                 ReActorContext ctx, long requestId) {
            return IntStream.range(0, filePaths.size())
                            .mapToObj(pathId -> ctx.spawnChild(new ReadFileWorker(ctx.getReActorSystem(),
                                                                                    filePaths.get(pathId),
                                                                                    requestId, pathId)))
                            .reduce(ReactiveResponse::detectAnyError)
                            .orElseGet(() -> Try.ofFailure(new IllegalStateException()));
        }

        private static <PayloadT> Try<PayloadT> detectAnyError(Try<PayloadT> first, Try<PayloadT> second) {
            return first.flatMap(payload -> second);
        }
    }

    private static class ReadFileWorker implements ReActor {
        private final ReActions readFileWorkerBehavior;
        private final ReActorConfig readFileWorkerCfg;
        private final ReactedSubmissionPublisher<String> dataPublisher;

        @Nullable
        private InputStreamReader fileLines;
        private ReadFileWorker(ReActorSystem reActorSystem, String filePath, long requestId, long workerId) {
            this.readFileWorkerBehavior = ReActions.newBuilder()
                                                   .reAct(ReActorInit.class,
                                                          ((ctx, init) -> onInit(ctx, filePath)))
                                                   .reAct(ReActorStop.class,
                                                          (ctx, stop) -> onStop(ctx))
                                                   .reAct(PublishRequest.class,
                                                          (ctx, pubStart) -> readFileLine(ctx, fileLines,
                                                                                            pubStart.backpressureDelay))
                                                   .reAct(ReActions::noReAction)
                                                   .build();
            this.readFileWorkerCfg = ReActorConfig.newBuilder()
                                                  .setReActorName(filePath + "|" + requestId + "|" + workerId)
                                                  .setDispatcherName(READER_DISPATCHER)
                                                  .build();
            this.dataPublisher = new ReactedSubmissionPublisher<>(reActorSystem, 10_000,
                                                                  String.format("publisher|%s|%s|%s", filePath, requestId, workerId));
        }

        @Nonnull
        @Override
        public ReActions getReActions() { return readFileWorkerBehavior; }

        @Nonnull
        @Override
        public ReActorConfig getConfig() { return readFileWorkerCfg; }

        private void onInit(ReActorContext ctx, String filePath) {
            this.fileLines = Try.of(() -> new InputStreamReader(new FileInputStream(filePath)))
                                .orElse(null, error -> ctx.getParent().publish(new InternalError(error)));

            if (fileLines == null) {
                dataPublisher.close();
                return;
            }
            if (ctx.getParent().publish(ctx.getSelf(), dataPublisher).isNotSent()) {
                ctx.getParent().publish(ctx.getSelf(), new InternalError(new DeliveryException()));
            }
        }

        private void onStop(ReActorContext ctx) {
            logOnStop(ctx);
            dataPublisher.interrupt();
            if (fileLines != null) {
                Try.ofRunnable(() -> fileLines.close());
            }
        }

        private void readFileLine(ReActorContext ctx, InputStreamReader file, Duration backpressureDelay) {
            if (ctx.isStop()) {
                return;
            }
            try {
                char[] buffer = new char[ReactiveServer.READ_CHUNK_SIZE];
                int read = file.read(buffer);
                if (read == -1) {
                    dataPublisher.close();
                    return;
                }
                if (dataPublisher.submit(new String(buffer, 0, read)).isBackpressureRequired()) {
                    ctx.rescheduleMessage(new PublishRequest(backpressureDelay.multipliedBy(2)),
                                            backpressureDelay.multipliedBy(2));
                } else {
                    ctx.getSelf().tell(ctx.getSelf(), new PublishRequest(Duration.ofNanos(1)));
                }
            } catch (Exception exc) {
                ctx.getParent().publish(new InternalError(exc));
            }
        }
    }

    private static void logOnStop(ReActorContext ctx) {
        ctx.logInfo("Stopping {}", ctx.getSelf().getReActorId().getReActorName());
    }

    private record PublishRequest(Duration backpressureDelay) implements ReActedMessage { }
    private record InternalError(Throwable anyError) implements ReActedMessage { }
}
