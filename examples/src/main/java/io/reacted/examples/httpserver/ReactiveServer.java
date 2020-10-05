package io.reacted.examples.httpserver;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.messages.reactors.ReActorStop;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactors.ReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;
import io.reacted.streams.ReactedSubmissionPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@NonNullByDefault
public class ReactiveServer {
    private static final Logger SERVER_LOGGER = LoggerFactory.getLogger(ReactiveServer.class);
    private static final String LOG_PATH = "/tmp/log";
    private static final String RESPONSE_DISPATCHER = "ResponseDispatcher";
    public static void main(String[] args) throws IOException {
        var serverReactorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                       /* Use chronicle driver and record execution property for replay */
                                                                       //.setLocalDriver(SystemLocalDrivers.getDirectCommunicationSimplifiedLogger(LOG_PATH))
                                                                       .setReactorSystemName("ReactiveServer")
                                                                       .addDispatcherConfig(DispatcherConfig.newBuilder()
                                                                                                            .setDispatcherName(RESPONSE_DISPATCHER)
                                                                                                            .setDispatcherThreadsNum(4)
                                                                                                            .setBatchSize(1)
                                                                                                            .build())
                                                                       .build()).initReActorSystem();
        ExecutorService serverPool = Executors.newSingleThreadExecutor();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);
        server.createContext("/read", new ReactiveHttpHandler(serverReactorSystem,
                                                              Executors.newFixedThreadPool(5),
                                                              new ThreadPoolExecutor(0, 1, 10, TimeUnit.SECONDS,
                                                                                     new LinkedBlockingDeque<>()),
                                                              Executors.newFixedThreadPool(5)));
        server.setExecutor(serverPool);
        server.start();
    }

    private static class ReactiveHttpHandler implements HttpHandler {
        private final ReActorSystem reactiveServerSystem;
        private final AtomicLong requestCounter;
        private final Executor backpressureExecutor;
        private final Executor outputExecutor;
        private final ThreadPoolExecutor singleThreadedSequencer;

        private ReactiveHttpHandler(ReActorSystem reactiveServerSystem, Executor backpressureExecutor,
                                    ThreadPoolExecutor singleThreadedSequencer, Executor outputExecutor) {
            this.reactiveServerSystem = Objects.requireNonNull(reactiveServerSystem);
            this.requestCounter = new AtomicLong();
            this.backpressureExecutor = Objects.requireNonNull(backpressureExecutor);
            this.outputExecutor = Objects.requireNonNull(outputExecutor);
            this.singleThreadedSequencer = Objects.requireNonNull(singleThreadedSequencer);
        }

        @Override
        public void handle(HttpExchange exchange) {
            handleResponse(exchange, "GET".equals(exchange.getRequestMethod())
                                     ? handleGetRequest(exchange)
                                     : List.of(), this.requestCounter.incrementAndGet());
        }

        private void handleResponse(HttpExchange exchange, List<String> filenames, long requestId) {
            if (!filenames.isEmpty()) {
                this.reactiveServerSystem.spawn(new ReactiveResponse(exchange, filenames, requestId,
                                                                     this.outputExecutor, this.backpressureExecutor,
                                                                     this.singleThreadedSequencer),
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
        private final Executor outputExecutor;
        private final Executor asyncBackpressureExecutor;
        private final ThreadPoolExecutor sequencer;
        private final long requestId;
        private final AtomicInteger processed;
        @Nullable
        private ReactedSubmissionPublisher<String> publisher;

        public ReactiveResponse(HttpExchange httpCtx, List<String> filePaths, long reqId,
                                Executor outputExecutor, Executor asyncBackpressureExecutor,
                                ThreadPoolExecutor sequencer) {
            this.httpCtx = Objects.requireNonNull(httpCtx);
            this.filePaths = Objects.requireNonNull(filePaths).stream()
                                    .sorted()
                                    .collect(Collectors.toUnmodifiableList());
            this.outputExecutor = Objects.requireNonNull(outputExecutor);
            this.asyncBackpressureExecutor = Objects.requireNonNull(asyncBackpressureExecutor);
            this.sequencer = Objects.requireNonNull(sequencer);
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
                            .reAct(InternalError.class, (raCtx, error) -> handleError(raCtx, error.anyError))
                            .reAct(ReActorStop.class, (raCtx, error) -> this.onStop(raCtx))
                            .reAct(ReActions::noReAction)
                            .build();
        }

        private void onInit(ReActorContext raCtx, ReActorInit init) {
            raCtx.logInfo("Initing {}", raCtx.getSelf().getReActorId().getReActorName());
            Try.ofRunnable(() -> this.httpCtx.sendResponseHeaders(200, 0))
               .flatMap(noVal -> Try.ofRunnable(() -> sendData("<html><body>")))
               .map(noVal -> spawnPathReaders(this.filePaths, raCtx, this.requestId))
               .flatMap(Try::identity)
               .ifError(error -> handleError(raCtx, error));
        }

        private void onDataPublisher(ReActorContext raCtx, ReactedSubmissionPublisher<String> publisher) {
            this.publisher = publisher;
            publisher.subscribe(ReactedSubmissionPublisher.ReActedSubscription.<String>newBuilder()
                                        .setAsyncBackpressurer(this.asyncBackpressureExecutor)
                                        .setSubscriberName("sub_" + raCtx.getSender().getReActorId().getReActorName())
                                        .setBufferSize(Flow.defaultBufferSize())
                                        .setBackpressureTimeout(ReactedSubmissionPublisher.RELIABLE_SUBSCRIPTION)
                                        .setSequencer(this.sequencer)
                                        .setSubscriber(getNexDataConsumer(raCtx, this.outputExecutor))
                                        .build())
                     .thenAccept(noVal -> raCtx.reply(new StartPublishing()));
        }

        private Flow.Subscriber<String> getNexDataConsumer(ReActorContext raCtx, Executor outputExecutor) {
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
                    outputExecutor.execute(() -> {
                        try {
                            sendData(item + "<br>");
                            Objects.requireNonNull(subscription).request(1);
                        } catch (Exception exc) {
                            onError(exc);
                        }
                    });
                }

                @Override
                public void onError(Throwable throwable) { handleError(raCtx, throwable); }

                @Override
                public void onComplete() {
                    if (ReactiveResponse.this.processed.decrementAndGet() == 0) {
                        raCtx.stop();
                    }
                }
            };
        }

        private void onStop(ReActorContext raCtx) {
            logOnStop(raCtx);
            if (this.publisher != null) {
                this.publisher.interrupt();
            }
            Try.ofRunnable(() -> sendData("</body></html>"))
               .ifSuccess(noVal -> { this.outputStream.flush();
                                     this.outputStream.close(); });
        }

        private void handleError(ReActorContext raCtx, Throwable anyError) {
            raCtx.stop();
            raCtx.logError("Error detected", anyError);
        }

        private void sendData(String htmlResponse) throws IOException {
            this.outputStream.write(htmlResponse.getBytes());
        }

        private Try<ReActorRef> spawnPathReaders(List<String> filePaths,
                                                 ReActorContext raCtx, long requestId) {
            return IntStream.range(0, filePaths.size())
                            .mapToObj(pathId -> raCtx.spawnChild(new ReadFileWorker(raCtx.getReActorSystem(),
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
                                                          ((raCtx, init) -> onInit(raCtx, filePath)))
                                                   .reAct(ReActorStop.class,
                                                          (raCtx, stop) -> onStop(raCtx))
                                                   .reAct(StartPublishing.class,
                                                          (raCtx, pubStart) -> CompletableFuture.runAsync(() -> readFileLine(raCtx, this.fileLines,
                                                                                                                             new char[2<<10])))
                                                   .reAct(ReActions::noReAction)
                                                   .build();
            this.readFileWorkerCfg = ReActorConfig.newBuilder()
                                                  .setReActorName(filePath + "|" + requestId + "|" + workerId)
                                                  .setDispatcherName(RESPONSE_DISPATCHER)
                                                  .build();
            this.dataPublisher = new ReactedSubmissionPublisher<>(reActorSystem, "publisher" + "|" + filePath + "|" +
                                                                                 requestId + "|" + workerId);
        }

        @Nonnull
        @Override
        public ReActions getReActions() { return this.readFileWorkerBehavior; }

        @Nonnull
        @Override
        public ReActorConfig getConfig() { return readFileWorkerCfg; }

        private void onInit(ReActorContext raCtx, String filePath) {
            this.fileLines = Try.of(() -> new InputStreamReader(new FileInputStream(filePath)))
                                .orElse(null, error -> raCtx.getParent().tell(raCtx.getSelf(),
                                                                              new InternalError(error)));

            if (this.fileLines == null) {
                this.dataPublisher.close();
                return;
            }
            raCtx.getParent()
                 .tell(raCtx.getSelf(), this.dataPublisher)
                 .thenAccept(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                               .ifError(error -> raCtx.getParent()
                                                                                      .tell(raCtx.getSelf(),
                                                                                            new InternalError(error))));
        }

        private void onStop(ReActorContext raCtx) {
            logOnStop(raCtx);
            if (this.fileLines != null) {
                Try.ofRunnable(() -> this.fileLines.close());
            }
        }

        private void readFileLine(ReActorContext raCtx, InputStreamReader file, char[] buffer) {
            try {
                int read = file.read(buffer);
                if (read == -1) {
                    this.dataPublisher.close();
                    return;
                }
                this.dataPublisher.backpressurableSubmit(new String(buffer, 0, read))
                                  .thenAccept(noVal -> readFileLine(raCtx,
                                                                    Objects.requireNonNull(fileLines), buffer));
            } catch (Exception exc) {
                raCtx.getParent().tell(raCtx.getSelf(), new InternalError(exc));
            }
        }
    }

    private static void logOnStop(ReActorContext raCtx) {
        raCtx.logInfo("Stopping {}", raCtx.getSelf().getReActorId().getReActorName());
    }

    @Immutable
    private static class StartPublishing implements Serializable { }

    @Immutable
    private static class InternalError implements Serializable {
        private final Throwable anyError;
        private InternalError(Throwable anyError) { this.anyError = anyError; }
    }
}
