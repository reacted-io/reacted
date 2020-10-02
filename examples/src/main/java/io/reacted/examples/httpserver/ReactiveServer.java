package io.reacted.examples.httpserver;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.mailboxes.BackpressuringMbox;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.mailboxes.MailBox;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@NonNullByDefault
public class ReactiveServer {
    private static final Logger SERVER_LOGGER = LoggerFactory.getLogger(ReactiveServer.class);
    private static final String LOG_PATH = "/tmp/log";
    public static void main(String[] args) throws IOException {
        var serverReactorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                       /* Use chronicle driver and record execution property for replay */
                                                                       //.setLocalDriver(SystemLocalDrivers.getDirectCommunicationSimplifiedLogger(LOG_PATH))
                                                                       .setReactorSystemName("ReactiveServer")
                                                                       .build()).initReActorSystem();
        ExecutorService serverPool = Executors.newSingleThreadExecutor();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);
        server.createContext("/read", new ReactiveHttpHandler(serverReactorSystem,
                                                              Executors.newFixedThreadPool(5),
                                                              Executors.newSingleThreadExecutor(),
                                                              Executors.newFixedThreadPool(5)));
        server.setExecutor(serverPool);
        server.start();
    }

    private static class ReactiveHttpHandler implements HttpHandler {
        private final ReActorSystem reactiveServerSystem;
        private final AtomicLong requestCounter;
        private final Executor backpressureExecutor;
        private final Executor outputExecutor;
        private final ExecutorService singleThreadedSequencer;

        private ReactiveHttpHandler(ReActorSystem reactiveServerSystem, Executor backpressureExecutor,
                                    ExecutorService singleThreadedSequencer, Executor outputExecutor) {
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
                                                                     this.outputExecutor),
                                                ReActorConfig.newBuilder()
                                                             .setMailBoxProvider(() -> ReactiveResponse.newBackpressuredMailbox(backpressureExecutor,
                                                                                                                                singleThreadedSequencer))
                                                             .setReActorName("Request " + requestId)
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
        private final long requestId;
        private int processed = 0;

        public ReactiveResponse(HttpExchange httpCtx, List<String> filePaths, long reqId,
                                Executor outputExecutor) {
            this.httpCtx = Objects.requireNonNull(httpCtx);
            this.filePaths = Objects.requireNonNull(filePaths).stream()
                                    .sorted()
                                    .collect(Collectors.toUnmodifiableList());
            this.outputExecutor = Objects.requireNonNull(outputExecutor);
            this.outputStream = httpCtx.getResponseBody();
            this.requestId = reqId;
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class, this::onInit)
                            .reAct(PublishNewLineRequest.class, this::publishNewLineRequest)
                            .reAct(ProcessComplete.class,
                                   (raCtx, processComplete) -> onProcessComplete(raCtx, ++processed))
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

        private void onProcessComplete(ReActorContext raCtx, int processed) {
            if (processed == this.filePaths.size()) {
                raCtx.stop();
            }
        }

        private void onStop(ReActorContext raCtx) {
            logOnStop(raCtx);
            Try.ofRunnable(() -> sendData("</body></html>"))
               .ifError(error -> raCtx.logError("Unable to properly close web page", error));
            Try.ofRunnable(this.outputStream::close)
               .ifError(error -> raCtx.logError("Error closing output stream", error));
        }

        private void publishNewLineRequest(ReActorContext raCtx, PublishNewLineRequest newLineRequest) {
            this.outputExecutor.execute(() ->
            Try.ofRunnable(() -> sendData(newLineRequest.newLine+"<br>"))
               .ifSuccessOrElse(success -> ((BackpressuringMbox)raCtx.getMbox()).request(1),
                                error -> handleError(raCtx, error)));
        }

        private void handleError(ReActorContext raCtx, Throwable anyError) {
            raCtx.stop();
            raCtx.logError("Error detected", anyError);
            Try.ofRunnable(() -> sendData(anyError.toString()))
               .ifError(error -> raCtx.logError("Error sending back the original error {}", anyError, error));
        }

        private void sendData(String htmlResponse) throws IOException {
            this.outputStream.write(htmlResponse.getBytes());
            this.outputStream.flush();
        }

        private Try<ReActorRef> spawnPathReaders(List<String> filePaths,
                                                 ReActorContext raCtx, long requestId) {
            return IntStream.range(0, filePaths.size())
                            .mapToObj(pathId -> raCtx.spawnChild(new ReadFileWorker(filePaths.get(pathId),
                                                                                    requestId, pathId)))
                            .reduce(ReactiveResponse::detectAnyError)
                            .orElseGet(() -> Try.ofFailure(new IllegalStateException()));
        }

        private static <PayloadT> Try<PayloadT> detectAnyError(Try<PayloadT> first, Try<PayloadT> second) {
            return first.flatMap(payload -> second);
        }

        private static MailBox newBackpressuredMailbox(Executor backpressureExecutor,
                                                       ExecutorService singleThreadedExecutor) {
            return BackpressuringMbox.newBuilder()
                                     .setRealMbox(new BasicMbox())
                                     .setBackpressureTimeout(Duration.ofNanos(Long.MAX_VALUE))
                                     .setBufferSize(Flow.defaultBufferSize())
                                     .setRequestOnStartup(1)
                                     .setAsyncBackpressurer(backpressureExecutor)
                                     .setSequencer(singleThreadedExecutor)
                                     .setNonDelayable(Set.of(InternalError.class, ReActorInit.class))
                                     .build();
        }
    }

    private static class ReadFileWorker implements ReActor {
        private final ReActions readFileWorkerBehavior;
        private final ReActorConfig readFileWorkerCfg;
        @Nullable
        private BufferedReader fileLines;
        private ReadFileWorker(String filePath, long requestId, long workerId) {
            this.readFileWorkerBehavior = ReActions.newBuilder()
                                                   .reAct(ReActorInit.class,
                                                          ((raCtx, init) -> onInit(raCtx, filePath)))
                                                   .reAct(ReActorStop.class,
                                                          (raCtx, stop) -> onStop(raCtx))
                                                   .reAct(NextLineRequest.class,
                                                          (raCtx, next) -> readFileLine(raCtx, this.fileLines))
                                                   .reAct(ReActions::noReAction)
                                                   .build();
            this.readFileWorkerCfg = ReActorConfig.newBuilder()
                                                  .setReActorName(filePath + "|" + requestId + "|" + workerId)
                                                  .build();
        }

        @Nonnull
        @Override
        public ReActions getReActions() { return this.readFileWorkerBehavior; }

        @Nonnull
        @Override
        public ReActorConfig getConfig() { return readFileWorkerCfg; }

        private void onInit(ReActorContext raCtx, String filePath) {
            this.fileLines = Try.of(() -> new BufferedReader(new InputStreamReader(new FileInputStream(filePath))))
                                .orElse(null, error -> raCtx.getParent().tell(raCtx.getSelf(),
                                                                              new InternalError(error)));
            if (this.fileLines != null) {
                CompletableFuture.runAsync(() -> raCtx.selfTell(new NextLineRequest()));
            }
        }

        private void onStop(ReActorContext raCtx) {
            if (this.fileLines != null) {
                Try.ofRunnable(() -> this.fileLines.close());
            }
            logOnStop(raCtx);
        }

        private void readFileLine(ReActorContext raCtx, BufferedReader fileLines) {
            String newLine;
            try {
                newLine = fileLines.readLine();
                if (newLine == null) {
                    raCtx.getParent().tell(raCtx.getSelf(), new ProcessComplete());
                    return;
                }
                raCtx.getParent().tell(raCtx.getSelf(), new PublishNewLineRequest(newLine))
                                 .thenAccept(deliveryAttempt -> deliveryAttempt.filter(DeliveryStatus::isDelivered)
                                                                               .ifSuccessOrElse(success -> raCtx.selfTell(new NextLineRequest()),
                                                                                                error -> raCtx.getParent().tell(raCtx.getSelf(),
                                                                                                                                new InternalError(error))));
            } catch (Exception exc) {
                raCtx.getParent().tell(raCtx.getSelf(), new InternalError(exc));
            }
        }
    }

    private static void logOnStop(ReActorContext raCtx) {
        raCtx.logInfo("Stopping {}", raCtx.getSelf().getReActorId().getReActorName());
    }

    @Immutable
    private static class NextLineRequest implements Serializable { }

    @Immutable
    private static class InternalError implements Serializable {
        private final Throwable anyError;
        private InternalError(Throwable anyError) { this.anyError = anyError; }
    }

    @Immutable
    private static class ProcessComplete implements Serializable { }

    @Immutable
    private static class PublishNewLineRequest implements Serializable {
        private final String newLine;
        private PublishNewLineRequest(String newLine) { this.newLine = newLine; }
    }
}
