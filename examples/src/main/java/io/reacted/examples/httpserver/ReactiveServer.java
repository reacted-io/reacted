package io.reacted.examples.httpserver;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.ReActorInit;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.ReActiveEntity;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.patterns.NonNullByDefault;
import io.reacted.patterns.Try;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@NonNullByDefault
public class ReactiveServer {
    public static void main(String[] args) throws IOException {
        var serverReactorSystem = new ReActorSystem(ReActorSystemConfig.newBuilder()
                                                                       .setReactorSystemName("io.reacted.examples.httpserver.ReactiveServer")
                                                                       .build()).initReActorSystem();
        ExecutorService serverPool = Executors.newSingleThreadExecutor();
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);
        server.createContext("/test", new ReactiveHttpHandler(serverReactorSystem));
        server.setExecutor(serverPool);
        server.start();
    }

    private static class ReactiveHttpHandler implements HttpHandler {
        private final ReActorSystem reactiveServerSystem;
        private final AtomicLong requestCounter;

        private ReactiveHttpHandler(ReActorSystem reactiveServerSystem) {
            this.reactiveServerSystem = reactiveServerSystem;
            this.requestCounter = new AtomicLong();
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            handleResponse(exchange, "GET".equals(exchange.getRequestMethod())
                                     ? handleGetRequest(exchange)
                                     : "No Value", this.requestCounter.incrementAndGet());
        }

        private void handleResponse(HttpExchange exchange, String paramValue, long requestId) {
            this.reactiveServerSystem.spawn(new ReactiveResponse(exchange, paramValue),
                                            ReActorConfig.newBuilder()
                                                         .setReActorName("Req_" + requestId)
                                                         .build());
        }

        private static String handleGetRequest(HttpExchange httpExchange) {
            return Try.of(() -> httpExchange.getRequestURI()
                                            .toString()
                                            .split("\\?")[1].split("=")[1])
                       .orElse("Invalid request");
        }
    }

    private static class ReactiveResponse implements ReActiveEntity {
        private final HttpExchange httpCtx;
        private final String paramValue;

        public ReactiveResponse(HttpExchange httpCtx, String paramValue) {
            this.httpCtx = Objects.requireNonNull(httpCtx);
            this.paramValue = Objects.requireNonNull(paramValue);
        }

        @Nonnull
        @Override
        public ReActions getReActions() {
            return ReActions.newBuilder()
                            .reAct(ReActorInit.class, this::onInit)
                            .reAct(IOException.class, ReactiveResponse::onError)
                            .reAct(HtmlPage.class, this::onHttpHeadersSent)
                            .reAct(ReActions::noReAction)
                            .build();
        }

        private void onInit(ReActorContext raCtx, ReActorInit init) {
            String htmlBuilder = "<html>" + "<body>" + "<h1>" + "Hello " + this.paramValue +
                                 "</h1>" + "</body>" + "</html>";
            CompletableFuture.supplyAsync(() -> Try.ofRunnable(() -> httpCtx.sendResponseHeaders(200, htmlBuilder.length()))
                                                   .ifSuccessOrElse(noVal -> raCtx.selfTell(new HtmlPage(htmlBuilder)),
                                                                    raCtx::selfTell));
        }

        private void onHttpHeadersSent(ReActorContext raCtx, HtmlPage htmlPage) {
            CompletableFuture.supplyAsync(() -> Try.ofRunnable(() -> sendBody(this.httpCtx, htmlPage.htmlresponse))
                                                   .ifSuccessOrElse(noVal -> raCtx.stop(), raCtx::selfTell));
        }

        private static void onError(ReActorContext raCtx, Throwable error) {
            raCtx.logError("Error sending reply", error);
            raCtx.stop();
        }

        private static void sendBody(HttpExchange httpCtx, String htmlResponse) throws IOException {
            var outputStream = httpCtx.getResponseBody();
            outputStream.write(htmlResponse.getBytes());
            outputStream.flush();
            outputStream.close();
        }

        private static final class HtmlPage implements Serializable {
            private final String htmlresponse;
            public HtmlPage(String htmlResponse) {
                this.htmlresponse = htmlResponse;
            }
        }
    }
}
