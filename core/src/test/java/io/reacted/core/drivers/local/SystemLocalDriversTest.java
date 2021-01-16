package io.reacted.core.drivers.local;

import io.reacted.core.MessageHelper;
import io.reacted.core.drivers.system.DirectCommunicationConfig;
import io.reacted.core.drivers.system.DirectCommunicationDriver;
import io.reacted.core.drivers.system.DirectCommunicationLoggerDriver;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.patterns.Try;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

class SystemLocalDriversTest {
    private static final String TMP_TEST_DIRECT_COMMUNICATION_TXT = "/tmp/testDirectCommunication.txt";
    private BasicMbox actorMbox;
    private DirectCommunicationDriver localDriver;
    private Message defaultMessage;
    private ReActorContext reActorContext;

    @BeforeEach
    void setUp() {
        actorMbox = new BasicMbox();
        reActorContext = ReActorContext.newBuilder()
                .setMbox(ctx -> actorMbox)
                .setReactorRef(ReActorRef.NO_REACTOR_REF)
                .setReActorSystem(mock(ReActorSystem.class))
                .setParentActor(ReActorRef.NO_REACTOR_REF)
                .setSubscriptions(TypedSubscription.LOCAL.forType(Message.class))
                .setDispatcher(mock(Dispatcher.class))
                .setReActions(mock(ReActions.class))
                .build();
        defaultMessage = MessageHelper.getDefaultMessage();
        localDriver = new DirectCommunicationDriver(DirectCommunicationConfig.newBuilder()
                .setChannelName("LOGGING_DIRECT_COMMUNICATION")
                .build());
    }

    @Test
    void fileCreatedInDirectCommunicationLogger() {
        Assertions.assertEquals("DIRECT_COMMUNICATION@LOGGING_DIRECT_COMMUNICATION",
                localDriver.getChannelId().toString());
        Assertions.assertTrue(new File(TMP_TEST_DIRECT_COMMUNICATION_TXT).exists());
    }

    private static Stream<Arguments> incorrectLogPaths() {
        return Stream.of(
                // no file name specified
                Arguments.of("/tmps/"),
                // directory not existent
                Arguments.of("/tmps/test.txt"));
    }

    @ParameterizedTest
    @MethodSource("incorrectLogPaths")
    void logfileNotCreatedWhenFileNameIsIncorrect(String logPath) {
        Assertions.assertThrows(UncheckedIOException.class,
                () -> SystemLocalDrivers.getDirectCommunicationLogger(logPath));
    }

    private static Stream<Arguments> logPaths() {
        return Stream.of(
                Arguments.of(TMP_TEST_DIRECT_COMMUNICATION_TXT),
                Arguments.of("/tmp/testDirect Communication.txt")
        );
    }

    @ParameterizedTest
    @MethodSource("logPaths")
    void messageIsLoggedInDirectCommunicationLogger(String logPath) throws IOException {
        var directCommunicationLogger = SystemLocalDrivers.getDirectCommunicationLogger(logPath);
        System.out.println(directCommunicationLogger.getDriverConfig().getLogFilePath());
        localDriver.sendMessage(reActorContext, defaultMessage);

        Assertions.assertEquals(defaultMessage.toString(),
                Files.readString(Path.of(directCommunicationLogger.getDriverConfig().getLogFilePath()), StandardCharsets.US_ASCII).strip());
    }

    @Test
    void simpleAsyncMessageIsSent() throws ExecutionException, InterruptedException {
        DirectCommunicationLoggerDriver directCommunicationLogger = SystemLocalDrivers.getDirectCommunicationLogger(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        CompletionStage<Try<DeliveryStatus>> tryCompletionStage = localDriver.sendAsyncMessage(reActorContext,
                defaultMessage);

//        Assertions.assertTrue(
//                tryCompletionStage.toCompletableFuture().get().ifSuccess().stream().findFirst().get().isDelivered());
        Assertions.assertEquals(defaultMessage, actorMbox.getNextMessage());
    }

    @Test
    void simpleAsyncMessageNotSentWhenDestinationIsStopped() throws ExecutionException, InterruptedException {
        DirectCommunicationLoggerDriver directCommunicationLogger = SystemLocalDrivers.getDirectCommunicationLogger(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        reActorContext.stop();
        CompletionStage tryCompletionStage = localDriver.sendAsyncMessage(reActorContext,
                defaultMessage);

//        Assertions.assertTrue(
//                tryCompletionStage.toCompletableFuture().get().stream().findFirst().get().isNotDelivered());
    }

    @Test
    void deliveryStatusIsDeliveredWhenMessageIsSent() {
        DirectCommunicationLoggerDriver directCommunicationLogger = SystemLocalDrivers.getDirectCommunicationLogger(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        Try<DeliveryStatus> deliveryStatusTry = localDriver.sendMessage(reActorContext, defaultMessage);
        Assertions.assertTrue(deliveryStatusTry.get().isDelivered());
    }
}