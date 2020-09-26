package io.reacted.core.drivers.local;

import static org.mockito.Mockito.mock;

import io.reacted.core.MessageHelper;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.patterns.Try;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SystemLocalDriversTest {
    public static final String TMP_TEST_DIRECT_COMMUNICATION_TXT = "/tmp/testDirectCommunication.txt";
    private LocalDriver localDriver;
    private Message defaultMessage;
    private ReActorContext reActorContext;

    @BeforeEach
    void setUp() {
        localDriver = SystemLocalDrivers.getDirectCommunicationLogger(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        reActorContext = ReActorContext.newBuilder()
                .setMbox(mock(BasicMbox.class))
                .setReactorRef(ReActorRef.NO_REACTOR_REF)
                .setReActorSystem(mock(ReActorSystem.class))
                .setParentActor(ReActorRef.NO_REACTOR_REF)
                .setInterceptRules()
                .setDispatcher(mock(Dispatcher.class))
                .setReActions(mock(ReActions.class))
                .build();
        defaultMessage = MessageHelper.getDefaultMessage();
    }

    @Test
    void fileCreatedInDirectCommunicationLogger() {
        Assertions.assertEquals(
                "DIRECT_COMMUNICATION@LOGGING_DIRECT_COMMUNICATION-" + TMP_TEST_DIRECT_COMMUNICATION_TXT,
                localDriver.getChannelId().toString());
        Assertions.assertTrue(new File(TMP_TEST_DIRECT_COMMUNICATION_TXT).exists());
    }

    @Test
    void messageIsLoggedInDirectCommunicationLogger() throws IOException {
        localDriver.sendMessage(reActorContext, defaultMessage);

        Assertions.assertEquals(defaultMessage.toString(),
                                Files.readString(Path.of(TMP_TEST_DIRECT_COMMUNICATION_TXT),
                                                 StandardCharsets.US_ASCII).strip());
    }

    @Test
    void messageNotSentWhenDestinationIsStoppedInDirectCommunicationLogger()
            throws ExecutionException, InterruptedException {
        reActorContext.stop();
        CompletionStage<Try<DeliveryStatus>> tryCompletionStage = localDriver.sendAsyncMessage(reActorContext,
                                                                                               defaultMessage);

        Assertions.assertTrue(
                tryCompletionStage.toCompletableFuture().get().stream().findFirst().get().isNotDelivered());

    }
}