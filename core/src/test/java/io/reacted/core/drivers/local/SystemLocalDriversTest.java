package io.reacted.core.drivers.local;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import io.reacted.core.CoreConstants;
import io.reacted.core.MessageHelper;
import io.reacted.core.config.dispatchers.DispatcherConfig;
import io.reacted.core.config.reactors.ReActorConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.system.DirectCommunicationConfig;
import io.reacted.core.drivers.system.DirectCommunicationDriver;
import io.reacted.core.mailboxes.BasicMbox;
import io.reacted.core.messages.Message;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactors.ReActions;
import io.reacted.core.reactors.systemreactors.MagicTestReActor;
import io.reacted.core.reactorsystem.ReActorContext;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import io.reacted.core.runtime.Dispatcher;
import io.reacted.core.typedsubscriptions.TypedSubscription;
import io.reacted.core.utils.ReActedUtils;
import io.reacted.patterns.Try;
import java.io.File;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SystemLocalDriversTest {
    private static final String TMP_TEST_DIRECT_COMMUNICATION_TXT = "/tmp/testDirectCommunication.txt";

    @Test
    void test_fileCreatedInDirectCommunicationLogger() {
        var reActorSystem = getTestSystem(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        Assertions.assertEquals("DIRECT_COMMUNICATION@LOGGING_DIRECT_COMMUNICATION",
                                reActorSystem.getLoopback().getBackingDriver().getChannelId().toString());
        Assertions.assertTrue(new File(TMP_TEST_DIRECT_COMMUNICATION_TXT).exists());
        reActorSystem.shutDown();
        //file needs to be removed
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
    void logFileNotCreatedWhenFileNameIsIncorrect(String logPath) {
        Assertions.assertThrows(UncheckedIOException.class, () -> getTestSystem(logPath));
    }

    private static Stream<Arguments> logPaths() {
        return Stream.of(
                Arguments.of(TMP_TEST_DIRECT_COMMUNICATION_TXT),
                Arguments.of("/tmp/testDirect Communication.txt")
        );
    }

    @ParameterizedTest
    @MethodSource("logPaths")
    void messageIsLoggedInDirectCommunicationLogger(String logPath) {
        var testSystem = getTestSystem(logPath);

        await().until(() -> Files.readString(Path.of(logPath), StandardCharsets.US_ASCII)
                                 .strip(), Matchers.equalTo("INIT"));
        testSystem.shutDown();
    }

    @Test
    void deliveryStatusIsDeliveredWhenMessageIsSent() {
        var testSystem = getTestSystem(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        var deliveryAttempt = testSystem.getSystemSink().tell("Payload of this message")
                                        .toCompletableFuture()
                                        .join();
        testSystem.shutDown();
        deliveryAttempt.filter(DeliveryStatus::isDelivered)
                       .ifError(Assertions::fail);
    }
    
    private static ReActorSystem getTestSystem(String logFilePath) {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                                         .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                                         .setLocalDriver(SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver(logFilePath))
                                         .build();

        return new ReActorSystem(reActorSystemConfig).initReActorSystem();
    }
}
