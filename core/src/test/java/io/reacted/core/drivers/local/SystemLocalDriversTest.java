package io.reacted.core.drivers.local;

import static org.awaitility.Awaitility.await;

import io.reacted.core.CoreConstants;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorSystem;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SystemLocalDriversTest {
    private static final String TMP_TEST_DIRECT_COMMUNICATION_TXT = "/tmp/testDirectCommunication.txt";

    @AfterEach
    void cleanUp() throws IOException {
        File logFile = new File(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        Files.deleteIfExists(logFile.toPath());
    }

    @Test
    void fileCreatedInDirectCommunicationLogger() {
        var reActorSystem = gerReactorSystem(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        Assertions.assertEquals("DIRECT_COMMUNICATION@SIMPLIFIED_LOGGING_DIRECT_COMMUNICATION-",
                                reActorSystem.getLoopback().getBackingDriver().getChannelId().toString());
        Assertions.assertTrue(new File(TMP_TEST_DIRECT_COMMUNICATION_TXT).exists());
        reActorSystem.shutDown();
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
        Assertions.assertThrows(UncheckedIOException.class, () -> gerReactorSystem(logPath));
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
        var reActorSystem = gerReactorSystem(logPath);

        await().until(() -> Files.readString(Path.of(logPath), StandardCharsets.US_ASCII).strip(),
                      Matchers.containsString("Init"));
        reActorSystem.shutDown();
    }

    @Test
    void deliveryStatusIsDeliveredWhenMessageIsSent() {
        var reActorSystem = gerReactorSystem(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        var deliveryAttempt = reActorSystem.getSystemSink().tell("Payload of this message")
                .toCompletableFuture()
                .join();
        reActorSystem.shutDown();
        deliveryAttempt.filter(DeliveryStatus::isDelivered)
                .ifError(Assertions::fail);
    }

    private static ReActorSystem gerReactorSystem(String logFilePath) {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                .setLocalDriver(SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver(logFilePath))
                .build();

        return new ReActorSystem(reActorSystemConfig).initReActorSystem();
    }
}
