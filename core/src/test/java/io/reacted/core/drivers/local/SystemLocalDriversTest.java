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
import java.nio.file.Paths;
import java.time.Instant;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SystemLocalDriversTest {
    private static final String TMP_TEST_LOGS = "/tmp/test_logs/";
    private static final String TMP_TEST_LOGS_TEST_DIRECT_COMMUNICATION_TXT =
            TMP_TEST_LOGS + "testDirectCommunication.txt";

    @BeforeEach
    void beforeEach() throws IOException {
        Files.createDirectories(Paths.get("/tmp/test_logs/"));
    }

    @AfterEach
    void cleanUp() throws IOException {
        FileUtils.deleteDirectory(new File(TMP_TEST_LOGS));
    }

    private static Stream<Arguments> logger() {
        return Stream.of(
                Arguments.of(true, "SIMPLIFIED_LOGGING_DIRECT_COMMUNICATION-"),
                Arguments.of(false, "LOGGING_DIRECT_COMMUNICATION-"));
    }

    @ParameterizedTest
    @MethodSource("logger")
    void fileCreatedInDirectCommunicationLogger(boolean isSimplifiedLogger, String channelId) {
        var reActorSystem = gerReactorSystem(isSimplifiedLogger, TMP_TEST_LOGS_TEST_DIRECT_COMMUNICATION_TXT);
        Assertions.assertEquals("DIRECT_COMMUNICATION@" + channelId,
                                reActorSystem.getLoopback().getBackingDriver().getChannelId().toString());
        Assertions.assertTrue(new File(TMP_TEST_LOGS_TEST_DIRECT_COMMUNICATION_TXT).exists());
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
        Assertions.assertThrows(UncheckedIOException.class, () -> gerReactorSystem(true, logPath));
    }

    private static Stream<Arguments> logPaths() {
        return Stream.of(
                Arguments.of(true, TMP_TEST_LOGS_TEST_DIRECT_COMMUNICATION_TXT),
                Arguments.of(false, "/tmp/test_logs/testDirect Communication.txt"),
                Arguments.of(false, "/tmp/test_logs/" + Instant.now().toEpochMilli() + ".txt")
        );
    }

    @ParameterizedTest
    @MethodSource("logPaths")
    void messageIsLoggedInDirectCommunicationLogger(boolean isSimplifiedLogger, String logPath) {
        var reActorSystem = gerReactorSystem(isSimplifiedLogger, logPath);

        await().until(() -> Files.readString(Path.of(logPath), StandardCharsets.US_ASCII).strip(),
                      Matchers.containsString("Init"));
        reActorSystem.shutDown();
    }

    private static Stream<Arguments> loggerType() {
        return Stream.of(
                Arguments.of(true),
                Arguments.of(false)
        );
    }

    @ParameterizedTest
    @MethodSource("loggerType")
    void deliveryStatusIsDeliveredWhenMessageIsSent(boolean isSimplifiedLogger) {
        var reActorSystem = gerReactorSystem(isSimplifiedLogger, TMP_TEST_LOGS_TEST_DIRECT_COMMUNICATION_TXT);
        var deliveryAttempt = reActorSystem.getSystemSink().tell("Payload of this message")
                .toCompletableFuture()
                .join();
        reActorSystem.shutDown();
        deliveryAttempt.filter(DeliveryStatus::isDelivered)
                .ifError(Assertions::fail);
    }

    private static ReActorSystem gerReactorSystem(boolean isSimplifiedLogger, String logFilePath) {
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                .setLocalDriver(isSimplifiedLogger ?
                                SystemLocalDrivers.getDirectCommunicationSimplifiedLoggerDriver(logFilePath)
                                                   :
                                SystemLocalDrivers.getDirectCommunicationLogger(logFilePath))
                .build();

        return new ReActorSystem(reActorSystemConfig).initReActorSystem();
    }
}
