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
import io.reacted.patterns.Try;
import java.io.File;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SystemLocalDriversTest {
    private static final String TMP_TEST_DIRECT_COMMUNICATION_TXT = "/tmp/testDirectCommunication.txt";
    private DirectCommunicationDriver localDriver;
    private ReActorContext reActorContext;

    @BeforeEach
    void setUp() {
        localDriver = new DirectCommunicationDriver(DirectCommunicationConfig.newBuilder()
                                                            .setChannelName("LOGGING_DIRECT_COMMUNICATION")
                                                            .build());
        ReActorSystemConfig reActorSystemConfig = ReActorSystemConfig.newBuilder()
                .setReactorSystemName(CoreConstants.REACTED_ACTOR_SYSTEM)
                .setMsgFanOutPoolSize(1)
                .setRecordExecution(false)
                .setLocalDriver(localDriver)
                .addDispatcherConfig(DispatcherConfig.newBuilder()
                                             .setDispatcherName(CoreConstants.DISPATCHER)
                                             .setBatchSize(1_000)
                                             .setDispatcherThreadsNum(1)
                                             .build())
                .build();
        ReActorSystem reActorSystem = new ReActorSystem(reActorSystemConfig);
        reActorSystem.initReActorSystem();

        localDriver.initDriverLoop(reActorSystem);

        TypedSubscription subscribedTypes = TypedSubscription.LOCAL.forType(Message.class);
        ReActorConfig reActorConfig = ReActorConfig.newBuilder()
                .setReActorName(CoreConstants.REACTOR_NAME)
                .setDispatcherName(CoreConstants.DISPATCHER)
                .setMailBoxProvider(ctx -> new BasicMbox())
                .setTypedSubscriptions(subscribedTypes)
                .build();

        ReActorRef reActorRef = reActorSystem.spawn(new MagicTestReActor(1, true, reActorConfig))
                .orElseSneakyThrow();

        reActorSystem.registerReActorSystemDriver(localDriver);

        reActorContext = ReActorContext.newBuilder()
                .setMbox(ctx -> new BasicMbox())
                .setReactorRef(reActorRef)
                .setReActorSystem(reActorSystem)
                .setParentActor(ReActorRef.NO_REACTOR_REF)
                .setSubscriptions(subscribedTypes)
                .setDispatcher(mock(Dispatcher.class))
                .setReActions(mock(ReActions.class))
                .build();
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
    void logFileNotCreatedWhenFileNameIsIncorrect(String logPath) {
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
    void messageIsLoggedInDirectCommunicationLogger(String logPath) {
        var directCommunicationLogger = SystemLocalDrivers.getDirectCommunicationLogger(logPath);
        localDriver.sendMessage(reActorContext, MessageHelper.getDefaultMessage());

        await().until(() -> Files.readString(Path.of(directCommunicationLogger.getDriverConfig().getLogFilePath()),
                                             StandardCharsets.US_ASCII).strip(),
                      Matchers.equalTo(MessageHelper.getDefaultMessage().toString()));
    }

    @Test
    void deliveryStatusIsDeliveredWhenMessageIsSent() {
        SystemLocalDrivers.getDirectCommunicationLogger(TMP_TEST_DIRECT_COMMUNICATION_TXT);
        Try<DeliveryStatus> deliveryStatusTry =
                localDriver.sendMessage(reActorContext, MessageHelper.getDefaultMessage());
        Assertions.assertTrue(deliveryStatusTry.get().isDelivered());
    }
}