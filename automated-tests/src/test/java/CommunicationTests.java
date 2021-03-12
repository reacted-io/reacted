import io.reacted.core.config.drivers.ChannelDriverConfig;
import io.reacted.core.config.reactorsystem.ReActorSystemConfig;
import io.reacted.core.drivers.local.LocalDriver;
import io.reacted.core.drivers.local.SystemLocalDrivers;
import io.reacted.core.drivers.serviceregistries.ServiceRegistryDriver;
import io.reacted.core.drivers.system.RemotingDriver;
import io.reacted.core.messages.reactors.DeliveryStatus;
import io.reacted.core.reactorsystem.ReActorRef;
import io.reacted.core.reactorsystem.ReActorSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utils.PreparationRequest;
import utils.SimpleTestReActor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class CommunicationTests {
    private static final Collection<ServiceRegistryDriver<?, ?>> NO_SERVICE_REGISTRIES = List.of();
    private static final Collection<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> NO_REMOTING_DRIVERS = List.of();
    public static final String DELIMITER = ":";

    @Test
    void test1() {
        var simpleReActorSystem = getDefaultStartedReActorSystem("ReactorSystem");

        var messagesToSend = 20;
        var newReActorInstance = new SimpleTestReActor(DELIMITER, messagesToSend);
        var newReActorReference = simpleReActorSystem.spawn(newReActorInstance.getReActions(),
                newReActorInstance.getConfig())
                .orElse(ReActorRef.NO_REACTOR_REF, error -> {
                    error.printStackTrace();
                    simpleReActorSystem.shutDown();
                });

        AtomicInteger count = new AtomicInteger();
        newReActorReference.tell(ReActorRef.NO_REACTOR_REF, new PreparationRequest())
                .toCompletableFuture()
                .join()
                .filter(DeliveryStatus::isDelivered);
        IntStream.range(0, messagesToSend).parallel()
                .forEach(msgNum -> {
                    newReActorReference.atell("Ping Request" + DELIMITER + msgNum);
                    count.getAndIncrement();
                });

        Assertions.assertEquals(messagesToSend, count.intValue());
    }

    public static ReActorSystem getDefaultStartedReActorSystem(String reActorSystemName) {
        return new ReActorSystem(getDefaultReActorSystemCfg(reActorSystemName)).initReActorSystem();
    }

    public static ReActorSystemConfig getDefaultReActorSystemCfg(String reActorSystemName) {
        return getDefaultReActorSystemCfg(reActorSystemName, SystemLocalDrivers.DIRECT_COMMUNICATION,
                NO_SERVICE_REGISTRIES, NO_REMOTING_DRIVERS);
    }

    public static ReActorSystemConfig getDefaultReActorSystemCfg(String reActorSystemName,
                                                                 LocalDriver<? extends ChannelDriverConfig<?, ?>> localDriver,
                                                                 Collection<ServiceRegistryDriver<?, ?>> serviceRegistryDrivers,
                                                                 Collection<RemotingDriver<? extends ChannelDriverConfig<?, ?>>> remotingDrivers) {
        var configBuilder = ReActorSystemConfig.newBuilder()
                .setLocalDriver(localDriver)
                .setMsgFanOutPoolSize(1)
                .setRecordExecution(true)
                .setReactorSystemName(reActorSystemName);
        serviceRegistryDrivers.forEach(configBuilder::addServiceRegistryDriver);
        remotingDrivers.forEach(configBuilder::addRemotingDriver);
        return configBuilder.build();
    }
}
